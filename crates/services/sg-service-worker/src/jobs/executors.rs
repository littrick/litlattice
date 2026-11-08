#![forbid(unsafe_code)]

//! `cmd/worker/internal/executors`: the `executors-janitor` job.
//!
//! Two routines:
//!
//! * `executor.heartbeat-janitor` deletes `executor_heartbeats` rows older
//!   than `EXECUTORS_HEARTBEAT_RECORD_MAX_AGE` (default `168h`);
//! * `executors.multiqueue-cache-cleaner` walks the multiqueue dequeue cache
//!   (`executor_multihandler_dequeues`, one hash per queue, field = unix
//!   nanoseconds) and deletes entries older than
//!   `EXECUTORS_MULTIQUEUE_CACHE_DEQUEUE_TTL` (default `5m0s`). For each queue
//!   it also registers a scrape-time `GaugeFunc`
//!   `multiqueue_executor_dequeue_cache_size{queue="..."}`.
//!
//! The baseline builds its cache from `redispool.Cache`, which dials lazily.
//! [`sg_redis::KeyValue`] dials eagerly, so when Redis is unreachable at
//! startup the cleaner is skipped with a warning (the heartbeat janitor still
//! runs); this is recorded as an erratum in docs/modules/06_worker.md.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use sg_core::{parse_go_duration, ConfigError, EnvReader};
use sg_db::OffsetDateTime;
use sg_metrics::prometheus::core::{Collector, Desc};
use sg_metrics::prometheus::proto::{
    Gauge as ProtoGauge, LabelPair, Metric, MetricFamily, MetricType,
};
use sg_rcache::Cache;
use sg_redis::KeyValue;
use tokio::runtime::RuntimeFlavor;
use tokio_util::sync::CancellationToken;

use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};

/// `executortypes.DequeueCachePrefix`.
pub const DEQUEUE_CACHE_PREFIX: &str = "executor_multihandler_dequeues";
/// `executortypes.DequeueTtl`.
pub const DEQUEUE_TTL: Duration = Duration::from_secs(5 * 60);
/// `executortypes.CleanupInterval`.
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
/// `executortypes.ValidQueueNames`.
pub const VALID_QUEUE_NAMES: [&str; 2] = ["batches", "codeintel"];

pub const ROUTINE_HEARTBEAT_JANITOR: &str = "executor.heartbeat-janitor";
pub const HEARTBEAT_DESCRIPTION: &str =
    "clean up executor heartbeat records for presumed dead executors";
pub const ROUTINE_CACHE_CLEANER: &str = "executors.multiqueue-cache-cleaner";
pub const CLEANER_DESCRIPTION: &str =
    "deletes entries from the dequeue cache older than the configured window";

pub const CACHE_SIZE_METRIC: &str = "multiqueue_executor_dequeue_cache_size";
pub const CACHE_SIZE_HELP: &str = "Current size of the executor dequeue cache";

/// `janitorConfig`.
#[derive(Debug, Default)]
pub struct JanitorConfig {
    values: Mutex<JanitorValues>,
    errs: Mutex<Vec<String>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JanitorValues {
    pub cleanup_task_interval: Duration,
    pub heartbeat_records_max_age: Duration,
    pub cache_cleanup_interval: Duration,
    pub cache_dequeue_ttl: Duration,
}

impl JanitorConfig {
    pub fn snapshot(&self) -> JanitorValues {
        *self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobConfig for JanitorConfig {
    /// `janitorConfig.Load`.
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let values = JanitorValues {
            cleanup_task_interval: get_interval(
                env,
                "EXECUTORS_CLEANUP_TASK_INTERVAL",
                "30m",
                "The frequency with which to run executor cleanup tasks.",
                &mut errs,
            ),
            heartbeat_records_max_age: get_interval(
                env,
                "EXECUTORS_HEARTBEAT_RECORD_MAX_AGE",
                "168h",
                "The age after which inactive executor heartbeat records are deleted.",
                &mut errs,
            ),
            cache_cleanup_interval: get_interval(
                env,
                "EXECUTORS_MULTIQUEUE_CACHE_CLEANUP_INTERVAL",
                "30s",
                "The frequency with which the multiqueue dequeue cache is cleaned up.",
                &mut errs,
            ),
            cache_dequeue_ttl: get_interval(
                env,
                "EXECUTORS_MULTIQUEUE_CACHE_DEQUEUE_TTL",
                "5m0s",
                "The duration after which a dequeue is deleted from the multiqueue dequeue cache.",
                &mut errs,
            ),
        };

        *self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = values;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    /// `env.BaseConfig.Validate`.
    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match combine_errors(&errs) {
            Some(message) => Err(ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// `BaseConfig.GetInterval`: parse `time.ParseDuration`, record
/// `invalid duration %q for %s: %s` and return zero on failure.
fn get_interval(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> Duration {
    let raw = env.get(name, default, description);
    match parse_go_duration(&raw) {
        Ok(duration) => duration,
        Err(message) => {
            errors.push(format!("invalid duration {raw:?} for {name}: {message}"));
            Duration::ZERO
        }
    }
}

/// `executors.janitorJob`.
pub struct ExecutorsJanitor {
    config: Arc<JanitorConfig>,
}

impl ExecutorsJanitor {
    pub fn new() -> Self {
        Self {
            config: Arc::new(JanitorConfig::default()),
        }
    }

    /// The job's `janitorConfigInst`, for tests and harnesses.
    pub fn config(&self) -> &JanitorConfig {
        &self.config
    }
}

impl Default for ExecutorsJanitor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for ExecutorsJanitor {
    /// `janitorJob.Description` returns the empty string.
    fn description(&self) -> String {
        String::new()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<PeriodicRoutine>, String> {
        let config = self.config.snapshot();

        let mut routines = vec![PeriodicRoutine::new(Arc::new(HeartbeatJanitorHandler {
            pool: deps.frontend.clone(),
            max_age: config.heartbeat_records_max_age,
        }) as Arc<dyn Handler>)
        .with_name(ROUTINE_HEARTBEAT_JANITOR)
        .with_description(HEARTBEAT_DESCRIPTION)
        .with_interval(config.cleanup_task_interval)];

        match KeyValue::connect(&sg_redis::cache_endpoint()).await {
            Ok(kv) => {
                let cache = Cache::new(kv, DEQUEUE_CACHE_PREFIX);
                register_cache_size_metrics(&cache);
                let queue_names = VALID_QUEUE_NAMES
                    .iter()
                    .map(|queue| (*queue).to_owned())
                    .collect();
                routines.push(
                    PeriodicRoutine::new(Arc::new(MultiqueueCacheCleaner::new(
                        queue_names,
                        cache,
                        config.cache_dequeue_ttl,
                    )) as Arc<dyn Handler>)
                    .with_name(ROUTINE_CACHE_CLEANER)
                    .with_description(CLEANER_DESCRIPTION)
                    .with_interval(config.cache_cleanup_interval),
                );
            }
            Err(err) => tracing::warn!(
                error = %err,
                "executors-janitor: cannot reach Redis, skipping the multiqueue cache cleaner"
            ),
        }

        Ok(routines)
    }
}

/// `DeleteInactiveHeartbeats(ctx, janitorConfigInst.HeartbeatRecordsMaxAge)`.
struct HeartbeatJanitorHandler {
    pool: sqlx::PgPool,
    max_age: Duration,
}

#[async_trait]
impl Handler for HeartbeatJanitorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        sg_db::executor_heartbeats::delete_inactive_heartbeats(
            &self.pool,
            timeutil_now(),
            self.max_age,
        )
        .await
        .map(|_| ())
        .map_err(|err| HandlerError::new(err.to_string()))
    }
}

/// `timeutil.Now`: `time.Now().UTC().Truncate(time.Microsecond)`.
pub fn timeutil_now() -> OffsetDateTime {
    let now = OffsetDateTime::now_utc();
    let micros = now.unix_timestamp_nanos() / 1_000;
    OffsetDateTime::from_unix_timestamp_nanos(micros * 1_000)
        .expect("truncated now is within OffsetDateTime's range")
}

/// `executors.multiqueueCacheCleaner`.
pub struct MultiqueueCacheCleaner {
    queue_names: Vec<String>,
    cache: Cache,
    window: Duration,
    now: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl MultiqueueCacheCleaner {
    pub fn new(queue_names: Vec<String>, cache: Cache, window: Duration) -> Self {
        Self {
            queue_names,
            cache,
            window,
            now: Arc::new(SystemTime::now),
        }
    }

    /// `Handle`: one pass over every queue.
    pub async fn clean(&self) -> Result<(), HandlerError> {
        let max_age = (self.now)().checked_sub(self.window).unwrap_or(UNIX_EPOCH);
        let max_age_nanos = unix_nanos(max_age);

        for queue_name in &self.queue_names {
            let all = self
                .cache
                .get_hash_all(queue_name)
                .await
                .map_err(|err| HandlerError::new(format!("multiqueue.cachecleaner: {err}")))?;

            for key in all.keys() {
                let Ok(key_as_unix_nano) = key.parse::<i64>() else {
                    // `strconv.ParseInt`'s error is returned verbatim.
                    return Err(HandlerError::new(format!(
                        "strconv.ParseInt: parsing {key:?}: invalid syntax"
                    )));
                };
                let timestamp_nanos = i128::from(key_as_unix_nano);
                if timestamp_nanos < max_age_nanos {
                    let deleted = self
                        .cache
                        .delete_hash_item(queue_name, key)
                        .await
                        .map_err(|err| HandlerError::new(err.to_string()))?;
                    if deleted == 0 {
                        return Err(HandlerError::new(format!(
                            "failed to delete hash item {key} for key {queue_name}: expected successful delete but redis deleted nothing"
                        )));
                    }
                    tracing::debug!(queue = %queue_name, key = %key, "Deleted stale dequeue cache key");
                } else {
                    tracing::debug!(queue = %queue_name, key = %key, "Preserved dequeue cache key");
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Handler for MultiqueueCacheCleaner {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        self.clean().await
    }
}

/// Unix nanoseconds of a `SystemTime` (negative before the epoch).
fn unix_nanos(time: SystemTime) -> i128 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => after.as_nanos() as i128,
        Err(before) => -(before.duration().as_nanos() as i128),
    }
}

/// One `prometheus.NewGaugeFunc` per queue, scraped from Redis.
///
/// Public so the differential harness can register against its diff-only
/// cache prefix exactly like the Go `initMetrics` call does.
pub fn register_cache_size_metrics(cache: &Cache) {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    if REGISTERED.set(()).is_err() {
        return;
    }

    for queue in VALID_QUEUE_NAMES {
        let collector = DequeueCacheSizeCollector {
            cache: cache.clone(),
            queue,
            desc: Desc::new(
                CACHE_SIZE_METRIC.to_owned(),
                CACHE_SIZE_HELP.to_owned(),
                Vec::new(),
                HashMap::from([("queue".to_owned(), queue.to_owned())]),
            )
            .expect("valid dequeue cache size desc"),
        };
        // Duplicate registration cannot happen: the collector is created once.
        let _ = sg_metrics::registry().register(Box::new(collector));
    }
}

struct DequeueCacheSizeCollector {
    cache: Cache,
    queue: &'static str,
    desc: Desc,
}

impl Collector for DequeueCacheSizeCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let value = scrape_cache_size(&self.cache, self.queue);

        let mut label = LabelPair::default();
        label.set_name("queue".to_owned());
        label.set_value(self.queue.to_owned());

        let mut gauge = ProtoGauge::default();
        gauge.set_value(value);

        let mut metric = Metric::default();
        metric.set_label(vec![label].into());
        metric.set_gauge(gauge);

        let mut family = MetricFamily::default();
        family.set_name(CACHE_SIZE_METRIC.to_owned());
        family.set_help(CACHE_SIZE_HELP.to_owned());
        family.set_field_type(MetricType::GAUGE);
        family.set_metric(vec![metric].into());
        vec![family]
    }
}

/// The `GaugeFunc` body: `len(GetHashAll(queue))`, 0 on error.
fn scrape_cache_size(cache: &Cache, queue: &str) -> f64 {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return 0.0;
    };
    // `block_in_place` is only available on the multi-threaded runtime; the
    // service always scrapes from it. Other contexts fall back to 0 so a
    // collector never panics the metrics endpoint.
    if !matches!(handle.runtime_flavor(), RuntimeFlavor::MultiThread) {
        return 0.0;
    }

    tokio::task::block_in_place(|| {
        handle.block_on(async {
            match cache.get_hash_all(queue).await {
                Ok(all) => all.len() as f64,
                Err(err) => {
                    tracing::error!(queue = %queue, error = %err, "Failed to get cache size");
                    0.0
                }
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests run on parallel threads; the process environment is shared.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env(vars: &[(&str, &str)], f: impl FnOnce()) {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (name, value) in vars {
            std::env::set_var(name, value);
        }
        f();
        for (name, _) in vars {
            std::env::remove_var(name);
        }
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = ExecutorsJanitor::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
        assert_eq!(DEQUEUE_CACHE_PREFIX, "executor_multihandler_dequeues");
        assert_eq!(DEQUEUE_TTL, Duration::from_secs(300));
        assert_eq!(CLEANUP_INTERVAL, Duration::from_secs(30));
        assert_eq!(VALID_QUEUE_NAMES, ["batches", "codeintel"]);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        with_env(
            &[
                ("EXECUTORS_CLEANUP_TASK_INTERVAL", ""),
                ("EXECUTORS_HEARTBEAT_RECORD_MAX_AGE", ""),
                ("EXECUTORS_MULTIQUEUE_CACHE_CLEANUP_INTERVAL", ""),
                ("EXECUTORS_MULTIQUEUE_CACHE_DEQUEUE_TTL", ""),
            ],
            || {
                // An empty value is *not* the default: Go's `get` only falls
                // back when the variable is unset, so all four become zero and
                // only emptiness is not an error here (ParseDuration("") is).
                let mut env = EnvReader::new();
                let config = JanitorConfig::default();
                config.load(&mut env);
                assert_eq!(config.snapshot(), JanitorValues::default());
                assert_eq!(
                    config.validate().expect_err("invalid").to_string(),
                    "4 errors occurred:\n\t* invalid duration \"\" for EXECUTORS_CLEANUP_TASK_INTERVAL: time: invalid duration \"\"\n\t* invalid duration \"\" for EXECUTORS_HEARTBEAT_RECORD_MAX_AGE: time: invalid duration \"\"\n\t* invalid duration \"\" for EXECUTORS_MULTIQUEUE_CACHE_CLEANUP_INTERVAL: time: invalid duration \"\"\n\t* invalid duration \"\" for EXECUTORS_MULTIQUEUE_CACHE_DEQUEUE_TTL: time: invalid duration \"\""
                );
            },
        );
    }

    #[test]
    fn config_parses_go_durations_and_reports_bad_values() {
        with_env(
            &[
                ("EXECUTORS_CLEANUP_TASK_INTERVAL", "1h30m"),
                ("EXECUTORS_HEARTBEAT_RECORD_MAX_AGE", "168h"),
                ("EXECUTORS_MULTIQUEUE_CACHE_CLEANUP_INTERVAL", "45s"),
                ("EXECUTORS_MULTIQUEUE_CACHE_DEQUEUE_TTL", "2m"),
            ],
            || {
                let mut env = EnvReader::new();
                let config = JanitorConfig::default();
                config.load(&mut env);
                assert!(config.validate().is_ok());
                assert_eq!(
                    config.snapshot(),
                    JanitorValues {
                        cleanup_task_interval: Duration::from_secs(5400),
                        heartbeat_records_max_age: Duration::from_secs(604_800),
                        cache_cleanup_interval: Duration::from_secs(45),
                        cache_dequeue_ttl: Duration::from_secs(120),
                    }
                );
            },
        );

        with_env(&[("EXECUTORS_CLEANUP_TASK_INTERVAL", "30x")], || {
            let mut env = EnvReader::new();
            let config = JanitorConfig::default();
            config.load(&mut env);
            assert_eq!(
                    config.validate().expect_err("invalid").to_string(),
                    "invalid duration \"30x\" for EXECUTORS_CLEANUP_TASK_INTERVAL: time: invalid duration \"30x\""
                );
        });
    }

    /// With a reachable Redis the job returns both routines; without one it
    /// keeps only the heartbeat janitor (see the module erratum). Exercised
    /// end-to-end by `test_ws/p9-go/run_executors_janitor_diff.sh`; not here,
    /// because `ConnectionManager` retries a dead endpoint for minutes.
    #[test]
    fn routine_metadata_matches_the_baseline() {
        assert_eq!(ROUTINE_HEARTBEAT_JANITOR, "executor.heartbeat-janitor");
        assert_eq!(ROUTINE_CACHE_CLEANER, "executors.multiqueue-cache-cleaner");
        assert_eq!(
            HEARTBEAT_DESCRIPTION,
            "clean up executor heartbeat records for presumed dead executors"
        );
        assert_eq!(
            CLEANER_DESCRIPTION,
            "deletes entries from the dequeue cache older than the configured window"
        );
        assert_eq!(CACHE_SIZE_METRIC, "multiqueue_executor_dequeue_cache_size");
        assert_eq!(
            CACHE_SIZE_HELP,
            "Current size of the executor dequeue cache"
        );
    }
}
