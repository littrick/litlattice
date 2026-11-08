#![forbid(unsafe_code)]

//! `internal/goroutine/recorder`: the `background-job-logger` that publishes
//! every routine's lifecycle and run history to Redis.
//!
//! Contract reproduced from the baseline:
//!
//! * the recorder cache is `rcache.NewWithTTL(redispool.Cache,
//!   "background-job-logger", 604800)`, so every key lives under
//!   `v2:background-job-logger:` (FIFO list keys under
//!   `v2:v2:background-job-logger:`);
//! * `knownJobNames`, `knownHostNames` and `knownRoutines` are hashes
//!   (`SaveKnown*`), `knownRoutines` items are compact JSON with `name`,
//!   `type`, `jobName`, `description` and `interval` (nanoseconds);
//! * `<job>:<routine>:<host>:lastStart` / `lastStop` are values written with
//!   the cache TTL (RFC3339, second precision);
//! * `<job>:<routine>:<host>:recentRuns` is a FIFO list capped at 100 runs of
//!   `{"at","hostname","durationMs","errorMessage"}`;
//! * `<job>:<routine>:runStats:<YYYY-MM-DD>` holds
//!   `{"since","runCount","errorCount","minDurationMs","avgDurationMs",
//!   "maxDurationMs"}` merged with `math.Round` for the average.
//!
//! Errata vs. the Go implementation:
//!
//! * writes are awaited instead of fire-and-forget goroutines (same ordering,
//!   stronger backpressure, sub-millisecond cost);
//! * timestamps are always emitted in UTC (`Z`); Go emits the local offset;
//! * an unreachable Redis only disables the recorder (warning) instead of
//!   failing every write, because the connection is established eagerly.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sg_rcache::Cache;
use time::OffsetDateTime;

/// `recorder.keyPrefix`.
pub const KEY_PREFIX: &str = "background-job-logger";
/// `recorder.ttlSeconds` (7 days).
pub const TTL_SECONDS: i64 = 604800;
/// `recorder.maxRecentRunsLength`.
pub const MAX_RECENT_RUNS: i64 = 100;

/// `recorder.RoutineType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutineType {
    Periodic,
    PeriodicWithMetrics,
    DbBacked,
    Custom,
}

impl RoutineType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Periodic => "PERIODIC",
            Self::PeriodicWithMetrics => "PERIODIC_WITH_METRICS",
            Self::DbBacked => "DB_BACKED",
            Self::Custom => "CUSTOM",
        }
    }
}

/// `recorder.Recordable`. `set_job_name` and `register_recorder` take `&self`
/// because routines are shared as `Arc`s; the baseline mutates them before
/// they start.
pub trait Recordable: Send + Sync {
    fn name(&self) -> &str;
    fn routine_type(&self) -> RoutineType;
    fn job_name(&self) -> String;
    fn set_job_name(&self, job_name: &str);
    fn description(&self) -> &str;
    fn interval(&self) -> Duration;
    fn register_recorder(&self, recorder: Arc<Recorder>);
}

/// `recorder.Recorder`.
pub struct Recorder {
    cache: Cache,
    host_name: String,
    recordables: Mutex<Vec<Arc<dyn Recordable>>>,
}

/// `serializableRoutineInfo`; field order matches the Go struct.
#[derive(Serialize)]
struct SerializableRoutineInfo<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    routine_type: &'a str,
    #[serde(rename = "jobName")]
    job_name: &'a str,
    description: &'a str,
    /// `time.Duration` marshals as nanoseconds.
    interval: i64,
}

/// `recorder.RoutineRun`.
#[derive(Serialize)]
struct RoutineRunJson<'a> {
    at: &'a str,
    hostname: &'a str,
    #[serde(rename = "durationMs")]
    duration_ms: i32,
    #[serde(rename = "errorMessage")]
    error_message: &'a str,
}

/// `recorder.RoutineRunStats`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RoutineRunStats {
    #[serde(default)]
    since: String,
    #[serde(default, rename = "runCount")]
    run_count: i32,
    #[serde(default, rename = "errorCount")]
    error_count: i32,
    #[serde(default, rename = "minDurationMs")]
    min_duration_ms: i32,
    #[serde(default, rename = "avgDurationMs")]
    avg_duration_ms: i32,
    #[serde(default, rename = "maxDurationMs")]
    max_duration_ms: i32,
}

impl Recorder {
    /// `recorder.New`.
    pub fn new(cache: Cache, host_name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            cache,
            host_name: host_name.into(),
            recordables: Mutex::new(Vec::new()),
        })
    }

    /// `recorder.GetCache` for an already-connected pool.
    pub fn cache(kv: sg_redis::KeyValue) -> Cache {
        Cache::new_with_ttl(kv, KEY_PREFIX, TTL_SECONDS)
    }

    /// `env.MyName`: the executable's file name (`worker`).
    pub fn process_name() -> String {
        std::env::current_exe()
            .ok()
            .and_then(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| "worker".to_owned())
    }

    pub fn host_name(&self) -> &str {
        &self.host_name
    }

    /// `Register`.
    pub fn register(&self, recordable: Arc<dyn Recordable>) {
        self.recordables
            .lock()
            .expect("recorder registry lock")
            .push(recordable);
    }

    /// `RegistrationDone`: save the known job names, the host name and every
    /// registered routine.
    pub async fn registration_done(&self) {
        let recordables: Vec<Arc<dyn Recordable>> = self
            .recordables
            .lock()
            .expect("recorder registry lock")
            .clone();

        for job_name in collect_all_job_names(&recordables) {
            self.save_known_job_name(&job_name).await;
        }
        self.save_known_host_name().await;
        for recordable in recordables {
            self.save_known_routine(recordable.as_ref()).await;
        }
    }

    /// `SaveKnownRoutine`.
    pub async fn save_known_routine(&self, recordable: &dyn Recordable) {
        let job_name = recordable.job_name();
        let routine = SerializableRoutineInfo {
            name: recordable.name(),
            routine_type: recordable.routine_type().as_str(),
            job_name: &job_name,
            description: recordable.description(),
            interval: duration_nanos(recordable.interval()),
        };
        let Ok(json) = serde_json::to_string(&routine) else {
            tracing::error!(
                routine_name = recordable.name(),
                "failed to serialize routine"
            );
            return;
        };
        let key = format!("{}:{}", routine.job_name, routine.name);
        if let Err(err) = self.cache.set_hash_item("knownRoutines", &key, json).await {
            tracing::error!(
                error = %err,
                routine_name = recordable.name(),
                "failed to save/update known routine"
            );
        }
    }

    /// `LogStart`.
    pub async fn log_start(&self, recordable: &dyn Recordable) {
        let key = format!(
            "{}:{}:{}:lastStart",
            recordable.job_name(),
            recordable.name(),
            self.host_name
        );
        if let Err(err) = self.cache.set(&key, now_rfc3339_seconds()).await {
            tracing::error!(error = %err, routine = recordable.name(), "failed to log routine start");
        }
        tracing::debug!(routine = recordable.name(), "Routine just started! 🚀");
    }

    /// `LogStop`.
    pub async fn log_stop(&self, recordable: &dyn Recordable) {
        let key = format!(
            "{}:{}:{}:lastStop",
            recordable.job_name(),
            recordable.name(),
            self.host_name
        );
        if let Err(err) = self.cache.set(&key, now_rfc3339_seconds()).await {
            tracing::error!(error = %err, routine = recordable.name(), "failed to log routine stop");
        }
        tracing::debug!(routine = recordable.name(), "routine just stopped! 🛑");
    }

    /// `LogRun`: append a run to the FIFO list, merge the daily stats and
    /// refresh the job/host "last seen" timestamps.
    pub async fn log_run(
        &self,
        recordable: &dyn Recordable,
        duration: Duration,
        error: Option<&str>,
    ) {
        let job_name = recordable.job_name();
        let routine_name = recordable.name();
        let duration_ms = duration.as_millis() as i32;

        if let Err(err) = self
            .save_run(&job_name, routine_name, duration_ms, error)
            .await
        {
            tracing::error!(error = %err, "failed to save run");
        }
        if let Err(err) = self
            .save_run_stats(&job_name, routine_name, duration_ms, error.is_some())
            .await
        {
            tracing::error!(error = %err, "failed to save run stats");
        }

        self.save_known_host_name().await;
        self.save_known_job_name(&job_name).await;
    }

    async fn save_known_job_name(&self, job_name: &str) {
        if let Err(err) = self
            .cache
            .set_hash_item("knownJobNames", job_name, now_rfc3339_seconds())
            .await
        {
            tracing::error!(
                error = %err,
                job_name = job_name,
                "failed to save/update known job name"
            );
        }
    }

    async fn save_known_host_name(&self) {
        if let Err(err) = self
            .cache
            .set_hash_item("knownHostNames", &self.host_name, now_rfc3339_seconds())
            .await
        {
            tracing::error!(
                error = %err,
                host_name = %self.host_name,
                "failed to save/update known host name"
            );
        }
    }

    async fn save_run(
        &self,
        job_name: &str,
        routine_name: &str,
        duration_ms: i32,
        error: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let at = now_rfc3339_nano();
        let run = RoutineRunJson {
            at: &at,
            hostname: &self.host_name,
            duration_ms,
            error_message: error.unwrap_or(""),
        };
        let json = serde_json::to_string(&run)?;
        self.cache
            .fifo_list(
                &format!("{job_name}:{routine_name}:{}:recentRuns", self.host_name),
                MAX_RECENT_RUNS,
            )
            .insert(json)
            .await?;
        Ok(())
    }

    async fn save_run_stats(
        &self,
        job_name: &str,
        routine_name: &str,
        duration_ms: i32,
        errored: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let iso_date = now_rfc3339_seconds();
        let key = format!("{job_name}:{routine_name}:runStats:{}", &iso_date[..10]);

        let last_stats = match self.cache.get_string(&key).await? {
            Some(raw) => serde_json::from_str::<RoutineRunStats>(&raw)?,
            None => RoutineRunStats::default(),
        };
        let new_stats = add_run_to_stats(last_stats, duration_ms, errored);
        self.cache
            .set(&key, serde_json::to_string(&new_stats)?)
            .await?;
        Ok(())
    }
}

/// `collectAllJobNames`: unique job names in registration order (the baseline
/// iterates a map, so the order only affects which timestamp wins).
fn collect_all_job_names(recordables: &[Arc<dyn Recordable>]) -> Vec<String> {
    let mut names = Vec::new();
    for recordable in recordables {
        let name = recordable.job_name();
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// `addRunToStats`.
fn add_run_to_stats(stats: RoutineRunStats, duration_ms: i32, errored: bool) -> RoutineRunStats {
    merge_stats(
        stats,
        RoutineRunStats {
            since: now_rfc3339_nano(),
            run_count: 1,
            error_count: i32::from(errored),
            min_duration_ms: duration_ms,
            avg_duration_ms: duration_ms,
            max_duration_ms: duration_ms,
        },
    )
}

/// `mergeStats`.
fn merge_stats(a: RoutineRunStats, b: RoutineRunStats) -> RoutineRunStats {
    let since = match (a.since.is_empty(), b.since.is_empty()) {
        (true, _) => b.since.clone(),
        (false, true) => a.since.clone(),
        (false, false) => {
            if timestamp_key(&a.since) < timestamp_key(&b.since) {
                a.since.clone()
            } else {
                b.since.clone()
            }
        }
    };

    let min_duration_ms = if a.min_duration_ms == 0 || b.min_duration_ms < a.min_duration_ms {
        b.min_duration_ms
    } else {
        a.min_duration_ms
    };
    let total_runs = f64::from(a.run_count) + f64::from(b.run_count);
    let avg_duration_ms = if total_runs == 0.0 {
        0
    } else {
        ((f64::from(a.avg_duration_ms) * f64::from(a.run_count)
            + f64::from(b.avg_duration_ms) * f64::from(b.run_count))
            / total_runs)
            .round() as i32
    };
    let max_duration_ms = if b.max_duration_ms > a.max_duration_ms {
        b.max_duration_ms
    } else {
        a.max_duration_ms
    };

    RoutineRunStats {
        since,
        run_count: a.run_count + b.run_count,
        error_count: a.error_count + b.error_count,
        min_duration_ms,
        avg_duration_ms,
        max_duration_ms,
    }
}

fn duration_nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn now_rfc3339_seconds() -> String {
    format_rfc3339_seconds(OffsetDateTime::now_utc())
}

fn now_rfc3339_nano() -> String {
    format_rfc3339_nano(OffsetDateTime::now_utc())
}

fn date_time(timestamp: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second()
    )
}

/// `time.RFC3339` with UTC (`2006-01-02T15:04:05Z`).
fn format_rfc3339_seconds(timestamp: OffsetDateTime) -> String {
    format!("{}Z", date_time(timestamp))
}

/// `time.RFC3339Nano` with UTC and trailing zeros trimmed
/// (`2006-01-02T15:04:05.123456789Z`).
fn format_rfc3339_nano(timestamp: OffsetDateTime) -> String {
    let nanos = timestamp.nanosecond();
    if nanos == 0 {
        return format!("{}Z", date_time(timestamp));
    }
    let fraction = format!("{nanos:09}");
    format!(
        "{}.{}Z",
        date_time(timestamp),
        fraction.trim_end_matches('0')
    )
}

/// Sort key for the timestamps this module writes: the fixed-width seconds
/// prefix plus the fraction padded to nanoseconds.
fn timestamp_key(value: &str) -> (String, u32) {
    if value.len() < 19 {
        return (value.to_owned(), 0);
    }
    let seconds = value[..19].to_owned();
    let rest = &value[19..];
    let nanos = rest
        .strip_prefix('.')
        .map(|fraction| {
            let digits: String = fraction
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            let mut padded = digits;
            while padded.len() < 9 {
                padded.push('0');
            }
            padded[..9].parse::<u32>().unwrap_or(0)
        })
        .unwrap_or(0);
    (seconds, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_match_go_rfc3339() {
        let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_000)
            .expect("valid timestamp")
            .replace_nanosecond(123_456_789)
            .expect("valid nanos");
        assert_eq!(format_rfc3339_seconds(timestamp), "2023-11-14T22:13:20Z");
        assert_eq!(
            format_rfc3339_nano(timestamp),
            "2023-11-14T22:13:20.123456789Z"
        );
        let whole = timestamp.replace_nanosecond(0).expect("valid nanos");
        assert_eq!(format_rfc3339_nano(whole), "2023-11-14T22:13:20Z");
        let trimmed = timestamp
            .replace_nanosecond(500_000_000)
            .expect("valid nanos");
        assert_eq!(format_rfc3339_nano(trimmed), "2023-11-14T22:13:20.5Z");
    }

    #[test]
    fn merge_stats_matches_the_baseline() {
        let a = RoutineRunStats {
            since: "2026-11-08T10:00:00.1Z".to_owned(),
            run_count: 1,
            error_count: 1,
            min_duration_ms: 1500,
            avg_duration_ms: 1500,
            max_duration_ms: 1500,
        };
        let b = RoutineRunStats {
            since: "2026-11-08T10:00:00.2Z".to_owned(),
            run_count: 1,
            error_count: 0,
            min_duration_ms: 500,
            avg_duration_ms: 500,
            max_duration_ms: 500,
        };
        let merged = merge_stats(a, b);
        assert_eq!(merged.since, "2026-11-08T10:00:00.1Z");
        assert_eq!(merged.run_count, 2);
        assert_eq!(merged.error_count, 1);
        assert_eq!(merged.min_duration_ms, 500);
        assert_eq!(merged.avg_duration_ms, 1000);
        assert_eq!(merged.max_duration_ms, 1500);
    }

    #[test]
    fn merge_stats_uses_the_fraction_when_comparing() {
        // Lexicographic comparison would order `.05Z` after `Z`; the padded
        // numeric key does not.
        assert!(
            !(timestamp_key("2026-11-08T10:00:00.05Z") < timestamp_key("2026-11-08T10:00:00Z"))
        );
        assert!(timestamp_key("2026-11-08T10:00:00Z") < timestamp_key("2026-11-08T10:00:00.5Z"));
    }

    #[test]
    fn routine_json_matches_the_baseline_shape() {
        let job_name = "webhook-log-janitor".to_owned();
        let routine = SerializableRoutineInfo {
            name: "batchchanges.webhook-log-janitor",
            routine_type: RoutineType::PeriodicWithMetrics.as_str(),
            job_name: &job_name,
            description: "cleans up stale webhook logs",
            interval: duration_nanos(Duration::from_secs(3600)),
        };
        assert_eq!(
            serde_json::to_string(&routine).expect("serialize"),
            r#"{"name":"batchchanges.webhook-log-janitor","type":"PERIODIC_WITH_METRICS","jobName":"webhook-log-janitor","description":"cleans up stale webhook logs","interval":3600000000000}"#
        );
    }

    #[test]
    fn run_stats_deserialize_go_json() {
        let raw = r#"{"since":"2026-11-08T00:55:29.346056415Z","runCount":12,"errorCount":0,"minDurationMs":6,"avgDurationMs":10,"maxDurationMs":18}"#;
        let stats: RoutineRunStats = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(stats.run_count, 12);
        assert_eq!(stats.min_duration_ms, 6);
        assert_eq!(stats.avg_duration_ms, 10);
        assert_eq!(stats.max_duration_ms, 18);
        assert_eq!(
            serde_json::to_string(&stats).expect("serialize"),
            raw,
            "round-trips with the same field order"
        );
    }
}
