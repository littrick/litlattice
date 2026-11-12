#![forbid(unsafe_code)]

//! Production adapters behind the read-only diagnostic seams (M9.15b).
//!
//! M9.14c introduced [`crate::diagnostics_graphql::DiagnosticsStore`] and left
//! the production implementation unset. This module ports the baseline storage
//! readers and exposes them through small, injectable traits so the
//! differential harnesses can drive them without Redis, gitserver or zoekt:
//!
//! | Seam | Baseline |
//! |---|---|
//! | [`RecorderSource`] + [`get_background_job_infos`] | `internal/goroutine/recorder/{reader.go,common.go}` |
//! | [`GitserverDiskInfoSource`] + [`systems_info`] | `internal/gitserver/client.go:500-572` |
//! | [`ProductionDiagnosticsStore`] | `internal/repos/status_messages.go`, `cmd/frontend/graphqlbackend/repository_stats.go` |
//! | [`IndexedStatsSource`] | `internal/search/env.go:80-94`, `internal/search/backend/horizontal.go:122-176` |
//!
//! Erratta (see the slice overview):
//!
//! * `SystemsInfo` overlaps the `DiskInfo` RPCs and `errors.Join`s the errors
//!   in completion order; [`systems_info`] runs them in address order, so the
//!   success set and joined error set match while the order is deterministic.
//! * Go's background-jobs reader panics for a negative `recentRunCount`
//!   (`slice[:negative]`); [`get_background_job_infos`] clamps it to zero.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::{Date, Duration, Month, OffsetDateTime, Time, UtcOffset};

use sg_config::LiveConfig;
use sg_db::repo_statistics::RepoStatistics;
use sg_git::GitserverClient;

use crate::diagnostics_graphql::{
    BackgroundJobData, DiagnosticsStore, GitserverInfoData, RecentRunData, RepositoryStatsData,
    RoutineData, RoutineInstanceData, RoutineStatsData, StatusMessageInputs, SyncErrorInput,
    ZoektStats, DAY_COUNT_FOR_STATS,
};
use crate::search_env::{list_all_indexed, IndexedJobStreamer};

// ------------------------------------------------------------------ constants

/// `recorder.keyPrefix` (`internal/goroutine/recorder/common.go:71`).
pub const KEY_PREFIX: &str = "background-job-logger";
/// `recorder.ttlSeconds` (`common.go:69`).
pub const TTL_SECONDS: i64 = 604800;
/// `recorder.maxRecentRunsLength` (`recorder.go:40`).
pub const MAX_RECENT_RUNS: i64 = 100;
/// `recorder.seenTimeout` (`recorder.go:35`).
pub const SEEN_TIMEOUT: Duration = Duration::days(6);

// ------------------------------------------------------------ recorder reader

/// The subset of `rcache.Cache` the background-jobs reader uses. Production
/// wraps [`sg_rcache::Cache`]; tests/differentials use an in-memory map.
#[async_trait]
pub trait RecorderSource: Send + Sync {
    /// `Cache.GetHashAll`.
    async fn hash_all(&self, key: &str) -> Result<HashMap<String, String>, String>;
    /// `Cache.Get` decoded as a string.
    async fn get(&self, key: &str) -> Result<Option<String>, String>;
    /// `Cache.FIFOList(key, maxRecentRunsLength).Slice(start, end)`; `end` is
    /// inclusive like Redis `LRANGE`.
    async fn fifo_slice(&self, key: &str, start: i64, end: i64) -> Result<Vec<String>, String>;
}

/// Production reader over the shared Redis cache pool (`recorder.GetCache`).
pub struct RedisRecorderSource {
    cache: sg_rcache::Cache,
}

impl RedisRecorderSource {
    pub fn new(cache: sg_rcache::Cache) -> Self {
        Self { cache }
    }

    /// `recorder.GetCache()` for an already-connected Redis pool.
    pub fn from_key_value(kv: sg_redis::KeyValue) -> Self {
        Self::new(sg_rcache::Cache::new_with_ttl(
            kv,
            KEY_PREFIX,
            TTL_SECONDS,
        ))
    }
}

#[async_trait]
impl RecorderSource for RedisRecorderSource {
    async fn hash_all(&self, key: &str) -> Result<HashMap<String, String>, String> {
        self.cache
            .get_hash_all(key)
            .await
            .map_err(|error| error.to_string())
    }

    async fn get(&self, key: &str) -> Result<Option<String>, String> {
        self.cache
            .get_string(key)
            .await
            .map_err(|error| error.to_string())
    }

    async fn fifo_slice(&self, key: &str, start: i64, end: i64) -> Result<Vec<String>, String> {
        self.cache
            .fifo_list(key, MAX_RECENT_RUNS)
            .slice(start, end)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| String::from_utf8_lossy(&item).into_owned())
                    .collect()
            })
            .map_err(|error| error.to_string())
    }
}

/// A source used when the cache pool could not be reached at startup. The
/// baseline's `rcache.Cache` exists unconditionally and surfaces the Redis
/// error per read; this does the same for background jobs while letting the
/// other diagnostic fields keep working.
pub struct UnavailableRecorderSource {
    message: String,
}

impl UnavailableRecorderSource {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
impl RecorderSource for UnavailableRecorderSource {
    async fn hash_all(&self, _key: &str) -> Result<HashMap<String, String>, String> {
        Err(self.message.clone())
    }

    async fn get(&self, _key: &str) -> Result<Option<String>, String> {
        Err(self.message.clone())
    }

    async fn fifo_slice(&self, _key: &str, _start: i64, _end: i64) -> Result<Vec<String>, String> {
        Err(self.message.clone())
    }
}

/// In-memory [`RecorderSource`] for unit tests and the differential harness.
///
/// Hash values, plain values and FIFO lists are seeded explicitly; list seeds
/// are stored newest first, matching `LRANGE` order.
#[derive(Default)]
pub struct InMemoryRecorderSource {
    hashes: Mutex<HashMap<String, HashMap<String, String>>>,
    values: Mutex<HashMap<String, String>>,
    lists: Mutex<HashMap<String, Vec<String>>>,
}

impl InMemoryRecorderSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_hash(&self, key: &str, field: &str, value: &str) {
        self.hashes
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default()
            .insert(field.to_owned(), value.to_owned());
    }

    pub fn set_value(&self, key: &str, value: &str) {
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_owned());
    }

    /// Seed a FIFO list in `LRANGE` order (newest first).
    pub fn set_list(&self, key: &str, items: Vec<String>) {
        self.lists.lock().unwrap().insert(key.to_owned(), items);
    }
}

#[async_trait]
impl RecorderSource for InMemoryRecorderSource {
    async fn hash_all(&self, key: &str) -> Result<HashMap<String, String>, String> {
        Ok(self.hashes.lock().unwrap().get(key).cloned().unwrap_or_default())
    }

    async fn get(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    async fn fifo_slice(&self, key: &str, start: i64, end: i64) -> Result<Vec<String>, String> {
        let lists = self.lists.lock().unwrap();
        let items = lists.get(key).cloned().unwrap_or_default();
        let len = items.len() as i64;
        if len == 0 {
            return Ok(Vec::new());
        }
        let start = if start < 0 { (len + start).max(0) } else { start };
        let end = if end < 0 { len + end } else { end };
        if start > end || start >= len {
            return Ok(Vec::new());
        }
        let end = end.min(len - 1);
        let mut slice = items[start as usize..=end as usize].to_vec();
        if slice.len() as i64 > MAX_RECENT_RUNS {
            slice.truncate(MAX_RECENT_RUNS as usize);
        }
        Ok(slice)
    }
}

/// `serializableRoutineInfo` (`internal/goroutine/recorder/common.go`).
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct SerializableRoutineInfo {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "type")]
    routine_type: String,
    #[serde(default, rename = "jobName")]
    job_name: String,
    #[serde(default)]
    description: String,
    /// `time.Duration` marshals as nanoseconds.
    #[serde(default)]
    interval: i64,
}

/// `recorder.RoutineRun` JSON.
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct RoutineRunJson {
    #[serde(default)]
    at: String,
    #[serde(default)]
    hostname: String,
    #[serde(default, rename = "durationMs")]
    duration_ms: i32,
    #[serde(default, rename = "errorMessage")]
    error_message: String,
}

/// `recorder.RoutineRunStats` JSON (`since` is a `time.Time`).
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct RoutineRunStatsJson {
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

/// `time.Time{}`: January 1, year 1, 00:00:00 UTC.
fn zero_time() -> OffsetDateTime {
    OffsetDateTime::new_utc(
        Date::from_calendar_date(1, Month::January, 1).expect("year 1 is representable"),
        Time::MIDNIGHT,
    )
}

fn parse_rfc3339(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())
}

/// `time.Time.IsZero` after parsing; the zero time maps to `None`.
fn parse_time_or_zero(value: &str) -> Result<Option<OffsetDateTime>, String> {
    let parsed = parse_rfc3339(value)?;
    Ok(if parsed == zero_time() {
        None
    } else {
        Some(parsed)
    })
}

fn parse_optional_time(value: Option<String>) -> Result<Option<OffsetDateTime>, String> {
    match value {
        Some(value) => parse_time_or_zero(&value),
        None => Ok(None),
    }
}

/// Parse a `RoutineRunStats` JSON value. An empty/absent/zero `since` is the
/// zero time, which maps to `None` like `gqlutil.DateTimeOrNil`.
fn parse_run_stats(raw: &str) -> Result<RoutineStatsData, String> {
    let parsed: RoutineRunStatsJson = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    let since = if parsed.since.is_empty() {
        None
    } else {
        parse_time_or_zero(&parsed.since)?
    };
    Ok(RoutineStatsData {
        since,
        run_count: parsed.run_count,
        error_count: parsed.error_count,
        min_duration_ms: parsed.min_duration_ms,
        avg_duration_ms: parsed.avg_duration_ms,
        max_duration_ms: parsed.max_duration_ms,
    })
}

/// `mergeStats` (`common.go:81-119`), including its `since` quirk: when both
/// inputs are non-zero and `a >= b`, the merged `since` stays zero (the caller
/// then stamps the current day).
fn merge_stats(a: &RoutineStatsData, b: &RoutineStatsData) -> RoutineStatsData {
    let mut since = None;
    if a.since.is_none() {
        since = b.since;
    }
    if b.since.is_none() {
        since = a.since;
    }
    if let (Some(a_since), Some(b_since)) = (a.since, b.since) {
        if a_since < b_since {
            since = Some(a_since);
        }
    }
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
    RoutineStatsData {
        since,
        run_count: a.run_count + b.run_count,
        error_count: a.error_count + b.error_count,
        min_duration_ms,
        avg_duration_ms,
        max_duration_ms,
    }
}

fn utc_date_key(now: OffsetDateTime, days_ago: i64) -> (String, OffsetDateTime) {
    let date = (now.to_offset(UtcOffset::UTC) - Duration::days(days_ago))
        .replace_time(time::Time::MIDNIGHT);
    let key = format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    );
    (key, date)
}

async fn get_known_names(
    source: &dyn RecorderSource,
    hash_key: &str,
    label: &str,
    now: OffsetDateTime,
) -> Result<Vec<String>, String> {
    let raw = source.hash_all(hash_key).await?;
    let mut values = Vec::with_capacity(raw.len());
    for (name, last_seen) in raw {
        let last_seen = parse_rfc3339(&last_seen)
            .map_err(|error| format!("failed to parse {label} last seen time: {error}"))?;
        if now - last_seen > SEEN_TIMEOUT {
            continue;
        }
        values.push(name);
    }
    values.sort();
    Ok(values)
}

async fn get_known_routines(
    source: &dyn RecorderSource,
    job_name: &str,
) -> Result<Vec<SerializableRoutineInfo>, String> {
    let raw = source.hash_all("knownRoutines").await?;
    let mut routines = Vec::with_capacity(raw.len());
    for item in raw.values() {
        routines.push(serde_json::from_str::<SerializableRoutineInfo>(item).map_err(|e| e.to_string())?);
    }
    let mut filtered: Vec<SerializableRoutineInfo> = routines
        .into_iter()
        .filter(|routine| routine.job_name == job_name)
        .collect();
    filtered.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(filtered)
}

async fn load_run_stats(
    source: &dyn RecorderSource,
    job_name: &str,
    routine_name: &str,
    now: OffsetDateTime,
    day_count: i32,
) -> Result<RoutineStatsData, String> {
    let mut stats = RoutineStatsData::default();
    for i in 0..day_count.max(0) {
        let (date_key, date) = utc_date_key(now, i64::from(i));
        let key = format!("{job_name}:{routine_name}:runStats:{date_key}");
        if let Some(raw) = source.get(&key).await? {
            let stats_for_day = parse_run_stats(&raw)
                .map_err(|error| format!("deserialize stats for day: {error}"))?;
            let mut merged = merge_stats(&stats, &stats_for_day);
            // The baseline ignores a corrupted (negative) average.
            if merged.avg_duration_ms < 0 {
                merged.avg_duration_ms = stats.avg_duration_ms;
            }
            stats = merged;
            if stats.since.is_none() {
                stats.since = Some(date);
            }
        }
    }
    Ok(stats)
}

async fn get_routine_info(
    source: &dyn RecorderSource,
    routine: &SerializableRoutineInfo,
    all_host_names: &[String],
    recent_run_count: i32,
    day_count: i32,
    now: OffsetDateTime,
) -> Result<RoutineData, String> {
    let mut instances = Vec::with_capacity(all_host_names.len());
    for host_name in all_host_names {
        let prefix = format!("{}:{}:{}", routine.job_name, routine.name, host_name);
        let last_started_at = parse_optional_time(
            source.get(&format!("{prefix}:lastStart")).await?,
        )
        .map_err(|error| format!("parse last start: {error}"))?;
        let last_stopped_at = parse_optional_time(
            source.get(&format!("{prefix}:lastStop")).await?,
        )
        .map_err(|error| format!("parse last stop: {error}"))?;
        instances.push(RoutineInstanceData {
            host_name: host_name.clone(),
            last_started_at,
            last_stopped_at,
        });
    }

    let mut recent_runs = Vec::new();
    for host_name in all_host_names {
        let list_key = format!(
            "{}:{}:{}:recentRuns",
            routine.job_name, routine.name, host_name
        );
        // Go's `Slice(0, count)` is the inclusive `LRANGE 0 count`; the global
        // truncation below applies the actual limit.
        let raw_runs = source
            .fifo_slice(&list_key, 0, i64::from(recent_run_count))
            .await
            .map_err(|error| format!("load recent runs: {error}"))?;
        for raw in raw_runs {
            let run: RoutineRunJson =
                serde_json::from_str(&raw).map_err(|error| format!("deserialize run: {error}"))?;
            let at = if run.at.is_empty() {
                zero_time()
            } else {
                parse_rfc3339(&run.at)
                    .map_err(|error| format!("deserialize run: {error}"))?
            };
            recent_runs.push(RecentRunData {
                at,
                host_name: run.hostname,
                duration_ms: run.duration_ms,
                error_message: run.error_message,
            });
        }
    }
    recent_runs.sort_by(|a, b| b.at.cmp(&a.at));
    // Baseline quirk: a negative `recentRunCount` panics. Clamp instead.
    let limit = usize::try_from(recent_run_count.max(0)).unwrap_or(usize::MAX);
    if recent_runs.len() > limit {
        recent_runs.truncate(limit);
    }

    let stats = load_run_stats(source, &routine.job_name, &routine.name, now, day_count)
        .await
        .map_err(|error| format!("load run stats: {error}"))?;

    Ok(RoutineData {
        name: routine.name.clone(),
        routine_type: routine.routine_type.clone(),
        description: routine.description.clone(),
        interval_ms: (routine.interval / 1_000_000) as i32,
        instances,
        recent_runs,
        stats,
    })
}

async fn get_background_job_info(
    source: &dyn RecorderSource,
    job_name: &str,
    recent_run_count: i32,
    day_count: i32,
    now: OffsetDateTime,
) -> Result<BackgroundJobData, String> {
    // The baseline reads the host set per job, before the routines.
    let all_host_names = get_known_names(source, "knownHostNames", "host", now).await?;
    let routines = get_known_routines(source, job_name).await?;
    let mut routine_infos = Vec::with_capacity(routines.len());
    for routine in &routines {
        routine_infos.push(
            get_routine_info(
                source,
                routine,
                &all_host_names,
                recent_run_count,
                day_count,
                now,
            )
            .await?,
        );
    }
    Ok(BackgroundJobData {
        id: job_name.to_owned(),
        name: job_name.to_owned(),
        routines: routine_infos,
    })
}

/// `recorder.GetBackgroundJobInfos(cache, after, recentRunCount,
/// dayCountForStats)`, with `now` injected for determinism.
///
/// Jobs are ordered by name ascending. The `after` filter keeps the first job
/// whose name sorts after the cursor and everything following it; when no job
/// does, it returns every job (baseline quirk in `reader.go:33-38`).
pub async fn get_background_job_infos(
    source: &dyn RecorderSource,
    after: &str,
    recent_run_count: i32,
    day_count: i32,
    now: OffsetDateTime,
) -> Result<Vec<BackgroundJobData>, String> {
    let job_names = get_known_names(source, "knownJobNames", "job", now)
        .await
        .map_err(|error| format!("get known job names: {error}"))?;

    let mut jobs = Vec::with_capacity(job_names.len());
    for job_name in &job_names {
        let job =
            get_background_job_info(source, job_name, recent_run_count, day_count, now)
        .await
        .map_err(|error| format!("get job info for {job_name:?}: {error}"))?;
        jobs.push(job);
    }

    if !after.is_empty() {
        for (index, job) in jobs.iter().enumerate() {
            if job.name.as_str() > after {
                return Ok(jobs.split_off(index));
            }
        }
    }
    Ok(jobs)
}

// ------------------------------------------------------- gitserver disk info

/// One `gitserver.DiskInfo` response (`proto.DiskInfoResponse`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DiskInfoData {
    pub free_space: u64,
    pub total_space: u64,
    pub percent_used: f32,
}

/// `gitserver.ClientSource` + `DiskInfo` seam.
#[async_trait]
pub trait GitserverDiskInfoSource: Send + Sync {
    /// `ClientSource.Addresses()`.
    async fn addresses(&self) -> Vec<String>;
    /// `getDiskInfo(addr)`, error text already rendered.
    async fn disk_info(&self, address: &str) -> Result<DiskInfoData, String>;
}

/// `clientImplementor.SystemsInfo`: query every address and collect the
/// successful results. Any failure yields the errors joined by `\n` (Go
/// `conc/pool` + `errors.Join`); results are returned in address order.
pub async fn systems_info(
    source: &dyn GitserverDiskInfoSource,
) -> Result<Vec<GitserverInfoData>, String> {
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for address in source.addresses().await {
        match source.disk_info(&address).await {
            Ok(info) => results.push(GitserverInfoData {
                address,
                free_space: info.free_space,
                total_space: info.total_space,
                percent_used_bits: info.percent_used.to_bits(),
            }),
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }
    Ok(results)
}

/// Production `SystemsInfo` source over the live gitserver shard list, reusing
/// the per-address lazy clients repository operations use.
pub struct LiveGitserverDiskInfoSource {
    live: Arc<LiveConfig>,
    clients: tokio::sync::Mutex<HashMap<Vec<String>, Arc<GitserverClient>>>,
}

impl LiveGitserverDiskInfoSource {
    pub fn new(live: Arc<LiveConfig>) -> Self {
        Self {
            live,
            clients: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    fn addresses_now(&self) -> Vec<String> {
        let unified = self.live.current();
        sg_git::addresses::GitserverAddresses::from_unified(&unified).addresses
    }

    async fn client(&self, addresses: Vec<String>) -> Arc<GitserverClient> {
        let mut clients = self.clients.lock().await;
        match clients.get(&addresses) {
            Some(client) => Arc::clone(client),
            None => {
                let client = Arc::new(GitserverClient::new(addresses.clone()));
                clients.insert(addresses, Arc::clone(&client));
                client
            }
        }
    }
}

#[async_trait]
impl GitserverDiskInfoSource for LiveGitserverDiskInfoSource {
    async fn addresses(&self) -> Vec<String> {
        self.addresses_now()
    }

    async fn disk_info(&self, address: &str) -> Result<DiskInfoData, String> {
        let client = self.client(self.addresses_now()).await;
        client
            .disk_info(address)
            .await
            .map(|response| DiskInfoData {
                free_space: response.free_space,
                total_space: response.total_space,
                percent_used: response.percent_used,
            })
            .map_err(|error| error.to_string())
    }
}

// -------------------------------------------------------------- indexed stats

/// `zoekt.RepoStats` subset used by `RepositoryStats.indexed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IndexedStats {
    pub repos: i64,
    pub lines: i64,
}

impl IndexedStats {
    /// `repos.Stats.DefaultBranchNewLinesCount + OtherBranchesNewLinesCount`.
    pub fn from_parts(repos: i64, default_branch_lines: u64, other_branches_lines: u64) -> Self {
        Self {
            repos,
            lines: i64::try_from(default_branch_lines)
                .unwrap_or(i64::MAX)
                .saturating_add(i64::try_from(other_branches_lines).unwrap_or(i64::MAX)),
        }
    }
}

/// `search.ListAllIndexed(ctx, search.Indexed())`'s observable result.
#[async_trait]
pub trait IndexedStatsSource: Send + Sync {
    async fn stats(&self) -> Result<IndexedStats, String>;
}

/// Production source: `Indexed()` (the same M9.8ig stack the search runtime
/// uses) through [`list_all_indexed`].
pub struct ZoektIndexedStatsSource {
    streamer: Arc<IndexedJobStreamer>,
}

impl ZoektIndexedStatsSource {
    pub fn new(streamer: Arc<IndexedJobStreamer>) -> Self {
        Self { streamer }
    }
}

#[async_trait]
impl IndexedStatsSource for ZoektIndexedStatsSource {
    async fn stats(&self) -> Result<IndexedStats, String> {
        let all = list_all_indexed(self.streamer.streamer().inner())
            .await
            .map_err(|error| error.to_string())?;
        Ok(IndexedStats::from_parts(
            all.stats.repos as i64,
            all.stats.default_branch_new_lines_count,
            all.stats.other_branches_new_lines_count,
        ))
    }
}

/// With no zoekt endpoints configured, the baseline's `HorizontalSearcher`
/// sees zero clients and `ListAllIndexed` returns zero stats without an error.
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyIndexedStatsSource;

#[async_trait]
impl IndexedStatsSource for EmptyIndexedStatsSource {
    async fn stats(&self) -> Result<IndexedStats, String> {
        Ok(IndexedStats::default())
    }
}

/// An endpoint list the production stack could not build. The baseline's
/// dialer is lazy, so the failure surfaces on every `List`; do the same.
pub struct ErrorIndexedStatsSource {
    message: String,
}

impl ErrorIndexedStatsSource {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
impl IndexedStatsSource for ErrorIndexedStatsSource {
    async fn stats(&self) -> Result<IndexedStats, String> {
        Err(self.message.clone())
    }
}

// ------------------------------------------------------- production store

/// The production [`DiagnosticsStore`]: Postgres readers plus the recorder,
/// gitserver and zoekt seams.
pub struct ProductionDiagnosticsStore {
    pool: sqlx::postgres::PgPool,
    recorder: Arc<dyn RecorderSource>,
    gitservers: Arc<dyn GitserverDiskInfoSource>,
    /// `None` keeps `RepositoryStats.indexed`/`indexedLinesCount` unported
    /// (`Ok(None)`), the M9.14c behaviour.
    indexed: Option<Arc<dyn IndexedStatsSource>>,
}

impl ProductionDiagnosticsStore {
    pub fn new(
        pool: sqlx::postgres::PgPool,
        recorder: Arc<dyn RecorderSource>,
        gitservers: Arc<dyn GitserverDiskInfoSource>,
        indexed: Option<Arc<dyn IndexedStatsSource>>,
    ) -> Self {
        Self {
            pool,
            recorder,
            gitservers,
            indexed,
        }
    }
}

#[async_trait]
impl DiagnosticsStore for ProductionDiagnosticsStore {
    async fn status_message_inputs(
        &self,
        include_zoekt: bool,
    ) -> Result<StatusMessageInputs, String> {
        let sync_errors = sg_db::external_services::get_latest_sync_errors(&self.pool)
            .await
            .map_err(|error| format!("fetching sync errors: {error}"))?
            .into_iter()
            .map(|error| SyncErrorInput {
                message: error.message,
                service_id: error.service_id,
            })
            .collect::<Vec<_>>();

        let stats: RepoStatistics = sg_db::repo_statistics::get_repo_statistics(&self.pool)
            .await
            .map_err(|error| format!("loading repo statistics: {error}"))?;

        // `FetchStatusMessages` returns before the zoekt query when there are
        // no sync errors, and skips it entirely on dotcom.
        let zoekt = if !sync_errors.is_empty() && include_zoekt {
            let zoekt = sg_db::zoekt_repos::get_statistics(&self.pool)
                .await
                .map_err(|error| format!("loading repo statistics: {error}"))?;
            Some(ZoektStats {
                total: zoekt.total,
                indexed: i32::try_from(zoekt.indexed).unwrap_or(i32::MAX),
                not_indexed: i32::try_from(zoekt.not_indexed).unwrap_or(i32::MAX),
            })
        } else {
            None
        };

        Ok(StatusMessageInputs {
            sync_errors,
            stats,
            zoekt,
        })
    }

    async fn repository_stats(&self) -> RepositoryStatsData {
        let stats = sg_db::repo_statistics::get_repo_statistics(&self.pool)
            .await
            .map_err(|error| error.to_string());
        let git_dir_bytes = sg_db::gitserver_repos::get_gitserver_git_dir_size(&self.pool)
            .await
            .map_err(|error| error.to_string());
        let embedded = sg_db::repo_embeddings::count_repo_embeddings(&self.pool)
            .await
            .map(|count| i32::try_from(count).unwrap_or(i32::MAX))
            .map_err(|error| error.to_string());
        let indexed = match &self.indexed {
            None => None,
            Some(source) => Some(source.stats().await),
        };
        RepositoryStatsData {
            stats,
            git_dir_bytes,
            embedded,
            indexed,
        }
    }

    async fn background_jobs(
        &self,
        after: &str,
        recent_run_count: i32,
    ) -> Result<Vec<BackgroundJobData>, String> {
        get_background_job_infos(
            self.recorder.as_ref(),
            after,
            recent_run_count,
            DAY_COUNT_FOR_STATS,
            OffsetDateTime::now_utc(),
        )
        .await
    }

    async fn gitservers(&self) -> Result<Vec<GitserverInfoData>, String> {
        systems_info(self.gitservers.as_ref()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> OffsetDateTime {
        OffsetDateTime::parse("2026-11-12T12:00:00Z", &Rfc3339).expect("now")
    }

    fn seed_routine(source: &InMemoryRecorderSource, job: &str, routine: &str) {
        source.set_hash(
            "knownRoutines",
            &format!("{job}:{routine}"),
            &format!(
                r#"{{"name":"{routine}","type":"PERIODIC","jobName":"{job}","description":"d","interval":90000000000}}"#
            ),
        );
    }

    #[tokio::test]
    async fn reads_jobs_like_the_baseline() {
        let source = InMemoryRecorderSource::new();
        source.set_hash("knownJobNames", "zeta", "2026-11-12T10:00:00Z");
        source.set_hash("knownJobNames", "alpha", "2026-11-12T10:00:00Z");
        // Older than the 6-day window: filtered out.
        source.set_hash("knownJobNames", "stale", "2026-11-01T10:00:00Z");
        source.set_hash("knownHostNames", "host-b", "2026-11-12T10:00:00Z");
        source.set_hash("knownHostNames", "host-a", "2026-11-12T10:00:00Z");
        seed_routine(&source, "alpha", "r1");
        source.set_value("alpha:r1:host-a:lastStart", "2026-11-12T11:00:00Z");
        source.set_list(
            "alpha:r1:host-a:recentRuns",
            vec![
                r#"{"at":"2026-11-12T11:30:00Z","hostname":"host-a","durationMs":10,"errorMessage":""}"#
                    .to_owned(),
                r#"{"at":"2026-11-12T11:20:00Z","hostname":"host-a","durationMs":20,"errorMessage":"boom"}"#
                    .to_owned(),
            ],
        );
        source.set_value(
            "alpha:r1:runStats:2026-11-12",
            r#"{"since":"2026-11-12T09:00:00Z","runCount":2,"errorCount":1,"minDurationMs":10,"avgDurationMs":15,"maxDurationMs":20}"#,
        );

        let jobs = get_background_job_infos(&source, "", 5, 7, now())
            .await
            .expect("jobs");
        assert_eq!(
            jobs.iter().map(|job| job.name.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        let alpha = &jobs[0];
        assert_eq!(alpha.routines.len(), 1);
        let routine = &alpha.routines[0];
        assert_eq!(routine.interval_ms, 90_000);
        assert_eq!(routine.instances.len(), 2);
        assert_eq!(routine.instances[0].host_name, "host-a");
        assert_eq!(
            routine.instances[0].last_started_at,
            Some(
                OffsetDateTime::parse("2026-11-12T11:00:00Z", &Rfc3339).expect("last start")
            )
        );
        assert_eq!(routine.recent_runs.len(), 2);
        assert_eq!(routine.recent_runs[0].duration_ms, 10);
        assert_eq!(routine.stats.run_count, 2);
        assert_eq!(routine.stats.since, Some(
            OffsetDateTime::parse("2026-11-12T09:00:00Z", &Rfc3339).expect("since")
        ));
    }

    #[tokio::test]
    async fn after_filters_and_no_match_returns_everything() {
        let source = InMemoryRecorderSource::new();
        for job in ["a", "b", "c"] {
            source.set_hash("knownJobNames", job, "2026-11-12T10:00:00Z");
        }
        let jobs = get_background_job_infos(&source, "a", 5, 7, now())
            .await
            .expect("jobs");
        assert_eq!(
            jobs.iter().map(|job| job.name.as_str()).collect::<Vec<_>>(),
            vec!["b", "c"]
        );
        let all = get_background_job_infos(&source, "zzz", 5, 7, now())
            .await
            .expect("jobs");
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn negative_recent_runs_are_clamped_not_panicking() {
        let source = InMemoryRecorderSource::new();
        source.set_hash("knownJobNames", "j", "2026-11-12T10:00:00Z");
        source.set_hash("knownHostNames", "h", "2026-11-12T10:00:00Z");
        seed_routine(&source, "j", "r");
        source.set_list(
            "j:r:h:recentRuns",
            vec![r#"{"at":"2026-11-12T11:30:00Z"}"#.to_owned()],
        );
        let jobs = get_background_job_infos(&source, "", -3, 7, now())
            .await
            .expect("jobs");
        assert!(jobs[0].routines[0].recent_runs.is_empty());
    }

    struct FakeDisks {
        addresses: Vec<String>,
        infos: HashMap<String, Result<DiskInfoData, String>>,
    }

    #[async_trait]
    impl GitserverDiskInfoSource for FakeDisks {
        async fn addresses(&self) -> Vec<String> {
            self.addresses.clone()
        }

        async fn disk_info(&self, address: &str) -> Result<DiskInfoData, String> {
            self.infos.get(address).cloned().unwrap_or(Ok(DiskInfoData::default()))
        }
    }

    #[tokio::test]
    async fn systems_info_aggregates_in_address_order() {
        let source = FakeDisks {
            addresses: vec!["b".to_owned(), "a".to_owned()],
            infos: [
                (
                    "a".to_owned(),
                    Ok(DiskInfoData {
                        free_space: 1,
                        total_space: 10,
                        percent_used: 90.0,
                    }),
                ),
                (
                    "b".to_owned(),
                    Ok(DiskInfoData {
                        free_space: 2,
                        total_space: 20,
                        percent_used: 10.0,
                    }),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let infos = systems_info(&source).await.expect("infos");
        assert_eq!(
            infos.iter().map(|info| info.address.as_str()).collect::<Vec<_>>(),
            vec!["b", "a"]
        );
        assert_eq!(infos[0].percent_used(), 10.0);
    }

    #[tokio::test]
    async fn systems_info_joins_errors() {
        let source = FakeDisks {
            addresses: vec!["a".to_owned(), "b".to_owned()],
            infos: [
                ("a".to_owned(), Err("first".to_owned())),
                ("b".to_owned(), Err("second".to_owned())),
            ]
            .into_iter()
            .collect(),
        };
        assert_eq!(systems_info(&source).await, Err("first\nsecond".to_owned()));
    }

    #[test]
    fn indexed_stats_sums_line_counts() {
        assert_eq!(
            IndexedStats::from_parts(3, 100, 20),
            IndexedStats { repos: 3, lines: 120 }
        );
    }
}
