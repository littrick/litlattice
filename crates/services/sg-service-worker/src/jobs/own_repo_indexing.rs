#![forbid(unsafe_code)]

//! `cmd/worker/internal/own/job.go` plus `internal/own/background`:
//! the `own-repo-indexing-queue` job.
//!
//! The job owns no env config. When `own.IsEnabled()` is false
//! (`DISABLE_OWN` parses to true) it returns no routines; otherwise it
//! returns, in the baseline's order:
//!
//! 1. the `own_background_worker` over `own_background_jobs` (view
//!    `own_background_jobs_config_aware`), with `MaxNumRetries` 3,
//!    `StalledMaxAge` 30s, `RetryAfter` 30s, `MaxNumResets` 10,
//!    `NumHandlers` from `own.background.repoIndexConcurrencyLimit`
//!    (default 5), interval 10s and heartbeat 20s;
//! 2. the `own_background_worker_resetter` (interval 20s);
//! 3. the `own-background-jobs-janitor` (interval 5m, 7-day retention);
//! 4. one scheduler routine per [`queue_per_repo_index_jobs`] entry
//!    (`recent-contributors` every 5m, `analytics` every 24h), each named
//!    after the job type, with the feature-flag wrapper (`loadConfig`, skip
//!    when disabled) and the baseline's `ownIndexRepoQuery`.
//!
//! The handler runs before each record is dispatched through an in-process
//! token bucket built from `own.background.repoIndexRateLimit` /
//! `own.background.repoIndexRateBurstLimit` (default 20/s, burst 5), rebuilt
//! from the live configuration on every dispatch like `conf.Watch` does.
//!
//! ## Ported signal indexers
//!
//! `recent-contributors` is ported in full: sub-repo-permissions skip, repo
//! lookup, commits in the last 90 days (`Order: TopoDate`, `Ranges: [HEAD]`),
//! `ClearSignals`, then one `AddCommit` per non-merge commit with its changed
//! files and `commit.Author` identity.
//!
//! ## Deferred (documented gaps, see docs §76)
//!
//! * `analytics` (`internal/own/background/analytics.go`): the handler returns
//!   a non-retryable "not ported yet" failure so the record does not spin.
//! * `recent-views` (`internal/own/background/recent_views.go`): its routine is
//!   not registered. It needs `event_logs_scrape_state_own`, which has no Rust
//!   store yet; the job type never appears in `QueuePerRepoIndexJobs`, so no
//!   queue records are created for it either.
//!
//! ## Errata
//!
//! * `rate.NewLimiter` panics on a negative burst; the Rust limiter clamps the
//!   burst to zero and fails `Wait` instead. A non-positive rate waits in 1s
//!   slices so cancellation is still observed (the Go limiter would block
//!   until the context deadline).
//! * `Commit.Author.Date` is optional in the gitserver protobuf; when it is
//!   absent the commit timestamp falls back to the Unix epoch (`time.Time`'s
//!   year-1 zero value is not representable here). Gitserver always sends a
//!   date for parsed signatures, so this only affects malformed history.
//! * A sub-repo-permissions store error is reported as
//!   `sub-repo permissions check: <err>`; the baseline passes the raw store
//!   error through `errcode.MakeNonRetryable`. The message is the only way the
//!   generic worker learns "non-retryable", hence the prefix.
//! * Not ported: RED metrics, the repo/commit counters, log lines and
//!   tracing spans. The store's debug-info lookup for unknown heartbeat IDs
//!   is also omitted (the framework already logs the unknown id).
//! * The Rust worker does not watch the site configuration to resize the
//!   handler pool; `NumHandlers` is read once when the routines are built, the
//!   same moment the baseline calls `getConcurrencyConfig()`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prost_types::Timestamp;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_db::OffsetDateTime;
use sg_git::{CommitsOptions, CommitsOrder, GitserverClient};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::env::parse_go_bool;
use crate::framework::dbworker::{
    default_hostname, DbBackedRoutine, DbRecord, DbResetter, DbWorkerHandler, DbWorkerStore,
    ResetterOptions, ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `ownRepoIndexingQueue.Description`.
pub const JOB_DESCRIPTION: &str = "Queue used to index ownership data partitioned per repository";

/// `types.SignalRecentContributors`.
pub const SIGNAL_RECENT_CONTRIBUTORS: &str = "recent-contributors";

/// `types.Analytics`.
pub const SIGNAL_ANALYTICS: &str = "analytics";

/// `tableName`.
pub const TABLE_NAME: &str = "own_background_jobs";

/// `viewName`.
pub const VIEW_NAME: &str = "own_background_jobs_config_aware";

/// `dbworkerstore.Options.Name` in `makeWorkerStore`.
pub const STORE_NAME: &str = "own_background_worker_store";

/// `workerutil.WorkerOptions.Name`.
pub const WORKER_NAME: &str = "own_background_worker";

/// `workerutil.WorkerOptions.Description`.
pub const WORKER_DESCRIPTION: &str =
    "Code ownership background processing partitioned by repository";

/// `workerutil.WorkerOptions.Interval`.
pub const WORKER_INTERVAL: Duration = Duration::from_secs(10);

/// `workerutil.WorkerOptions.HeartbeatInterval`.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

/// `dbworker.ResetterOptions.Name`.
pub const RESETTER_NAME: &str = "own_background_worker_resetter";

/// `dbworker.ResetterOptions.Interval`.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(20);

/// `background.JanitorOptions.Name`.
pub const JANITOR_NAME: &str = "own-background-jobs-janitor";

/// `background.JanitorOptions.Description`.
pub const JANITOR_DESCRIPTION: &str = "Janitor for own-background-jobs queue";

/// `background.JanitorOptions.Interval`.
pub const JANITOR_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// `janitorFunc(db, time.Hour*24*7)`.
pub const JANITOR_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// `dbworkerstore.Options.MaxNumResets`.
pub const MAX_NUM_RESETS: i32 = 10;

/// `dbworkerstore.Options.StalledMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(30);

/// `dbworkerstore.Options.RetryAfter`.
pub const RETRY_AFTER: Duration = Duration::from_secs(30);

/// `dbworkerstore.Options.MaxNumRetries`.
pub const MAX_NUM_RETRIES: i32 = 3;

/// `defaultResetFailureMessage`.
pub const DEFAULT_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// `ownIndexRepoQuery`, verbatim.
pub const OWN_INDEX_REPO_QUERY: &str = r#"
WITH signal_config AS (SELECT * FROM own_signal_configurations WHERE name = $1 LIMIT 1),
     ineligible_repos AS (SELECT repo_id
                          FROM own_background_jobs,
                               signal_config
                          WHERE job_type = signal_config.id
                              AND (state IN ('failed', 'completed') AND finished_at > $2) OR (state IN ('processing', 'errored', 'queued'))
                          UNION
                            SELECT repo.id FROM repo, signal_config WHERE repo.name ~~ ANY(signal_config.excluded_repo_patterns))
INSERT
INTO own_background_jobs (repo_id, job_type) (SELECT gr.repo_id, signal_config.id
                                              FROM gitserver_repos gr,
                                                   signal_config
                                              WHERE gr.repo_id NOT IN (SELECT * FROM ineligible_repos)
                                                AND gr.clone_status = 'cloned');"#;

/// `janitorQuery`.
pub const JANITOR_QUERY: &str = "DELETE FROM own_background_jobs \
     WHERE (state NOT IN ('queued', 'processing', 'errored') AND finished_at < $1) \
     OR (id NOT IN (select id from own_background_jobs_config_aware))";

/// `DefaultRateLimit`.
pub const DEFAULT_RATE_LIMIT: f64 = 20.0;

/// `DefaultRateBurstLimit`.
pub const DEFAULT_RATE_BURST_LIMIT: usize = 5;

/// `DefaultMaxConcurrency`.
pub const DEFAULT_MAX_CONCURRENCY: usize = 5;

/// The failure returned for `analytics` while the indexer is not ported.
pub const ANALYTICS_NOT_PORTED: &str =
    "own analytics indexer is not ported yet: unsupported own index job type";

/// `own.IsEnabled`.
pub fn own_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("DISABLE_OWN") {
        Ok(raw) => !parse_go_bool(&raw).unwrap_or(false),
        Err(_) => true,
    })
}

/// Testable form of `own.IsEnabled`.
pub fn own_enabled_from(raw: Result<String, std::env::VarError>) -> bool {
    match raw {
        Ok(raw) => !parse_go_bool(&raw).unwrap_or(false),
        Err(_) => true,
    }
}

/// `IndexJobType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexJobType {
    pub name: &'static str,
    pub index_interval: Duration,
    pub refresh_interval: Duration,
}

/// `QueuePerRepoIndexJobs` (minus `recent-views`, which has no routine yet).
pub const RECENT_CONTRIBUTORS_JOB: IndexJobType = IndexJobType {
    name: SIGNAL_RECENT_CONTRIBUTORS,
    index_interval: Duration::from_secs(24 * 60 * 60),
    refresh_interval: Duration::from_secs(5 * 60),
};

/// `QueuePerRepoIndexJobs[1]`.
pub const ANALYTICS_JOB: IndexJobType = IndexJobType {
    name: SIGNAL_ANALYTICS,
    index_interval: Duration::from_secs(24 * 60 * 60),
    refresh_interval: Duration::from_secs(24 * 60 * 60),
};

/// `QueuePerRepoIndexJobs`.
pub fn queue_per_repo_index_jobs() -> Vec<IndexJobType> {
    vec![RECENT_CONTRIBUTORS_JOB, ANALYTICS_JOB]
}

fn int_field(raw: &serde_json::Value, key: &str) -> Option<i64> {
    raw.get(key).and_then(serde_json::Value::as_i64)
}

/// `getConcurrencyConfig`.
pub fn index_concurrency_limit(site: &SiteConfiguration) -> usize {
    let value = int_field(site.raw(), "own.background.repoIndexConcurrencyLimit").unwrap_or(0);
    if value == 0 {
        DEFAULT_MAX_CONCURRENCY
    } else {
        usize::try_from(value).unwrap_or(0)
    }
}

/// `(rate.Limit, int)` returned by `getRateLimitConfig`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateLimitConfig {
    /// Events per second; may be zero.
    pub limit: f64,
    pub burst: usize,
}

/// `getRateLimitConfig`.
pub fn rate_limit_config(site: &SiteConfiguration) -> RateLimitConfig {
    let raw = site.raw();
    let mut limit = int_field(raw, "own.background.repoIndexRateLimit").unwrap_or(0);
    let mut burst = int_field(raw, "own.background.repoIndexRateBurstLimit").unwrap_or(0);
    if limit == 0 {
        limit = DEFAULT_RATE_LIMIT as i64;
    }
    if burst == 0 {
        burst = DEFAULT_RATE_BURST_LIMIT as i64;
    }
    RateLimitConfig {
        limit: limit as f64,
        burst: usize::try_from(burst).unwrap_or(0),
    }
}

struct BucketState {
    tokens: f64,
    last: Instant,
}

/// A minimal `golang.org/x/time/rate.Limiter` equivalent: token bucket with
/// `Wait` blocking until one token is available.
pub struct OwnRateLimiter {
    state: Mutex<BucketState>,
    limit: Mutex<f64>,
    burst: Mutex<usize>,
}

impl OwnRateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            state: Mutex::new(BucketState {
                tokens: config.burst as f64,
                last: Instant::now(),
            }),
            limit: Mutex::new(config.limit),
            burst: Mutex::new(config.burst),
        }
    }

    /// `conf.Watch(func() { setRateLimitConfig(limiter) })`.
    pub fn reconfigure(&self, config: RateLimitConfig) {
        self.set_limit(config.limit);
        self.set_burst(config.burst);
    }

    fn set_limit(&self, limit: f64) {
        *self.limit.lock().expect("limit lock") = limit;
    }

    /// `rate.Limiter.SetBurst`, including its token clamp.
    fn set_burst(&self, burst: usize) {
        let mut state = self.state.lock().expect("bucket lock");
        if state.tokens > burst as f64 {
            state.tokens = burst as f64;
        }
        *self.burst.lock().expect("burst lock") = burst;
    }

    fn limit(&self) -> f64 {
        *self.limit.lock().expect("limit lock")
    }

    fn burst(&self) -> usize {
        *self.burst.lock().expect("burst lock")
    }

    /// `rate.Limiter.Wait(ctx)` for `n == 1`.
    pub async fn wait(&self, ctx: &CancellationToken) -> Result<(), String> {
        loop {
            if ctx.is_cancelled() {
                return Err("limiter.Wait: context canceled".to_owned());
            }

            let burst = self.burst();
            let limit = self.limit();
            let sleep_for = {
                let mut state = self.state.lock().expect("bucket lock");
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(state.last).as_secs_f64();
                state.last = now;
                state.tokens = (state.tokens + elapsed * limit).min(burst as f64);

                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    return Ok(());
                }
                if burst == 0 {
                    return Err(
                        "limiter.Wait: rate: Wait(n=1) exceeds limiter's burst 0".to_owned()
                    );
                }
                if limit > 0.0 {
                    Duration::from_secs_f64((1.0 - state.tokens) / limit)
                } else {
                    // A non-positive rate never accumulates tokens; poll so a
                    // cancellation is still observed.
                    Duration::from_secs(1)
                }
            };

            tokio::select! {
                _ = ctx.cancelled() => return Err("limiter.Wait: context canceled".to_owned()),
                _ = sleep(sleep_for) => {}
            }
        }
    }
}

/// `background.Job`; the columns the worker reads.
#[derive(Debug, Clone)]
pub struct OwnBackgroundJob {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub last_heartbeat_at: OffsetDateTime,
    pub worker_hostname: String,
    pub cancel: bool,
    pub repo_id: i32,
    pub config_name: String,
}

impl DbRecord for OwnBackgroundJob {
    fn record_id(&self) -> i64 {
        i64::from(self.id)
    }
}

fn stringify(err: sqlx::Error) -> String {
    err.to_string()
}

fn scan_job(row: &PgRow) -> Result<OwnBackgroundJob, String> {
    Ok(OwnBackgroundJob {
        id: row.try_get("id").map_err(stringify)?,
        state: row.try_get("state").map_err(stringify)?,
        failure_message: row.try_get("failure_message").map_err(stringify)?,
        queued_at: row.try_get("queued_at").map_err(stringify)?,
        started_at: row.try_get("started_at").map_err(stringify)?,
        finished_at: row.try_get("finished_at").map_err(stringify)?,
        process_after: row.try_get("process_after").map_err(stringify)?,
        num_resets: row.try_get("num_resets").map_err(stringify)?,
        num_failures: row.try_get("num_failures").map_err(stringify)?,
        last_heartbeat_at: row.try_get("last_heartbeat_at").map_err(stringify)?,
        worker_hostname: row.try_get("worker_hostname").map_err(stringify)?,
        cancel: row.try_get("cancel").map_err(stringify)?,
        repo_id: row.try_get("repo_id").map_err(stringify)?,
        config_name: row.try_get("config_name").map_err(stringify)?,
    })
}

/// `own_background_jobs` through the generic `dbworker/store.Store` with
/// `makeWorkerStore`'s options.
pub struct OwnBackgroundStore {
    pool: PgPool,
}

impl OwnBackgroundStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn retry_after_seconds(&self) -> i32 {
        RETRY_AFTER.as_secs() as i32
    }
}

#[async_trait]
impl DbWorkerStore for OwnBackgroundStore {
    type Record = OwnBackgroundJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        let now = OffsetDateTime::now_utc();
        let rows = sqlx::query(
            r#"
WITH potential_candidates AS (
    SELECT
        id AS candidate_id,
        ROW_NUMBER() OVER (ORDER BY id) AS order
    FROM own_background_jobs_config_aware
    WHERE
        (
            (
                state = 'queued' AND
                (process_after IS NULL OR process_after <= $1)
            ) OR (
                $2 > 0 AND
                state = 'errored' AND
                $1 - finished_at > ($2 * '1 second'::interval)
            )
        )
    ORDER BY id
    LIMIT 50
),
candidate AS (
    SELECT
        own_background_jobs_config_aware.id
    FROM own_background_jobs_config_aware
    JOIN potential_candidates pc ON pc.candidate_id = own_background_jobs_config_aware.id
    WHERE
        state IN ('queued', 'errored')
    ORDER BY pc.order
    FOR UPDATE OF own_background_jobs_config_aware SKIP LOCKED
    LIMIT 1
),
updated_record AS (
    UPDATE
        own_background_jobs
    SET
        state = 'processing',
        started_at = $1,
        last_heartbeat_at = $1,
        finished_at = NULL,
        failure_message = NULL,
        execution_logs = NULL,
        worker_hostname = $3
    WHERE
        own_background_jobs.id IN (SELECT id FROM candidate)
)
SELECT
    id,
    'processing'::text AS state,
    NULL::text AS failure_message,
    queued_at,
    $1 AS started_at,
    NULL::timestamptz AS finished_at,
    process_after,
    num_resets,
    num_failures,
    $1 AS last_heartbeat_at,
    $3::text AS worker_hostname,
    cancel,
    repo_id,
    config_name
FROM
    own_background_jobs_config_aware
WHERE
    id IN (SELECT id FROM candidate)
"#,
        )
        .bind(now)
        .bind(self.retry_after_seconds())
        .bind(worker_hostname)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;

        match rows.len() {
            0 => Ok(None),
            1 => scan_job(&rows[0]).map(Some),
            count => Err(format!("more than one record dequeued: {count}")),
        }
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        if ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let ids: Vec<i64> = ids
            .iter()
            .map(|id| {
                id.parse::<i64>()
                    .map_err(|err| format!("failed to convert record ID to int: {err}"))
            })
            .collect::<Result<_, _>>()?;

        let rows = sqlx::query(
            r#"
WITH alive_candidates AS (
    SELECT
        id
    FROM
        own_background_jobs
    WHERE
        id = ANY($1) AND
        state = 'processing'
    ORDER BY
        id ASC
    FOR UPDATE
)
UPDATE
    own_background_jobs
SET
    last_heartbeat_at = $2
WHERE
    own_background_jobs.id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
"#,
        )
        .bind(&ids)
        .bind(OffsetDateTime::now_utc())
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;

        let mut known = Vec::new();
        let mut cancel = Vec::new();
        for row in rows {
            let id: i32 = row.try_get("id").map_err(stringify)?;
            let canceled: bool = row.try_get("cancel").map_err(stringify)?;
            known.push(id.to_string());
            if canceled {
                cancel.push(id.to_string());
            }
        }
        Ok((known, cancel))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        let rows = sqlx::query(
            r#"
UPDATE own_background_jobs
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'
RETURNING id
"#,
        )
        .bind(record.id)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;
        Ok(!rows.is_empty())
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        let rows = sqlx::query(&format!(
            r#"
UPDATE own_background_jobs
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= {MAX_NUM_RETRIES} THEN 'failed' ELSE 'errored' END,
    finished_at = clock_timestamp(),
    failure_message = $2,
    num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id
"#
        ))
        .bind(record.id)
        .bind(failure_message)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;
        Ok(!rows.is_empty())
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        let rows = sqlx::query(
            r#"
UPDATE own_background_jobs
SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
    finished_at = clock_timestamp(),
    failure_message = $2,
    num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'
RETURNING id
"#,
        )
        .bind(record.id)
        .bind(failure_message)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;
        Ok(!rows.is_empty())
    }
}

#[async_trait]
impl DbResetter for OwnBackgroundStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let now = OffsetDateTime::now_utc();
        let stalled_secs = STALLED_MAX_AGE.as_secs() as i32;

        let reset = sqlx::query(
            r#"
WITH stalled AS (
    SELECT id FROM own_background_jobs
    WHERE
        state = 'processing' AND
        $1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
        num_resets < $3
    FOR UPDATE SKIP LOCKED
)
UPDATE own_background_jobs
SET
    state = 'queued',
    queued_at = clock_timestamp(),
    started_at = null,
    num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(stalled_secs)
        .bind(MAX_NUM_RESETS)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;

        let failed = sqlx::query(
            r#"
WITH stalled AS (
    SELECT id FROM own_background_jobs
    WHERE
        state = 'processing' AND
        $1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
        num_resets >= $3
    FOR UPDATE SKIP LOCKED
)
UPDATE own_background_jobs
SET
    state = 'failed',
    finished_at = clock_timestamp(),
    failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(stalled_secs)
        .bind(MAX_NUM_RESETS)
        .bind(DEFAULT_RESET_FAILURE_MESSAGE)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;

        Ok((scan_ages(reset, now)?, scan_ages(failed, now)?))
    }
}

fn scan_ages(rows: Vec<PgRow>, now: OffsetDateTime) -> Result<Vec<(i64, Duration)>, String> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id").map_err(stringify)?;
        let last_heartbeat: OffsetDateTime = row.try_get("last_heartbeat_at").map_err(stringify)?;
        let age = now - last_heartbeat;
        ages.push((
            i64::from(id),
            std::time::Duration::try_from(age).unwrap_or(Duration::ZERO),
        ));
    }
    ages.sort_by_key(|(id, _)| *id);
    Ok(ages)
}

/// The janitor's `CleanupFunc`.
pub async fn cleanup(pool: &PgPool) -> Result<i64, String> {
    let delete_since = OffsetDateTime::now_utc() - JANITOR_RETENTION;
    let result = sqlx::query(JANITOR_QUERY)
        .bind(delete_since)
        .execute(pool)
        .await
        .map_err(stringify)?;
    Ok(result.rows_affected() as i64)
}

/// `background.NewJanitorJob`'s handler.
pub struct JanitorHandler {
    pool: PgPool,
}

impl JanitorHandler {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Handler for JanitorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        cleanup(&self.pool)
            .await
            .map(|_| ())
            .map_err(HandlerError::new)
    }
}

/// `cockroachdb/errors.Wrap`: renders `"{context}: {err}"`, dropping a single
/// trailing newline from the inner error. The Go baseline's
/// `errors.Newf("... %s\n", name)` in `loadConfig` renders with a newline when
/// returned directly, but wrapped errors lose it.
fn wrap_error(context: &str, err: &str) -> String {
    format!("{context}: {}", err.strip_suffix('\n').unwrap_or(err))
}

/// `loadConfig`: first configuration for `name`, or the baseline's error.
pub async fn load_config(
    pool: &PgPool,
    name: &str,
) -> Result<sg_db::own_signal_configurations::SignalConfiguration, String> {
    sg_db::own_signal_configurations::load_config(pool, name).await
}

/// `authz.SubRepoEnabled` for the worker's perforce checker:
/// `experimentalFeatures.subRepoPermissions.enabled`.
pub fn sub_repo_permissions_enabled(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("experimentalFeatures")
        .and_then(|value| value.get("subRepoPermissions"))
        .and_then(|value| value.get("enabled"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// `ownRepoIndexSchedulerJob.Handle`.
pub async fn run_scheduler(pool: &PgPool, job_type: &IndexJobType) -> Result<i64, String> {
    let after = OffsetDateTime::now_utc() - job_type.index_interval;
    let result = sqlx::query(OWN_INDEX_REPO_QUERY)
        .bind(job_type.name)
        .bind(after)
        .execute(pool)
        .await
        .map_err(|err| {
            wrap_error(
                &format!("ownRepoIndexSchedulerJob.Handle {}", job_type.name),
                &err.to_string(),
            )
        })?;
    Ok(result.rows_affected() as i64)
}

/// `newFeatureFlagWrapper` + `ownRepoIndexSchedulerJob`.
pub struct SchedulerHandler {
    pool: PgPool,
    job_type: IndexJobType,
}

impl SchedulerHandler {
    pub fn new(pool: PgPool, job_type: IndexJobType) -> Self {
        Self { pool, job_type }
    }
}

#[async_trait]
impl Handler for SchedulerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let config = load_config(&self.pool, self.job_type.name)
            .await
            .map_err(|err| HandlerError::new(wrap_error("loadConfig", &err)))?;
        if !config.enabled {
            return Ok(());
        }
        run_scheduler(&self.pool, &self.job_type)
            .await
            .map(|_| ())
            .map_err(HandlerError::new)
    }
}

fn timestamp_to_offset(timestamp: Option<Timestamp>) -> OffsetDateTime {
    let Some(timestamp) = timestamp else {
        return OffsetDateTime::UNIX_EPOCH;
    };
    let nanos = i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos);
    OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn offset_to_timestamp(value: OffsetDateTime) -> Timestamp {
    let nanos = value.unix_timestamp_nanos();
    Timestamp {
        seconds: nanos.div_euclid(1_000_000_000) as i64,
        nanos: nanos.rem_euclid(1_000_000_000) as i32,
    }
}

/// The gitserver operations the recent-contributors indexer needs.
#[async_trait]
pub trait RecentContributionsProvider: Send + Sync {
    async fn commits(
        &self,
        repo: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<sg_git::Commit>, String>;

    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<sg_git::ChangedFile>, String>;
}

#[async_trait]
impl RecentContributionsProvider for GitserverClient {
    async fn commits(
        &self,
        repo: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<sg_git::Commit>, String> {
        GitserverClient::commits(self, repo, options)
            .await
            .map_err(|err| err.to_string())
    }

    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<sg_git::ChangedFile>, String> {
        GitserverClient::changed_files(self, repo, base, head)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `recentContributorsIndexer.indexRepo`.
pub async fn index_recent_contributors(
    pool: &PgPool,
    git: &dyn RecentContributionsProvider,
    repo_id: i32,
    sub_repo_permissions_enabled: bool,
) -> Result<(), String> {
    // 🚨 SECURITY: the background indexer is not associated with any user; the
    // internal actor sees every repo and file. The Rust port never applies
    // authz filtering on this path.
    //
    // `authz.SubRepoEnabledForRepoID`: the checker only reports supported
    // repos when `experimentalFeatures.subRepoPermissions.enabled` is true.
    if sub_repo_permissions_enabled {
        let sub_repo = sg_db::sub_repo_perms::repo_id_supported(pool, repo_id)
            .await
            .map_err(|err| wrap_error("sub-repo permissions check", &err.to_string()))?;
        if sub_repo {
            return Ok(());
        }
    }

    let repo = match sg_db::repos::RepoStore::new(pool.clone())
        .get_by_id(repo_id)
        .await
        .map_err(|err| wrap_error("repoStore.Get", &err.to_string()))?
    {
        Some(repo) => repo,
        None => {
            return Err(format!("repoStore.Get: repo not found: id={repo_id}"));
        }
    };
    if let Some(blocked) = &repo.blocked {
        let reason = blocked
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        return Err(format!(
            "repoStore.Get: repository {} has been blocked. reason: {reason}",
            repo.name
        ));
    }

    let options = CommitsOptions {
        ranges: vec!["HEAD".to_owned()],
        after: Some(offset_to_timestamp(
            OffsetDateTime::now_utc() - Duration::from_secs(90 * 24 * 60 * 60),
        )),
        order: Some(CommitsOrder::TopoDate),
        ..CommitsOptions::default()
    };
    let commits = git
        .commits(&repo.name, &options)
        .await
        .map_err(|err| wrap_error("Commits", &err))?;

    sg_db::recent_contribution_signals::clear_signals(pool, repo_id)
        .await
        .map_err(|err| wrap_error("ClearSignals", &err))?;

    for commit in commits {
        if commit.parents.len() > 1 {
            // Merge commits do not count as contributions.
            continue;
        }
        let changed = git
            .changed_files(&repo.name, "", &commit.oid)
            .await
            .map_err(|err| wrap_error("ChangedFiles", &err))?;
        let files_changed: Vec<String> = changed.into_iter().map(|file| file.path).collect();
        sg_db::recent_contribution_signals::add_commit(
            pool,
            &sg_db::recent_contribution_signals::Commit {
                repo_id,
                author_name: commit.author_name.clone(),
                author_email: commit.author_email.clone(),
                timestamp: timestamp_to_offset(commit.author_date),
                commit_sha: commit.oid.clone(),
                files_changed,
            },
        )
        .await
        .map_err(|err| wrap_error(&format!("AddCommit {commit:?}"), &err))?;
    }
    Ok(())
}

/// `workerutil.NonRetryableError` classification for the generic worker.
pub fn is_non_retryable(failure_message: &str) -> bool {
    failure_message == "unsupported own index job type"
        || failure_message == ANALYTICS_NOT_PORTED
        || failure_message.starts_with("sub-repo permissions check: ")
}

/// `handler.Handle`.
pub struct OwnBackgroundHandler {
    pool: PgPool,
    git: Arc<dyn RecentContributionsProvider>,
    live_config: Arc<LiveConfig>,
    limiter: Arc<OwnRateLimiter>,
}

impl OwnBackgroundHandler {
    pub fn new(
        pool: PgPool,
        git: Arc<dyn RecentContributionsProvider>,
        live_config: Arc<LiveConfig>,
        limiter: Arc<OwnRateLimiter>,
    ) -> Self {
        Self {
            pool,
            git,
            live_config,
            limiter,
        }
    }
}

#[async_trait]
impl DbWorkerHandler<OwnBackgroundStore> for OwnBackgroundHandler {
    async fn handle(
        &self,
        ctx: CancellationToken,
        record: &OwnBackgroundJob,
    ) -> Result<(), String> {
        self.limiter
            .reconfigure(rate_limit_config(&self.live_config.current().site));
        self.limiter.wait(&ctx).await?;

        match record.config_name.as_str() {
            SIGNAL_RECENT_CONTRIBUTORS => {
                index_recent_contributors(
                    &self.pool,
                    self.git.as_ref(),
                    record.repo_id,
                    sub_repo_permissions_enabled(&self.live_config.current().site),
                )
                .await
            }
            SIGNAL_ANALYTICS => Err(ANALYTICS_NOT_PORTED.to_owned()),
            _ => Err("unsupported own index job type".to_owned()),
        }
    }

    fn non_retryable(&self, failure_message: &str) -> bool {
        is_non_retryable(failure_message)
    }
}

/// `own.NewOwnRepoIndexingQueue`.
#[derive(Debug, Default)]
pub struct OwnRepoIndexingQueue;

impl OwnRepoIndexingQueue {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for OwnRepoIndexingQueue {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !own_enabled() {
            return Ok(Vec::new());
        }

        let unified = deps.live_config.current();
        let git = Arc::new(GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));
        let store = Arc::new(OwnBackgroundStore::new(deps.frontend.clone()));
        let limiter = Arc::new(OwnRateLimiter::new(rate_limit_config(&unified.site)));

        let worker = WorkerRoutine::new(
            Arc::clone(&store),
            Arc::new(OwnBackgroundHandler::new(
                deps.frontend.clone(),
                git,
                deps.live_config.clone(),
                limiter,
            )),
            WorkerOptions {
                name: WORKER_NAME.to_owned(),
                description: WORKER_DESCRIPTION.to_owned(),
                interval: WORKER_INTERVAL,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                num_handlers: index_concurrency_limit(&unified.site),
                worker_hostname: default_hostname(),
            },
        );

        let resetter = ResetterRoutine::new(
            Arc::clone(&store) as Arc<dyn DbResetter>,
            ResetterOptions {
                name: RESETTER_NAME.to_owned(),
                interval: RESETTER_INTERVAL,
            },
        );

        let janitor = PeriodicRoutine::new(Arc::new(JanitorHandler::new(deps.frontend.clone())))
            .with_name(JANITOR_NAME)
            .with_description(JANITOR_DESCRIPTION)
            .with_interval(JANITOR_INTERVAL);

        let mut routines = vec![
            BackgroundRoutine::DbBacked(Arc::new(worker) as Arc<dyn DbBackedRoutine>),
            BackgroundRoutine::DbBacked(Arc::new(resetter) as Arc<dyn DbBackedRoutine>),
            janitor.into(),
        ];

        for job_type in queue_per_repo_index_jobs() {
            let scheduler = PeriodicRoutine::new(Arc::new(SchedulerHandler::new(
                deps.frontend.clone(),
                job_type,
            )))
            .with_name(job_type.name)
            .with_description("")
            .with_interval(job_type.refresh_interval);
            routines.push(scheduler.into());
        }

        Ok(routines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_config::SiteConfiguration;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    fn deps() -> JobDeps {
        JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        }
    }

    #[test]
    fn job_description_matches_the_baseline() {
        assert_eq!(
            OwnRepoIndexingQueue::new().description(),
            "Queue used to index ownership data partitioned per repository"
        );
    }

    #[test]
    fn own_enabled_parses_disable_own() {
        use std::env::VarError;
        assert!(!own_enabled_from(Ok("true".to_owned())));
        assert!(!own_enabled_from(Ok("1".to_owned())));
        assert!(own_enabled_from(Ok("false".to_owned())));
        assert!(own_enabled_from(Ok("nonsense".to_owned())));
        assert!(own_enabled_from(Err(VarError::NotPresent)));
    }

    #[test]
    fn config_readers_match_the_defaults() {
        let empty = site("{}");
        assert_eq!(index_concurrency_limit(&empty), 5);
        assert_eq!(
            rate_limit_config(&empty),
            RateLimitConfig {
                limit: 20.0,
                burst: 5
            }
        );

        let configured = site(
            r#"{
                "own.background.repoIndexConcurrencyLimit": 9,
                "own.background.repoIndexRateLimit": 42,
                "own.background.repoIndexRateBurstLimit": 7
            }"#,
        );
        assert_eq!(index_concurrency_limit(&configured), 9);
        assert_eq!(
            rate_limit_config(&configured),
            RateLimitConfig {
                limit: 42.0,
                burst: 7
            }
        );
    }

    #[test]
    fn scheduler_routines_match_the_baseline() {
        let jobs = queue_per_repo_index_jobs();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].name, "recent-contributors");
        assert_eq!(jobs[0].index_interval, Duration::from_secs(24 * 60 * 60));
        assert_eq!(jobs[0].refresh_interval, Duration::from_secs(5 * 60));
        assert_eq!(jobs[1].name, "analytics");
        assert_eq!(jobs[1].refresh_interval, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn non_retryable_classification() {
        assert!(is_non_retryable("unsupported own index job type"));
        assert!(is_non_retryable(ANALYTICS_NOT_PORTED));
        assert!(is_non_retryable("sub-repo permissions check: boom"));
        assert!(!is_non_retryable("repoStore.Get: repo not found: id=1"));
        assert!(!is_non_retryable("Commits: boom"));
    }

    #[test]
    fn timestamp_round_trip() {
        let value = OffsetDateTime::from_unix_timestamp(1_700_000_000)
            .expect("timestamp")
            .replace_nanosecond(123_456_789)
            .expect("nanos");
        let timestamp = offset_to_timestamp(value);
        assert_eq!(timestamp.seconds, 1_700_000_000);
        assert_eq!(timestamp.nanos, 123_456_789);
        assert_eq!(timestamp_to_offset(Some(timestamp)), value);
    }

    #[tokio::test]
    async fn limiter_starts_full_and_reconfigures() {
        let limiter = OwnRateLimiter::new(RateLimitConfig {
            limit: 20.0,
            burst: 3,
        });
        let ctx = CancellationToken::new();
        for _ in 0..3 {
            limiter.wait(&ctx).await.expect("burst tokens available");
        }

        limiter.reconfigure(RateLimitConfig {
            limit: 20.0,
            burst: 0,
        });
        assert!(limiter.wait(&ctx).await.is_err());
    }

    #[tokio::test]
    async fn job_builds_the_baseline_routines() {
        let routines = OwnRepoIndexingQueue::new()
            .routines(&deps(), CancellationToken::new())
            .await
            .expect("routines");

        let names: Vec<&str> = routines.iter().map(BackgroundRoutine::name).collect();
        assert_eq!(
            names,
            vec![
                "own_background_worker",
                "own_background_worker_resetter",
                "own-background-jobs-janitor",
                "recent-contributors",
                "analytics",
            ]
        );
        assert_eq!(
            routines[2].description(),
            "Janitor for own-background-jobs queue"
        );
        assert_eq!(routines[2].interval(), Duration::from_secs(5 * 60));
        assert_eq!(routines[3].description(), "");
        assert_eq!(routines[3].interval(), Duration::from_secs(5 * 60));
        assert_eq!(routines[4].interval(), Duration::from_secs(24 * 60 * 60));
    }
}
