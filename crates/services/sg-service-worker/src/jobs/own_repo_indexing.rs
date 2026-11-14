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
//!    when disabled) and the baseline's `ownIndexRepoQuery`;
//! 5. the standalone `recent-views` routine (every 5m) for
//!    `internal/own/background/recent_views.go`.
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
//! `analytics` (`internal/own/background/analytics.go`) is ported in full:
//! sub-repo-permissions skip, repo lookup, `HEAD` resolution (non-retryable
//! on failure), a recursive `ReadDir` that counts every file, CODEOWNERS
//! ownership (ingested `codeowners.contents`, else the baseline's git file
//! locations with last-matching-rule semantics) and assigned owners/teams
//! inherited down the tree, then the two idempotent root writes to
//! `repo_paths.tree_files_count` and `ownership_path_stats`.
//!
//! ## Deferred (documented gaps, see docs §76)
//!
//! * `recent-views` (`internal/own/background/recent_views.go`) is ported:
//!   the bookmark in `event_logs_scrape_state_own`, the `ViewBlob` event
//!   scan (limit 5000, ascending once bookmarked), sub-repo filtering,
//!   `BuildAggregateFromEvents` (excluded repo patterns, ancestor counts,
//!   additive `own_aggregate_recent_view` upserts) and the bookmark advance.
//!   The routine is registered separately from `QueuePerRepoIndexJobs`, just
//!   like the baseline.
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
//! * The analytics indexer parses ingested CODEOWNERS from the `codeowners.contents`
//!   text instead of the `contents_proto` protobuf, and its part wildcards
//!   support only `*`/`?` (no character classes). Git files and ordinary
//!   patterns behave the same as `wildmatch-go`; see the M9.42d overview.
//! * Not ported: RED metrics, the repo/commit counters, log lines and
//!   tracing spans. The store's debug-info lookup for unknown heartbeat IDs
//!   is also omitted (the framework already logs the unknown id).
//! * `recent_views.go` advances the bookmark with
//!   `events[numberOfEvents-1].ID`, indexing the *unfiltered* event list with
//!   the filtered count. The Rust port keeps that bug verbatim: when events
//!   are dropped (malformed JSON, sub-repo repo), the bookmark can lag and
//!   the next run re-counts the tail.
//! * `encoding/json` matches field names case-insensitively and accepts a
//!   JSON `null` as zero values; [`parse_repo_path_and_name`] does both.
//! * `Repos().GetByName` returns a blocked repo together with its
//!   `BlockedRepoError`; `BuildAggregateFromEvents` treats that as a fatal
//!   (non-NotFound) error, and so does the Rust port.
//! * The Rust worker does not watch the site configuration to resize the
//!   handler pool; `NumHandlers` is read once when the routines are built, the
//!   same moment the baseline calls `getConcurrencyConfig()`.

use std::collections::{HashMap, HashSet};
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

/// `types.SignalRecentViews`.
pub const SIGNAL_RECENT_VIEWS: &str = "recent-views";

/// `recent.RefreshInterval` in `GetOwnIndexSchedulerRoutines`.
pub const RECENT_VIEWS_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);

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

/// `codeownersLocations` from `internal/own/service.go`, in priority order.
pub const CODEOWNERS_LOCATIONS: &[&str] = &[
    ".github/test.CODEOWNERS",
    "CODEOWNERS",
    ".github/CODEOWNERS",
    ".gitlab/CODEOWNERS",
    "docs/CODEOWNERS",
];

/// Prefix for a failed `ResolveRevision` (`cannot resolve HEAD`), which the
/// baseline marks non-retryable via `errcode.MakeNonRetryable`.
pub const CANNOT_RESOLVE_HEAD: &str = "cannot resolve HEAD";

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

/// `QueuePerRepoIndexJobs[0]` (`recent-views` is not queued per repository).
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

/// `authz.SubRepoPermissionChecker` subset used by the recent-views filter:
/// [`enabled`](SubRepoEnabledChecker::enabled) is `checker.Enabled()`, and
/// [`enabled_for_repo`](SubRepoEnabledChecker::enabled_for_repo) is
/// `EnabledForRepo` (the caller still gates on `enabled()` like
/// `authz.SubRepoEnabledForRepo`).
#[async_trait]
pub trait SubRepoEnabledChecker: Send + Sync {
    fn enabled(&self) -> bool;
    async fn enabled_for_repo(&self, repo: &str) -> Result<bool, String>;
}

/// Production checker: `srp.NewSubRepoPermsClient(db.SubRepoPerms())` — the
/// site feature flag plus `SubRepoPermsStore.RepoSupported` (a private
/// Perforce repository).
pub struct DefaultSubRepoChecker {
    pool: PgPool,
    site_enabled: bool,
}

impl DefaultSubRepoChecker {
    pub fn new(pool: PgPool, site_enabled: bool) -> Self {
        Self {
            pool,
            site_enabled,
        }
    }
}

#[async_trait]
impl SubRepoEnabledChecker for DefaultSubRepoChecker {
    fn enabled(&self) -> bool {
        self.site_enabled
    }

    async fn enabled_for_repo(&self, repo: &str) -> Result<bool, String> {
        if !self.site_enabled {
            return Ok(false);
        }
        sg_db::sub_repo_perms::repo_supported(&self.pool, repo)
            .await
            .map_err(|err| err.to_string())
    }
}

/// Go `json.Unmarshal(event.PublicArgument, &viewBlob)` / `&repoPathAndName`.
///
/// Mirrors `encoding/json` into a two-string struct: a JSON `null`, `{}` or a
/// missing field leaves the zero value (no error), while a wrong field type or
/// a non-object top-level value is an error. Keys are matched
/// case-insensitively like `encoding/json`.
pub fn parse_repo_path_and_name(raw: &str) -> Option<(String, String)> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    let serde_json::Value::Object(map) = value else {
        // `null` decodes into the struct as zero values; anything else is an
        // "cannot unmarshal ... into Go value of type" error.
        return match value {
            serde_json::Value::Null => Some((String::new(), String::new())),
            _ => None,
        };
    };
    let field = |key: &str| -> Option<String> {
        let mut value = String::new();
        for (candidate, raw) in &map {
            if candidate.eq_ignore_ascii_case(key) {
                match raw {
                    serde_json::Value::Null => value = String::new(),
                    serde_json::Value::String(s) => value = s.clone(),
                    _ => return None,
                }
            }
        }
        Some(value)
    };
    Some((field("repoName")?, field("filePath")?))
}

/// `recentViewsIndexer.handle(ctx, authz.DefaultSubRepoPermsChecker)`.
pub async fn index_recent_views(
    pool: &PgPool,
    checker: &dyn SubRepoEnabledChecker,
) -> Result<(), String> {
    let bookmark = sg_db::recent_view_signals::get_bookmark(pool, SIGNAL_RECENT_VIEWS)
        .await
        .map_err(|err| format!("getting latest processed event ID: {err}"))?;
    let events = sg_db::recent_view_signals::list_view_blob_events(pool, bookmark, 5000)
        .await
        .map_err(|err| format!("getting event logs: {err}"))?;

    let mut filtered: Vec<sg_db::recent_view_signals::ViewBlobEvent> = Vec::new();
    let mut sub_repo_cache: HashMap<String, bool> = HashMap::new();
    for event in &events {
        let Some((repo_name, _file_path)) = parse_repo_path_and_name(&event.public_argument)
        else {
            continue;
        };
        if let Some(is_sub_repo) = sub_repo_cache.get(&repo_name) {
            if !is_sub_repo {
                filtered.push(event.clone());
            }
            continue;
        }
        // `authz.SubRepoEnabledForRepo`: a disabled checker short-circuits to
        // false; only then is `EnabledForRepo` consulted.
        if !checker.enabled() {
            filtered.push(event.clone());
            sub_repo_cache.insert(repo_name, false);
            continue;
        }
        match checker.enabled_for_repo(&repo_name).await {
            // `r.logger.Debug(...)`: the event is neither kept nor cached.
            Err(_) => {}
            Ok(true) => {
                sub_repo_cache.insert(repo_name, true);
            }
            Ok(false) => {
                filtered.push(event.clone());
                sub_repo_cache.insert(repo_name, false);
            }
        }
    }

    let number_of_events = filtered.len();
    if number_of_events == 0 {
        return Ok(());
    }
    build_aggregate_from_events(pool, &filtered)
        .await
        .map_err(|err| format!("building aggregates from events: {err}"))?;
    // Baseline bug kept verbatim: the index is into `events` (unfiltered),
    // not `filtered`.
    let new_bookmark = events[number_of_events - 1].id as i32;
    sg_db::recent_view_signals::update_bookmark(pool, new_bookmark, SIGNAL_RECENT_VIEWS).await
}

struct RepoMetadata {
    repo_id: i32,
    path_to_id: HashMap<String, i32>,
}

/// `recentViewSignalStore.BuildAggregateFromEvents`.
async fn build_aggregate_from_events(
    pool: &PgPool,
    events: &[sg_db::recent_view_signals::ViewBlobEvent],
) -> Result<(), String> {
    // `OwnSignalConfigurations().LoadConfigurations({Name: "recent-views"})`
    // then `regexifyPatterns` (`%` -> `.*`, unanchored `MatchString`).
    let raw_patterns = sg_db::recent_view_signals::load_excluded_patterns(pool)
        .await
        .map_err(|err| format!("error during fetching own signals configuration: {err}"))?;
    let mut excludes: Vec<regex::Regex> = Vec::with_capacity(raw_patterns.len());
    for pattern in raw_patterns {
        let regex = regex::Regex::new(&pattern.replace('%', ".*"))
            .map_err(|err| format!("error during fetching own signals configuration: {err}"))?;
        excludes.push(regex);
    }

    let mut repo_meta: HashMap<String, RepoMetadata> = HashMap::new();
    let mut user_to_count_by_path: HashMap<u32, HashMap<(String, String), i32>> = HashMap::new();
    let mut not_found_repos: HashSet<String> = HashSet::new();

    for event in events {
        let Some((repo_name, file_path)) = parse_repo_path_and_name(&event.public_argument)
        else {
            continue;
        };
        if excludes.iter().any(|regex| regex.is_match(&repo_name)) {
            continue;
        }
        let user_id = event.user_id as u32;
        *user_to_count_by_path
            .entry(user_id)
            .or_default()
            .entry((repo_name.clone(), file_path.clone()))
            .or_insert(0) += 1;

        if !repo_meta.contains_key(&repo_name) {
            if not_found_repos.contains(&repo_name) {
                continue;
            }
            let repo = sg_db::repos::RepoStore::new(pool.clone())
                .get_by_name(&repo_name)
                .await
                .map_err(|err| format!("error during fetching the repository: {err}"))?;
            let Some(repo) = repo else {
                not_found_repos.insert(repo_name);
                continue;
            };
            if let Some(blocked) = &repo.blocked {
                // `GetByName` returns `repo, repo.IsBlocked()`.
                let reason = blocked
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                return Err(format!(
                    "error during fetching the repository: repository {} has been blocked. reason: {reason}",
                    repo.name
                ));
            }
            let mut path_to_id = HashMap::new();
            path_to_id.insert(file_path.clone(), 0);
            repo_meta.insert(
                repo_name.clone(),
                RepoMetadata {
                    repo_id: repo.id,
                    path_to_id,
                },
            );
        }
        repo_meta
            .get_mut(&repo_name)
            .expect("repo metadata inserted above")
            .path_to_id
            .insert(file_path, 0);
    }

    // `ensureRepoPaths` per repo; `paths` and the returned IDs are parallel.
    for meta in repo_meta.values_mut() {
        let paths: Vec<String> = meta.path_to_id.keys().cloned().collect();
        let ids = sg_db::repo_paths::ensure_repo_paths(pool, &paths, meta.repo_id)
            .await
            .map_err(|err| format!("cannot insert repo paths: {err}"))?;
        for (path, id) in paths.into_iter().zip(ids) {
            meta.path_to_id.insert(path, id);
        }
    }

    let mut users: Vec<(u32, HashMap<(String, String), i32>)> =
        user_to_count_by_path.into_iter().collect();
    users.sort_by_key(|(user_id, _)| *user_id);
    for (user_id, path_and_count) in users {
        let mut repo_path_id_to_count: std::collections::BTreeMap<i32, i32> =
            std::collections::BTreeMap::new();
        for ((repo_name, file_path), count) in path_and_count {
            match repo_meta
                .get(&repo_name)
                .and_then(|meta| meta.path_to_id.get(&file_path))
            {
                Some(path_id) => {
                    repo_path_id_to_count.insert(*path_id, count);
                }
                None if not_found_repos.contains(&repo_name) => {}
                None => {
                    return Err(format!(
                        "cannot find id of path {file_path:?} of repo {repo_name:?}: this is a bug"
                    ))
                }
            }
        }
        sg_db::recent_view_signals::insert_paths(pool, user_id as i32, &repo_path_id_to_count)
            .await?;
    }
    Ok(())
}

/// `newRecentViewsIndexer(db, logger)` wrapped by `featureFlagWrapper`.
pub struct RecentViewsHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

impl RecentViewsHandler {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self { pool, live_config }
    }
}

#[async_trait]
impl Handler for RecentViewsHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        // `featureFlagWrapper.Handle`: `loadConfig`, skip when disabled.
        let config = load_config(&self.pool, SIGNAL_RECENT_VIEWS)
            .await
            .map_err(|err| HandlerError::new(wrap_error("loadConfig", &err)))?;
        if !config.enabled {
            return Ok(());
        }
        let checker = DefaultSubRepoChecker::new(
            self.pool.clone(),
            sub_repo_permissions_enabled(&self.live_config.current().site),
        );
        index_recent_views(&self.pool, &checker)
            .await
            .map_err(HandlerError::new)
    }
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

/// One `ReadDir` entry. `name` is repository-relative for recursive reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticsDirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// The gitserver operations the analytics indexer needs. Split from
/// [`RecentContributionsProvider`] because analytics resolves a revision and
/// reads a recursive tree plus optional text files.
#[async_trait]
pub trait AnalyticsProvider: Send + Sync {
    async fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, String>;

    async fn read_dir_recursive(
        &self,
        repo: &str,
        commit: &str,
    ) -> Result<Vec<AnalyticsDirEntry>, String>;

    async fn read_file_optional(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<String>, String>;
}

#[async_trait]
impl AnalyticsProvider for GitserverClient {
    async fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, String> {
        // `ResolveRevisionOptions{EnsureRevision: false}`.
        GitserverClient::resolve_revision(self, repo, rev, false)
            .await
            .map_err(|err| err.to_string())
    }

    async fn read_dir_recursive(
        &self,
        repo: &str,
        commit: &str,
    ) -> Result<Vec<AnalyticsDirEntry>, String> {
        GitserverClient::read_dir_entries(self, repo, commit, "", true)
            .await
            .map_err(|err| err.to_string())
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| AnalyticsDirEntry {
                        name: entry.name,
                        is_dir: entry.is_dir,
                    })
                    .collect()
            })
    }

    async fn read_file_optional(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<String>, String> {
        match GitserverClient::read_file_optional(self, repo, commit, path).await {
            Ok(Some(contents)) => Ok(Some(String::from_utf8_lossy(&contents).into_owned())),
            Ok(None) => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    }
}

/// One compiled CODEOWNERS rule (`internal/own/codeowners` +
/// `internal/paths.Compile`). The owner identities are irrelevant to the
/// analytics indexer: only "has at least one owner" matters.
struct CodeownersRule {
    parts: Vec<GlobPart>,
    has_owners: bool,
}

enum GlobPart {
    AnySubPath,
    AnyMatch,
    Exact(String),
    Wild(String),
}

/// `paths.Compile`. `None` mirrors a compile error, which
/// `CompiledRule.match` treats as a no-match for that rule.
fn compile_glob(pattern: &str) -> Option<Vec<GlobPart>> {
    let mut parts = Vec::new();
    if !pattern.starts_with('/') {
        // No leading `/` is equivalent to prefixing with `/**/`.
        parts.push(GlobPart::AnySubPath);
    }
    for part in pattern.trim_matches('/').split('/') {
        match part {
            "" => return None, // two consecutive forward slashes
            "**" => parts.push(GlobPart::AnySubPath),
            "*" => parts.push(GlobPart::AnyMatch),
            part if part.contains('*') => parts.push(GlobPart::Wild(part.to_owned())),
            part => parts.push(GlobPart::Exact(part.to_owned())),
        }
    }
    if pattern.ends_with('/') {
        parts.push(GlobPart::AnySubPath);
    }
    if matches!(parts.last(), Some(GlobPart::AnySubPath)) {
        // A trailing `/**` is necessarily `/**/*`: it must not match the
        // directory itself.
        parts.push(GlobPart::AnyMatch);
    }
    Some(parts)
}

fn part_matches(part: &GlobPart, text: &str) -> bool {
    match part {
        GlobPart::AnySubPath | GlobPart::AnyMatch => true,
        GlobPart::Exact(exact) => exact == text,
        GlobPart::Wild(pattern) => wildcard_match(pattern, text),
    }
}

/// `wildmatch-go` for the `*`/`?` subset CODEOWNERS patterns use.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    fn go(pattern: &[char], text: &[char]) -> bool {
        let Some((&first, rest)) = pattern.split_first() else {
            return text.is_empty();
        };
        match first {
            '*' => go(rest, text) || (!text.is_empty() && go(pattern, &text[1..])),
            '?' => !text.is_empty() && go(rest, &text[1..]),
            _ => text.first() == Some(&first) && go(rest, &text[1..]),
        }
    }
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    go(&pattern, &text)
}

/// `paths.GlobPattern.Match`, including the `**` state machine.
fn glob_matches(parts: &[GlobPart], file_path: &str) -> bool {
    if parts.is_empty() || parts.len() >= 100 {
        return false;
    }
    let mut path = file_path;
    path = path.strip_prefix('/').unwrap_or(path);
    path = path.strip_suffix('/').unwrap_or(path);

    // State bit `i` means "the first `i` pattern parts matched the consumed
    // path so far". Leading `**` also matches the empty prefix.
    let mut initial: u128 = 1;
    for (index, part) in parts.iter().enumerate() {
        if !matches!(part, GlobPart::AnySubPath) {
            break;
        }
        initial |= 1 << (index + 1);
    }

    let mut current = initial;
    let mut next: u128 = 0;
    let mut remaining = path;
    loop {
        let (part, has_next, rest) = next_part(remaining, '/');
        remaining = rest;
        for index in 0..parts.len() {
            if (current >> index) & 1 == 0 {
                continue;
            }
            let current_part = &parts[index];
            if part_matches(current_part, part) {
                next |= 1 << (index + 1);
                if index + 1 < parts.len()
                    && matches!(parts[index + 1], GlobPart::AnySubPath)
                {
                    next |= 1 << (index + 2);
                }
            } else {
                next &= !(1 << (index + 1));
                if index + 1 < parts.len()
                    && matches!(parts[index + 1], GlobPart::AnySubPath)
                {
                    next &= !(1 << (index + 2));
                }
            }
            // `**` may consume any number of parts, so keep the preceding bit.
            if matches!(current_part, GlobPart::AnySubPath) {
                next |= 1 << index;
            }
        }
        if current == 0 {
            return false;
        }
        current = next;
        if !has_next {
            break;
        }
        next = 0;
    }
    (current >> parts.len()) & 1 == 1
}

fn next_part(s: &str, separator: char) -> (&str, bool, &str) {
    match s.find(separator) {
        Some(index) => (
            &s[..index],
            true,
            &s[index + separator.len_utf8()..],
        ),
        None => (s, false, s),
    }
}

/// `lineWithoutComments`.
fn line_without_comments(line: &str) -> &str {
    let mut escaped = false;
    for (index, character) in line.char_indices() {
        if character == '#' && !escaped {
            return &line[..index];
        }
        if character == '\\' && !escaped {
            escaped = true;
            continue;
        }
        escaped = false;
    }
    line
}

/// `matchSection`: `[name]`, `^[name]` or `[name][2]`.
fn is_section(line: &str) -> bool {
    let mut rest = line.trim();
    if let Some(stripped) = rest.strip_prefix('^') {
        rest = stripped.trim_start();
    }
    let Some(after_open) = rest.strip_prefix('[') else {
        return false;
    };
    let Some(close) = after_open.find(']') else {
        return false;
    };
    if close == 0 {
        return false; // `[^\]]+` requires a name
    }
    let tail = after_open[close + 1..].trim();
    if tail.is_empty() {
        return true;
    }
    let Some(inner) = tail.strip_prefix('[').and_then(|t| t.strip_suffix(']')) else {
        return false;
    };
    !inner.is_empty() && inner.chars().all(|character| character.is_ascii_digit())
}

/// `matchRule`: pattern (with `\ ` escapes) followed by whitespace-separated
/// owners. Returns the unescaped pattern and owner tokens.
fn parse_rule(line: &str) -> Option<(String, Vec<&str>)> {
    let bytes = line.as_bytes();
    let mut start = 0;
    while start < bytes.len() && (bytes[start] as char).is_ascii_whitespace() {
        start += 1;
    }
    if start == bytes.len() {
        return None;
    }
    let mut end = start;
    while end < bytes.len() {
        let character = bytes[end] as char;
        if character == '\\' && end + 1 < bytes.len() {
            end += 2;
            continue;
        }
        if character.is_ascii_whitespace() {
            break;
        }
        end += 1;
    }
    let raw_pattern = &line[start..end];
    let owners: Vec<&str> = line[end..].split_whitespace().collect();
    let mut pattern = String::with_capacity(raw_pattern.len());
    let mut escaped = false;
    for character in raw_pattern.chars() {
        if character == '\\' && !escaped {
            escaped = true;
            continue;
        }
        pattern.push(character);
        escaped = false;
    }
    Some((pattern, owners))
}

/// `codeowners.Parse`: ordered rules; invalid globs are kept as no-match
/// rules, matching the lazy-compile behavior in `CompiledRule.match`.
fn parse_codeowners(contents: &str) -> Vec<CodeownersRule> {
    let mut rules = Vec::new();
    for line in contents.lines() {
        let line = line_without_comments(line);
        if line.trim().is_empty() || is_section(line) {
            continue;
        }
        let Some((pattern, owners)) = parse_rule(line) else {
            continue;
        };
        let Some(parts) = compile_glob(&pattern) else {
            continue;
        };
        rules.push(CodeownersRule {
            parts,
            has_owners: !owners.is_empty(),
        });
    }
    rules
}

/// `Ruleset.Match`: the last matching rule wins; no match has no owners.
fn ruleset_owns(rules: &[CodeownersRule], path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    let mut absolute = String::with_capacity(path.len() + 1);
    absolute.push('/');
    absolute.push_str(path);
    for rule in rules.iter().rev() {
        if glob_matches(&rule.parts, &absolute) {
            return rule.has_owners;
        }
    }
    false
}

/// `analyticsIndexer.codeowners`: ingested `codeowners.contents` first, then
/// the baseline's git locations. Any error makes the whole ruleset absent
/// (`noOwners`), exactly like the Go helper's `if ruleset == nil || err != nil`.
async fn load_codeowners(
    pool: &PgPool,
    git: &dyn AnalyticsProvider,
    repo: &str,
    repo_id: i32,
    commit: &str,
) -> Vec<CodeownersRule> {
    match sg_db::ownership_stats::ingested_codeowners(pool, repo_id).await {
        Ok(Some(contents)) => return parse_codeowners(&contents),
        Ok(None) => {}
        // A store error makes `RulesetForRepo` fail, which the Go helper
        // collapses to `noOwners` without falling back to git files.
        Err(_) => return Vec::new(),
    }
    for path in CODEOWNERS_LOCATIONS {
        match git.read_file_optional(repo, commit, path).await {
            Ok(Some(contents)) => return parse_codeowners(&contents),
            Ok(None) => continue,
            Err(_) => return Vec::new(),
        }
    }
    Vec::new()
}

/// The union of assigned owner and team paths (`.Match` only tests presence).
async fn load_assigned_paths(pool: &PgPool, repo_id: i32) -> HashMap<String, ()> {
    let owners = sg_db::ownership_stats::assigned_owner_paths(pool, repo_id).await;
    let teams = sg_db::ownership_stats::assigned_team_paths(pool, repo_id).await;
    match (owners, teams) {
        (Ok(owners), Ok(teams)) => {
            let mut paths: HashMap<String, ()> = owners.into_iter().map(|path| (path, ())).collect();
            for path in teams {
                paths.insert(path, ());
            }
            paths
        }
        // `analyticsIndexer.assignedOwners` returns `noOwners` on either error.
        _ => HashMap::new(),
    }
}

/// `analyticsIndexer.indexRepo`.
pub async fn index_analytics(
    pool: &PgPool,
    git: &dyn AnalyticsProvider,
    repo_id: i32,
    sub_repo_permissions_enabled: bool,
) -> Result<(), String> {
    // 🚨 SECURITY: the background indexer uses the internal actor, so it sees
    // every repository and file. Only sub-repo-permission repos are skipped.
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
        None => return Err(format!("repoStore.Get: repo not found: id={repo_id}")),
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

    let commit = git
        .resolve_revision(&repo.name, "HEAD")
        .await
        .map_err(|err| wrap_error(CANNOT_RESOLVE_HEAD, &err))?;

    let entries = git
        .read_dir_recursive(&repo.name, &commit)
        .await
        .map_err(|err| wrap_error("ls-tree", &err))?;

    let rules = load_codeowners(pool, git, &repo.name, repo_id, &commit).await;
    let assigned = load_assigned_paths(pool, repo_id).await;

    let mut total_count: i32 = 0;
    let mut counts = sg_db::ownership_stats::PathAggregateCounts::default();
    for entry in entries {
        if entry.is_dir {
            continue;
        }
        total_count += 1;
        let codeowned = ruleset_owns(&rules, &entry.name);
        let assigned_owned = sg_db::ownership_stats::assigned_match(&assigned, &entry.name);
        if codeowned {
            counts.codeowned_file_count += 1;
        }
        if assigned_owned {
            counts.assigned_ownership_file_count += 1;
        }
        if codeowned || assigned_owned {
            counts.total_owned_file_count += 1;
        }
    }

    let timestamp = OffsetDateTime::now_utc();
    let rows = sg_db::repo_paths::update_file_counts(
        pool,
        repo_id,
        "",
        total_count,
        timestamp,
    )
    .await
    .map_err(|err| wrap_error("UpdateFileCounts", &err))?;
    if rows == 0 {
        return Err("expected total file count updates".to_owned());
    }

    let rows =
        sg_db::ownership_stats::update_aggregate_counts(pool, repo_id, "", counts, timestamp)
            .await
            .map_err(|err| wrap_error("UpdateAggregateCounts", &err))?;
    if rows == 0 {
        return Err("expected CODEOWNERS-owned file count update".to_owned());
    }
    Ok(())
}

/// `workerutil.NonRetryableError` classification for the generic worker.
pub fn is_non_retryable(failure_message: &str) -> bool {
    failure_message == "unsupported own index job type"
        || failure_message.starts_with("sub-repo permissions check: ")
        || failure_message.starts_with(CANNOT_RESOLVE_HEAD)
}

/// `handler.Handle`.
pub struct OwnBackgroundHandler {
    pool: PgPool,
    git: Arc<dyn RecentContributionsProvider>,
    analytics_git: Arc<dyn AnalyticsProvider>,
    live_config: Arc<LiveConfig>,
    limiter: Arc<OwnRateLimiter>,
}

impl OwnBackgroundHandler {
    pub fn new(
        pool: PgPool,
        git: Arc<dyn RecentContributionsProvider>,
        analytics_git: Arc<dyn AnalyticsProvider>,
        live_config: Arc<LiveConfig>,
        limiter: Arc<OwnRateLimiter>,
    ) -> Self {
        Self {
            pool,
            git,
            analytics_git,
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
            SIGNAL_ANALYTICS => {
                index_analytics(
                    &self.pool,
                    self.analytics_git.as_ref(),
                    record.repo_id,
                    sub_repo_permissions_enabled(&self.live_config.current().site),
                )
                .await
            }
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
        let recent_git: Arc<dyn RecentContributionsProvider> = git.clone();
        let analytics_git: Arc<dyn AnalyticsProvider> = git;
        let store = Arc::new(OwnBackgroundStore::new(deps.frontend.clone()));
        let limiter = Arc::new(OwnRateLimiter::new(rate_limit_config(&unified.site)));

        let worker = WorkerRoutine::new(
            Arc::clone(&store),
            Arc::new(OwnBackgroundHandler::new(
                deps.frontend.clone(),
                recent_git,
                analytics_git,
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

        // `recent-views` is not queued per repo; it has its own periodic
        // routine appended after the per-repo schedulers.
        let recent_views = PeriodicRoutine::new(Arc::new(RecentViewsHandler::new(
            deps.frontend.clone(),
            deps.live_config.clone(),
        )))
        .with_name(SIGNAL_RECENT_VIEWS)
        .with_description("")
        .with_interval(RECENT_VIEWS_REFRESH_INTERVAL);
        routines.push(recent_views.into());

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
        assert!(is_non_retryable("cannot resolve HEAD: revision not found"));
        assert!(is_non_retryable("sub-repo permissions check: boom"));
        assert!(!is_non_retryable("repoStore.Get: repo not found: id=1"));
        assert!(!is_non_retryable("Commits: boom"));
    }

    fn owns(pattern: &str, path: &str) -> bool {
        let rules = parse_codeowners(&format!("{pattern} @owner\n"));
        ruleset_owns(&rules, path)
    }

    #[test]
    fn codeowners_globs_match_the_baseline() {
        assert!(owns("*", "a.go"));
        assert!(owns("*.go", "a.go"));
        assert!(!owns("*.go", "README.md"));
        assert!(owns("/src/", "src/b.go"));
        assert!(!owns("/src/", "a.go"));
        assert!(owns("src/**", "src/deep/c.go"));
        assert!(!owns("src/**", "src"));
        assert!(owns("src/*.go", "src/b.go"));
        assert!(!owns("src/*.go", "src/deep/c.go"));
    }

    #[test]
    fn codeowners_last_rule_wins_and_comments_are_ignored() {
        let rules = parse_codeowners("*.go @a\n/src/b.go\n");
        assert!(ruleset_owns(&rules, "a.go"));
        assert!(!ruleset_owns(&rules, "src/b.go"));

        let rules = parse_codeowners(
            "# comment\n[documentation]\ndocs/ @docs\nsrc/*.go @gophers # tail\n",
        );
        assert!(ruleset_owns(&rules, "docs/guide.md"));
        assert!(ruleset_owns(&rules, "src/b.go"));
        assert!(!ruleset_owns(&rules, "src/deep/c.go"));
        assert!(is_section("[documentation]"));
        assert!(is_section("^[Documentation][2]"));
        assert!(!is_section("[not closed"));
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
                "recent-views",
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
        assert_eq!(routines[5].description(), "");
        assert_eq!(routines[5].interval(), RECENT_VIEWS_REFRESH_INTERVAL);
    }
}
