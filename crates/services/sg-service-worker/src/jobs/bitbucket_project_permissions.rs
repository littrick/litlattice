#![forbid(unsafe_code)]

//! `bitbucket-project-permissions`
//! (`cmd/worker/internal/permissions/bitbucket_projects.go`).
//!
//! Applies an explicit-permissions job to every repository of a Bitbucket
//! Server project. The job owns one config (`permissions.ConfigInst`) and
//! returns two `DB_BACKED` routines:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `explicit_permissions_bitbucket_projects_jobs_worker` | `BITBUCKET_PROJECT_PERMISSIONS_WORKER_POLL_INTERVAL` (10s) | dequeue one `explicit_permissions_bitbucket_projects_jobs` row and `Handle` it |
//! | `explicit_permissions_bitbucket_projects_jobs_worker_resetter` | 1m | `ResetStalled` (`StalledMaxAge` 60s, `MaxNumResets` 0) |
//!
//! The worker store is the generic `dbworker` store configured by
//! `createBitbucketProjectPermissionsStore`: `RetryAfter`
//! `BITBUCKET_PROJECT_PERMISSIONS_WORKER_RETRY_INTERVAL` (30s),
//! `MaxNumRetries` 5, order by `id`. Because `MaxNumResets` is unset (0), the
//! reset query never matches (`num_resets < 0`) and the failure query matches
//! every stalled row, so a stalled job goes straight to `failed`.
//!
//! `Handle` reproduces the baseline flow and its error wrapping:
//!
//! 1. `ExternalServices().GetByID` — every failure is non-retryable
//!    (`failed to get external service %d: …`);
//! 2. the kind must be `BITBUCKETSERVER`
//!    (`expected Bitbucket Server external service, got: …`, non-retryable);
//! 3. `getBitbucketClient` decrypts the config and builds a
//!    [`BitbucketServerClient`];
//! 4. `ProjectRepos` lists every repository of the project;
//! 5. `getRepoIDsByNames` maps Bitbucket repository IDs to `repo.id` through
//!    the external-repo spec (`bitbucketServer` + normalised base URL), with
//!    the same `deleted_at`/`blocked` conditions as `Repos().List`;
//! 6. unrestricted jobs call `SetRepoPermissionsUnrestricted(ids, true)`; other
//!    jobs map bind IDs to users (`MapUsers`) and write, per repository in one
//!    transaction, the restricted marker, the API-sourced repo permissions and
//!    the pending permissions for bind IDs without a user.
//!
//! Errata (docs §74.3):
//!
//! * `authorization.oauth` (OAuth 1.0a request signing) is rejected with an
//!   explicit error instead of being signed; Basic and Bearer auth match the
//!   baseline;
//! * the `certificate` client option is not implemented;
//! * `MapUsers`'s `filtered = append(bindIDs, bindID)` quirk is reproduced
//!   exactly ([`filtered_bind_ids`]), including its effect on padded bind IDs.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::{ConfigError, EnvReader};
use sg_db::external_services::{ExternalService, ExternalServiceError, ExternalServiceStore};
use sg_db::perms_store::{
    set_repo_pending_permissions_tx, set_repo_permissions_unrestricted,
    set_repo_permissions_unrestricted_tx, set_repo_perms_tx, UserIdWithExternalAccountId,
    SOURCE_API,
};
use sg_keyring::Keyring;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::bitbucket_server_client::{
    go_quote, BitbucketServerClient, BitbucketServerConnection, Repo,
};
use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::dbworker::{
    default_hostname, DbBackedRoutine, DbRecord, DbResetter, DbWorkerHandler, DbWorkerStore,
    ResetterOptions, ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use crate::framework::BackgroundRoutine;

/// `bitbucketProjectPermissionsJob.Description`.
pub const JOB_DESCRIPTION: &str =
    "Applies explicit permissions to all repositories of a Bitbucket Project.";

/// `workerutil.WorkerOptions.Name`.
pub const WORKER_NAME: &str = "explicit_permissions_bitbucket_projects_jobs_worker";
/// `workerutil.WorkerOptions.Description`.
pub const WORKER_DESCRIPTION: &str = "syncs Bitbucket Projects via Explicit Permissions API";
/// `dbworker.ResetterOptions.Name`.
pub const RESETTER_NAME: &str = "explicit_permissions_bitbucket_projects_jobs_worker_resetter";

/// `dbworkerstore.Options.MaxNumRetries`.
pub const MAX_NUM_RETRIES: i32 = 5;
/// `dbworkerstore.Options.StalledMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(60);
/// `dbworkerstore.Options.MaxNumResets` (unset in the baseline, hence 0).
pub const MAX_NUM_RESETS: i32 = 0;
/// `workerutil.WorkerOptions.HeartbeatInterval`.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `dbworker.ResetterOptions.Interval`.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(60);

/// `defaultResetFailureMessage`.
pub const DEFAULT_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

/// `authz.SourcegraphServiceType`.
pub const SOURCEGRAPH_SERVICE_TYPE: &str = "sourcegraph";
/// `authz.SourcegraphServiceID`.
pub const SOURCEGRAPH_SERVICE_ID: &str = "https://sourcegraph.com/";
/// `authz.Read`.
pub const PERM_READ: &str = "read";

/// `BITBUCKET_PROJECT_PERMISSIONS_WORKER_POLL_INTERVAL`.
pub const POLL_INTERVAL_VAR: &str = "BITBUCKET_PROJECT_PERMISSIONS_WORKER_POLL_INTERVAL";
/// `BITBUCKET_PROJECT_PERMISSIONS_WORKER_CONCURRENCY`.
pub const CONCURRENCY_VAR: &str = "BITBUCKET_PROJECT_PERMISSIONS_WORKER_CONCURRENCY";
/// `BITBUCKET_PROJECT_PERMISSIONS_WORKER_RETRY_INTERVAL`.
pub const RETRY_INTERVAL_VAR: &str = "BITBUCKET_PROJECT_PERMISSIONS_WORKER_RETRY_INTERVAL";

const POLL_INTERVAL_DESCRIPTION: &str = "How frequently to query the job queue";
const CONCURRENCY_DESCRIPTION: &str =
    "The maximum number of projects that can be processed concurrently";
const RETRY_INTERVAL_DESCRIPTION: &str =
    "The minimum number of time to wait before retrying a failed job";

pub const POLL_INTERVAL_MUST_BE_NON_NEGATIVE: &str =
    "BITBUCKET_PROJECT_PERMISSIONS_WORKER_POLL_INTERVAL must be greater than or equal to 0";
pub const CONCURRENCY_MUST_BE_POSITIVE: &str =
    "BITBUCKET_PROJECT_PERMISSIONS_WORKER_CONCURRENCY must be greater than 0";

/// `permissions.config` (`ConfigInst`).
///
/// `time.Duration` is signed: `-1s` is a legal parse that `Validate` rejects
/// but `Load` still stores, and the baseline exposes it via
/// `int64(d.Seconds())`. `std::time::Duration` cannot represent that, so the
/// intervals are kept as signed nanoseconds and clamped in the accessors the
/// worker actually uses (validation has already failed by then).
#[derive(Debug, Default)]
pub struct BitbucketProjectPermissionsConfig {
    poll_interval_nanos: Mutex<i64>,
    concurrency: Mutex<i64>,
    retry_interval_nanos: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl BitbucketProjectPermissionsConfig {
    pub fn poll_interval(&self) -> Duration {
        nanos_to_duration(*lock(&self.poll_interval_nanos))
    }

    /// Signed nanoseconds, for the differential harness (`Seconds()` in Go).
    pub fn poll_interval_nanos(&self) -> i64 {
        *lock(&self.poll_interval_nanos)
    }

    pub fn concurrency(&self) -> i64 {
        *lock(&self.concurrency)
    }

    pub fn retry_interval(&self) -> Duration {
        nanos_to_duration(*lock(&self.retry_interval_nanos))
    }

    /// Signed nanoseconds, for the differential harness (`Seconds()` in Go).
    pub fn retry_interval_nanos(&self) -> i64 {
        *lock(&self.retry_interval_nanos)
    }
}

fn nanos_to_duration(nanos: i64) -> Duration {
    Duration::from_nanos(nanos.max(0) as u64)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl JobConfig for BitbucketProjectPermissionsConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        *lock(&self.poll_interval_nanos) = get_signed_interval(
            env,
            POLL_INTERVAL_VAR,
            "10s",
            POLL_INTERVAL_DESCRIPTION,
            &mut errs,
        );

        *lock(&self.concurrency) = get_int(
            env,
            CONCURRENCY_VAR,
            "1",
            CONCURRENCY_DESCRIPTION,
            &mut errs,
        );
        *lock(&self.retry_interval_nanos) = get_signed_interval(
            env,
            RETRY_INTERVAL_VAR,
            "30s",
            RETRY_INTERVAL_DESCRIPTION,
            &mut errs,
        );

        if *lock(&self.poll_interval_nanos) < 0 {
            errs.push(POLL_INTERVAL_MUST_BE_NON_NEGATIVE.to_owned());
        }
        if *lock(&self.concurrency) < 1 {
            errs.push(CONCURRENCY_MUST_BE_POSITIVE.to_owned());
        }

        *lock(&self.errs) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let errs = lock(&self.errs).clone();
        match combine_errors(&errs) {
            None => Ok(()),
            Some(message) => Err(ConfigError::new(message)),
        }
    }
}

/// `env.BaseConfig.GetInt`: `strconv.ParseInt`'s error text is part of the
/// validation message, so reproduce the `invalid int %q for %s: %s` form
/// (unlike `env.MustGetInt`, which panics at package init).
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match raw.parse::<i64>() {
        Ok(value) => value,
        Err(err) => {
            use std::num::IntErrorKind;
            let reason = match err.kind() {
                IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => "value out of range",
                _ => "invalid syntax",
            };
            errors.push(format!(
                "invalid int {raw:?} for {name}: strconv.ParseInt: parsing {raw:?}: {reason}"
            ));
            0
        }
    }
}

/// `env.BaseConfig.GetInterval`, but keeping the sign: `time.ParseDuration`
/// accepts `-1s`, and the baseline stores it (validation rejects it later).
fn get_signed_interval(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match parse_signed_go_duration(&raw) {
        Ok(nanos) => nanos,
        Err(message) => {
            errors.push(format!("invalid duration {raw:?} for {name}: {message}"));
            0
        }
    }
}

/// `time.ParseDuration` with the sign preserved, in nanoseconds.
///
/// `sg_core::parse_go_duration` parses the unsigned magnitude (and produces
/// the baseline's error text for it), so only the sign and the quoted value
/// in the error message need restoring.
fn parse_signed_go_duration(raw: &str) -> Result<i64, String> {
    let (negative, unsigned) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    let value = match sg_core::parse_go_duration(unsigned) {
        Ok(value) => value,
        Err(err) => {
            let prefix = if err.starts_with("time: missing unit in duration") {
                "time: missing unit in duration"
            } else {
                "time: invalid duration"
            };
            return Err(format!("{prefix} {raw:?}"));
        }
    };
    let magnitude = value.as_nanos() as i128;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).map_err(|_| format!("time: invalid duration {raw:?}"))
}

/// `types.UserPermission`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct UserPermission {
    #[serde(rename = "bindID", default)]
    pub bind_id: String,
    #[serde(default)]
    pub permission: String,
}

/// `types.BitbucketProjectPermissionJob` (the columns the worker reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketProjectPermissionJob {
    pub id: i64,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub worker_hostname: String,
    pub project_key: String,
    pub external_service_id: i64,
    pub permissions: Vec<UserPermission>,
    pub unrestricted: bool,
}

impl DbRecord for BitbucketProjectPermissionJob {
    fn record_id(&self) -> i64 {
        self.id
    }
}

/// `makeDequeueSelectExpressions`: the values the dequeue `UPDATE` assigns are
/// substituted literally, because the CTE's changes are not visible to the
/// final `SELECT` in the same statement.
fn dequeue_select_columns() -> String {
    "id, 'processing'::text AS state, NULL::text AS failure_message, queued_at, \
     $1::timestamptz AS started_at, NULL::timestamptz AS finished_at, process_after, \
     num_resets, num_failures, $1::timestamptz AS last_heartbeat_at, \
     $3::text AS worker_hostname, project_key, external_service_id, \
     to_json(permissions)::text AS permissions, unrestricted"
        .to_owned()
}

fn scan_job(row: &sqlx::postgres::PgRow) -> Result<BitbucketProjectPermissionJob, String> {
    let permissions: Option<String> = row.try_get("permissions").map_err(stringify)?;
    let permissions = match permissions {
        None => Vec::new(),
        Some(raw) => serde_json::from_str(&raw)
            .map_err(|err| format!("failed to unmarshal permissions: {err}"))?,
    };
    Ok(BitbucketProjectPermissionJob {
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
        project_key: row.try_get("project_key").map_err(stringify)?,
        external_service_id: row.try_get("external_service_id").map_err(stringify)?,
        permissions,
        unrestricted: row.try_get("unrestricted").map_err(stringify)?,
    })
}

fn stringify(err: sqlx::Error) -> String {
    err.to_string()
}

/// `explicit_permissions_bitbucket_projects_jobs` through the generic
/// `dbworker/store.Store` with the options from
/// `createBitbucketProjectPermissionsStore`.
#[derive(Clone)]
pub struct BitbucketProjectPermissionsStore {
    pool: PgPool,
    retry_interval: Duration,
}

impl BitbucketProjectPermissionsStore {
    pub fn new(pool: PgPool, retry_interval: Duration) -> Self {
        Self {
            pool,
            retry_interval,
        }
    }

    fn retry_after_seconds(&self) -> i32 {
        self.retry_interval.as_secs() as i32
    }
}

#[async_trait]
impl DbWorkerStore for BitbucketProjectPermissionsStore {
    type Record = BitbucketProjectPermissionJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        let now = OffsetDateTime::now_utc();
        let retry_after = self.retry_after_seconds();
        let query = format!(
            r#"
WITH potential_candidates AS (
    SELECT
        explicit_permissions_bitbucket_projects_jobs.id AS candidate_id,
        ROW_NUMBER() OVER (ORDER BY explicit_permissions_bitbucket_projects_jobs.id) AS order
    FROM explicit_permissions_bitbucket_projects_jobs
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
    ORDER BY explicit_permissions_bitbucket_projects_jobs.id
    LIMIT 50
),
candidate AS (
    SELECT
        explicit_permissions_bitbucket_projects_jobs.id
    FROM explicit_permissions_bitbucket_projects_jobs
    JOIN potential_candidates pc ON pc.candidate_id = explicit_permissions_bitbucket_projects_jobs.id
    WHERE
        state IN ('queued', 'errored')
    ORDER BY pc.order
    FOR UPDATE OF explicit_permissions_bitbucket_projects_jobs SKIP LOCKED
    LIMIT 1
),
updated_record AS (
    UPDATE
        explicit_permissions_bitbucket_projects_jobs
    SET
        state = 'processing',
        started_at = $1,
        last_heartbeat_at = $1,
        finished_at = NULL,
        failure_message = NULL,
        execution_logs = NULL,
        worker_hostname = $3
    WHERE
        explicit_permissions_bitbucket_projects_jobs.id IN (SELECT id FROM candidate)
)
SELECT {}
FROM
    explicit_permissions_bitbucket_projects_jobs
WHERE
    explicit_permissions_bitbucket_projects_jobs.id IN (SELECT id FROM candidate)
"#,
            dequeue_select_columns()
        );

        let rows = sqlx::query(&query)
            .bind(now)
            .bind(retry_after)
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
        explicit_permissions_bitbucket_projects_jobs.id
    FROM
        explicit_permissions_bitbucket_projects_jobs
    WHERE
        explicit_permissions_bitbucket_projects_jobs.id = ANY($1) AND
        state = 'processing'
    ORDER BY
        explicit_permissions_bitbucket_projects_jobs.id ASC
    FOR UPDATE
)
UPDATE
    explicit_permissions_bitbucket_projects_jobs
SET
    last_heartbeat_at = $2
WHERE
    explicit_permissions_bitbucket_projects_jobs.id IN (SELECT id FROM alive_candidates)
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
            let id: i64 = row.try_get("id").map_err(stringify)?;
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
UPDATE explicit_permissions_bitbucket_projects_jobs
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
UPDATE explicit_permissions_bitbucket_projects_jobs
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
UPDATE explicit_permissions_bitbucket_projects_jobs
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
impl DbResetter for BitbucketProjectPermissionsStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let now = OffsetDateTime::now_utc();
        let stalled_secs = STALLED_MAX_AGE.as_secs() as i32;

        let reset = sqlx::query(
            r#"
WITH stalled AS (
    SELECT id FROM explicit_permissions_bitbucket_projects_jobs
    WHERE
        state = 'processing' AND
        $1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
        num_resets < $3
    FOR UPDATE SKIP LOCKED
)
UPDATE explicit_permissions_bitbucket_projects_jobs
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
    SELECT id FROM explicit_permissions_bitbucket_projects_jobs
    WHERE
        state = 'processing' AND
        $1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
        num_resets >= $3
    FOR UPDATE SKIP LOCKED
)
UPDATE explicit_permissions_bitbucket_projects_jobs
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

fn scan_ages(
    rows: Vec<sqlx::postgres::PgRow>,
    now: OffsetDateTime,
) -> Result<Vec<(i64, Duration)>, String> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.try_get("id").map_err(stringify)?;
        let last_heartbeat: OffsetDateTime = row.try_get("last_heartbeat_at").map_err(stringify)?;
        let age = now - last_heartbeat;
        ages.push((id, age.try_into().unwrap_or(Duration::ZERO)));
    }
    ages.sort_by_key(|(id, _)| *id);
    Ok(ages)
}

/// `bitbucketProjectPermissionsHandler`.
pub struct BitbucketProjectPermissionsHandler {
    pool: PgPool,
    keyring: Arc<Keyring>,
    live_config: Arc<LiveConfig>,
    http: reqwest::Client,
}

impl BitbucketProjectPermissionsHandler {
    pub fn new(
        pool: PgPool,
        keyring: Arc<Keyring>,
        live_config: Arc<LiveConfig>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            pool,
            keyring,
            live_config,
            http,
        }
    }

    async fn decrypt_config(
        &self,
        svc: &ExternalService,
    ) -> Result<BitbucketServerConnection, String> {
        let raw = sg_encryption::maybe_decrypt(
            self.keyring.external_service_key().as_deref(),
            &svc.config,
            &svc.encryption_key_id,
        )
        .await
        .map_err(|err| format!("external service id={} config error: {err}", svc.id))?;
        let plain = sg_config::strip_jsonc(&raw);
        serde_json::from_str::<BitbucketServerConnection>(&plain)
            .map_err(|err| format!("external service id={} config error: {err}", svc.id))
    }

    /// `getRepoIDsByNames`.
    async fn get_repo_ids_by_names(
        &self,
        _svc: &ExternalService,
        config: &BitbucketServerConnection,
        repos: &[Repo],
    ) -> Result<Vec<i32>, String> {
        if repos.is_empty() {
            return Ok(Vec::new());
        }

        // The baseline decrypts the config again here; the already-decrypted
        // config is the same value, so a second decryption cannot fail once
        // `getBitbucketClient` succeeded.
        let base = normalize_base_url(&config.url)
            .map_err(|_| "error during parsing external service URL".to_owned())?;
        let service_type = "bitbucketServer";
        let external_ids: Vec<String> = repos.iter().map(|repo| repo.id.to_string()).collect();
        let service_types = vec![service_type.to_owned(); repos.len()];
        let service_ids = vec![base; repos.len()];

        let rows = sqlx::query(
            r#"
SELECT repo.id
FROM repo
WHERE
    repo.deleted_at IS NULL AND
    repo.blocked IS NULL AND
    EXISTS (
        SELECT 1
        FROM unnest($1::text[], $2::text[], $3::text[]) AS spec(external_id, external_service_type, external_service_id)
        WHERE
            repo.external_id = spec.external_id AND
            repo.external_service_type = spec.external_service_type AND
            repo.external_service_id = spec.external_service_id
    )
ORDER BY repo.id ASC
"#,
        )
        .bind(external_ids)
        .bind(service_types)
        .bind(service_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;

        rows_to_ids(&rows)
    }

    /// `setPermissionsForUsers`.
    async fn set_permissions_for_users(
        &self,
        perms: &[UserPermission],
        repo_ids: Vec<i32>,
        _project_key: &str,
    ) -> Result<(), String> {
        let mut perms = perms.to_vec();
        perms.sort_by(|left, right| left.bind_id.cmp(&right.bind_id));
        let mut repo_ids = repo_ids;
        repo_ids.sort_unstable();

        let bind_ids: Vec<String> = perms.iter().map(|perm| perm.bind_id.clone()).collect();
        let mapping = permissions_user_mapping(&self.live_config.current().site);
        let mapped = map_users(&self.pool, &bind_ids, &mapping.bind_id)
            .await
            .map_err(|err| format!("failed to map bind IDs to user IDs: {err}"))?;

        let user_ids: BTreeSet<i32> = mapped.values().copied().collect();
        let pending_bind_ids: Vec<String> = bind_ids
            .iter()
            .filter(|bind_id| !mapped.contains_key(*bind_id))
            .cloned()
            .collect();

        for repo_id in repo_ids {
            self.set_repo_permissions(repo_id, &user_ids, &pending_bind_ids)
                .await
                .map_err(|err| format!("failed to set permissions for repo {repo_id}: {err}"))?;
        }
        Ok(())
    }

    /// `setRepoPermissions`: one transaction per repository. Public so the
    /// differential harness can exercise the `repo not found` path directly
    /// (the handler only ever passes IDs it just read from `repo`).
    pub async fn set_repo_permissions(
        &self,
        repo_id: i32,
        user_ids: &BTreeSet<i32>,
        pending_bind_ids: &[String],
    ) -> Result<(), String> {
        self.repo_exists(repo_id)
            .await
            .map_err(|err| format!("failed to query repo {repo_id}: {err}"))?;

        let users: Vec<UserIdWithExternalAccountId> = user_ids
            .iter()
            .map(|user_id| UserIdWithExternalAccountId {
                user_id: *user_id,
                external_account_id: 0,
            })
            .collect();
        let now = OffsetDateTime::now_utc();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| format!("failed to start transaction: {err}"))?;

        set_repo_permissions_unrestricted_tx(&mut tx, &[repo_id], false, now)
            .await
            .map_err(|err| format!("failed to set repo {repo_id} to restricted: {err}"))?;

        set_repo_perms_tx(&mut tx, repo_id, &users, SOURCE_API, now)
            .await
            .map_err(|err| {
                format!(
                    "failed to set user repo permissions for repo {repo_id} and users {}: {err}",
                    go_users(&users)
                )
            })?;

        set_repo_pending_permissions_tx(
            &mut tx,
            repo_id,
            PERM_READ,
            SOURCEGRAPH_SERVICE_TYPE,
            SOURCEGRAPH_SERVICE_ID,
            pending_bind_ids,
            now,
        )
        .await
        .map_err(|err| format!("failed to set pending permissions for repo {repo_id}: {err}"))?;

        tx.commit().await.map_err(stringify)?;
        Ok(())
    }

    /// `repoExists`.
    async fn repo_exists(&self, repo_id: i32) -> Result<(), String> {
        let row = sqlx::query("SELECT id FROM repo WHERE id = $1")
            .bind(repo_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(stringify)?;
        if row.is_none() {
            return Err("repo not found".to_owned());
        }
        Ok(())
    }
}

fn rows_to_ids(rows: &[sqlx::postgres::PgRow]) -> Result<Vec<i32>, String> {
    rows.iter()
        .map(|row| row.try_get("id").map_err(stringify))
        .collect()
}

fn go_users(users: &[UserIdWithExternalAccountId]) -> String {
    let mut out = String::from("[");
    for (index, user) in users.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        out.push_str(&format!(
            "{{{} {}}}",
            user.user_id, user.external_account_id
        ));
    }
    out.push(']');
    out
}

#[async_trait]
impl DbWorkerHandler<BitbucketProjectPermissionsStore> for BitbucketProjectPermissionsHandler {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        job: &BitbucketProjectPermissionJob,
    ) -> Result<(), String> {
        let store = ExternalServiceStore::new(self.pool.clone());
        let svc = match store.get_by_id(job.external_service_id).await {
            Ok(svc) => svc,
            Err(ExternalServiceError::NotFound { id }) => {
                return Err(format!(
                    "failed to get external service {id}: external service not found: {id}"
                ));
            }
            Err(err) => {
                return Err(format!(
                    "failed to get external service {}: {err}",
                    job.external_service_id
                ));
            }
        };

        if svc.kind != "BITBUCKETSERVER" {
            return Err(format!(
                "expected Bitbucket Server external service, got: {}",
                svc.kind
            ));
        }

        let config = self.decrypt_config(&svc).await.map_err(|err| {
            format!(
                "failed to build Bitbucket client for external service {}: {err}",
                svc.id
            )
        })?;
        let client = BitbucketServerClient::new(&config, self.http.clone()).map_err(|err| {
            format!(
                "failed to build Bitbucket client for external service {}: {err}",
                svc.id
            )
        })?;

        let bitbucket_repos = client
            .project_repos(&job.project_key)
            .await
            .map_err(|err| {
                format!(
                    "failed to list repositories of Bitbucket Project {}: {err}",
                    go_quote(&job.project_key)
                )
            })?;

        let repo_ids = self
            .get_repo_ids_by_names(&svc, &config, &bitbucket_repos)
            .await
            .map_err(|err| format!("failed to get gitserver repos from the database: {err}"))?;

        if job.unrestricted {
            let mut repo_ids = repo_ids;
            repo_ids.sort_unstable();
            set_repo_permissions_unrestricted(
                &self.pool,
                &repo_ids,
                true,
                OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|err| {
                format!(
                    "failed to set permissions to unrestricted for Bitbucket Project {}: {err}",
                    go_quote(&job.project_key)
                )
            })?;
            return Ok(());
        }

        self.set_permissions_for_users(&job.permissions, repo_ids, &job.project_key)
            .await
            .map_err(|err| {
                format!(
                    "failed to set permissions for Bitbucket Project {}: {err}",
                    go_quote(&job.project_key)
                )
            })
    }

    fn non_retryable(&self, failure_message: &str) -> bool {
        is_non_retryable(failure_message)
    }
}

/// `service.SetRepoPermissionsUnrestricted`'s error class is not part of the
/// message; the classifier only needs the baseline's `errcode.MakeNonRetryable`
/// cases.
pub fn is_non_retryable(failure_message: &str) -> bool {
    if failure_message.starts_with("failed to get external service ") {
        return true;
    }
    if failure_message.starts_with("expected Bitbucket Server external service, got: ") {
        return true;
    }
    if failure_message.contains("failed to query repo ") {
        return true;
    }
    // The baseline wrapper (`failed to build Bitbucket client for ...`) is a
    // plain `errors.Wrapf` without `errcode.MakeNonRetryable`, so an
    // unsupported OAuth 1.0 configuration stays retryable. See docs §74.3.
    if let Some(code) = bitbucket_http_error_code(failure_message) {
        return code < 200 || (300..500).contains(&code) || code == 501 || code >= 600;
    }
    false
}

fn bitbucket_http_error_code(message: &str) -> Option<u16> {
    let rest = message
        .split_once("Bitbucket API HTTP error: code=")
        .map(|(_, rest)| rest)?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// `schema.PermissionsUserMapping` as returned by `conf.PermissionsUserMapping`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionsUserMapping {
    pub enabled: bool,
    pub bind_id: String,
}

impl Default for PermissionsUserMapping {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_id: "email".to_owned(),
        }
    }
}

/// `conf.PermissionsUserMapping`: the flat `permissions.userMapping` key, with
/// every missing or invalid value falling back to `{Enabled: false,
/// BindID: "email"}`.
pub fn permissions_user_mapping(site: &SiteConfiguration) -> PermissionsUserMapping {
    let Some(mapping) = site.raw().get("permissions.userMapping") else {
        return PermissionsUserMapping::default();
    };
    let bind_id = mapping
        .get("bindID")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if bind_id != "email" && bind_id != "username" {
        return PermissionsUserMapping::default();
    }
    PermissionsUserMapping {
        enabled: mapping
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        bind_id: bind_id.to_owned(),
    }
}

/// `MapUsers`'s filtered slice, reproducing
/// `filtered = append(bindIDs, bindID)` from the baseline: every non-blank
/// input yields `bindIDs` plus that iteration's trimmed value, so the result
/// is the original list with the *last* non-blank value appended; an all-blank
/// list filters down to nothing (docs §74.3).
pub fn filtered_bind_ids(bind_ids: &[String]) -> Vec<String> {
    let last = bind_ids.iter().rev().find_map(|bind_id| {
        let trimmed = bind_id.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    });
    match last {
        None => Vec::new(),
        Some(last) => {
            let mut filtered = bind_ids.to_vec();
            filtered.push(last);
            filtered
        }
    }
}

/// `PermsStore.MapUsers`.
pub async fn map_users(
    pool: &PgPool,
    bind_ids: &[String],
    bind_id_type: &str,
) -> Result<HashMap<String, i32>, String> {
    let filtered = filtered_bind_ids(bind_ids);
    match bind_id_type {
        "email" => {
            if filtered.is_empty() {
                return Ok(HashMap::new());
            }
            // `email` is citext; the lookup is case-insensitive while the
            // comparison below is the baseline's exact (case-sensitive) one.
            let rows = sqlx::query(
                r#"
SELECT user_id, email::text AS email
FROM user_emails
WHERE email = ANY($1::citext[]) AND verified_at IS NOT NULL
"#,
            )
            .bind(filtered.clone())
            .fetch_all(pool)
            .await
            .map_err(stringify)?;
            let mut mapping = HashMap::new();
            for row in rows {
                let user_id: i32 = row.try_get("user_id").map_err(stringify)?;
                let email: String = row.try_get("email").map_err(stringify)?;
                for bind_id in &filtered {
                    if email == *bind_id {
                        mapping.insert(bind_id.clone(), user_id);
                        break;
                    }
                }
            }
            Ok(mapping)
        }
        "username" => {
            if filtered.is_empty() {
                return Ok(HashMap::new());
            }
            let rows = sqlx::query(
                r#"
SELECT id, username::text AS username
FROM users
WHERE username = ANY($1::citext[]) AND deleted_at IS NULL
ORDER BY id ASC
"#,
            )
            .bind(filtered.clone())
            .fetch_all(pool)
            .await
            .map_err(stringify)?;
            let mut mapping = HashMap::new();
            for row in rows {
                let user_id: i32 = row.try_get("id").map_err(stringify)?;
                let username: String = row.try_get("username").map_err(stringify)?;
                for bind_id in &filtered {
                    if username == *bind_id {
                        mapping.insert(bind_id.clone(), user_id);
                        break;
                    }
                }
            }
            Ok(mapping)
        }
        other => Err(format!(
            "unrecognized user mapping bind ID type {}",
            go_quote(other)
        )),
    }
}

/// `extsvc.NormalizeBaseURL`: lowercase the host and ensure the path ends with
/// `/`.
pub fn normalize_base_url(raw: &str) -> Result<String, url::ParseError> {
    let mut url = Url::parse(raw)?;
    let host = url.host_str().map(str::to_ascii_lowercase);
    if let Some(host) = host {
        // `set_host` only fails for hosts the parser already rejected.
        let _ = url.set_host(Some(&host));
    }
    let path = url.path().to_owned();
    if !path.ends_with('/') {
        url.set_path(&format!("{path}/"));
    }
    Ok(url.to_string())
}

/// `cmd/worker/internal/permissions.NewBitbucketProjectPermissionsJob`.
#[derive(Debug, Default)]
pub struct BitbucketProjectPermissionsJob {
    config: Arc<BitbucketProjectPermissionsConfig>,
}

impl BitbucketProjectPermissionsJob {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(&self) -> &BitbucketProjectPermissionsConfig {
        &self.config
    }
}

#[async_trait]
impl Job for BitbucketProjectPermissionsJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![self.config.clone()]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let store = Arc::new(BitbucketProjectPermissionsStore::new(
            deps.frontend.clone(),
            self.config.retry_interval(),
        ));
        let handler = Arc::new(BitbucketProjectPermissionsHandler::new(
            deps.frontend.clone(),
            Arc::clone(&deps.keyring),
            Arc::clone(&deps.live_config),
            reqwest::Client::new(),
        ));

        let worker = WorkerRoutine::new(
            Arc::clone(&store),
            handler,
            WorkerOptions {
                name: WORKER_NAME.to_owned(),
                description: WORKER_DESCRIPTION.to_owned(),
                interval: self.config.poll_interval(),
                heartbeat_interval: HEARTBEAT_INTERVAL,
                num_handlers: self.config.concurrency().max(1) as usize,
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

        Ok(vec![
            BackgroundRoutine::DbBacked(Arc::new(worker) as Arc<dyn DbBackedRoutine>),
            BackgroundRoutine::DbBacked(Arc::new(resetter) as Arc<dyn DbBackedRoutine>),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    #[test]
    fn user_mapping_defaults_and_validation() {
        assert_eq!(
            permissions_user_mapping(&site("{}")),
            PermissionsUserMapping::default()
        );
        assert_eq!(
            permissions_user_mapping(&site(
                r#"{"permissions.userMapping":{"bindID":"username","enabled":true}}"#
            )),
            PermissionsUserMapping {
                enabled: true,
                bind_id: "username".to_owned(),
            }
        );
        assert_eq!(
            permissions_user_mapping(&site(
                r#"{"permissions.userMapping":{"bindID":"login","enabled":true}}"#
            )),
            PermissionsUserMapping::default()
        );
        assert_eq!(
            permissions_user_mapping(&site(r#"{"permissions.userMapping":{"enabled":true}}"#)),
            PermissionsUserMapping::default()
        );
    }

    #[test]
    fn filtered_bind_ids_reproduces_the_baseline_quirk() {
        let strings = |values: &[&str]| {
            values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        };

        assert_eq!(
            filtered_bind_ids(&strings(&["b", "a"])),
            strings(&["b", "a", "a"])
        );
        assert_eq!(
            filtered_bind_ids(&strings(&[" a ", "b"])),
            strings(&[" a ", "b", "b"])
        );
        assert_eq!(
            filtered_bind_ids(&strings(&["", "  "])),
            Vec::<String>::new()
        );
        assert_eq!(filtered_bind_ids(&[]), Vec::<String>::new());
    }

    #[test]
    fn non_retryable_matches_errcode_cases() {
        assert!(is_non_retryable(
            "failed to get external service 1: external service not found: 1"
        ));
        assert!(is_non_retryable(
            "expected Bitbucket Server external service, got: GITHUB"
        ));
        assert!(is_non_retryable(
            "failed to set permissions for repo 1: failed to query repo 1: repo not found"
        ));
        assert!(is_non_retryable(
            r#"failed to list repositories of Bitbucket Project "P": Bitbucket API HTTP error: code=404 url="x" body="""#
        ));
        assert!(!is_non_retryable(
            r#"failed to list repositories of Bitbucket Project "P": Bitbucket API HTTP error: code=500 url="x" body="""#
        ));
        assert!(!is_non_retryable(
            r#"failed to list repositories of Bitbucket Project "P": project key empty"#
        ));
        assert!(!is_non_retryable(
            "failed to build Bitbucket client for external service 1: authorization.oauth: nope"
        ));
    }

    #[test]
    fn bitbucket_http_error_code_extracts_status() {
        assert_eq!(
            bitbucket_http_error_code("x Bitbucket API HTTP error: code=401 url=1"),
            Some(401)
        );
        assert_eq!(bitbucket_http_error_code("nothing"), None);
    }

    #[test]
    fn normalize_base_url_lowercases_and_adds_trailing_slash() {
        assert_eq!(
            normalize_base_url("HTTPS://BBS.Example.COM/Bitbucket").expect("url"),
            "https://bbs.example.com/Bitbucket/"
        );
        assert_eq!(
            normalize_base_url("http://bbs.example.com").expect("url"),
            "http://bbs.example.com/"
        );
        assert_eq!(
            normalize_base_url("http://bbs.example.com:7990/bitbucket/?a=b").expect("url"),
            "http://bbs.example.com:7990/bitbucket/?a=b"
        );
    }

    #[test]
    fn permissions_decode_from_json_array_text() {
        let value: Vec<UserPermission> =
            serde_json::from_str(r#"[{"bindID":"a@example.com","permission":"read"}]"#)
                .expect("permissions");
        assert_eq!(
            value,
            vec![UserPermission {
                bind_id: "a@example.com".to_owned(),
                permission: "read".to_owned(),
            }]
        );
    }

    #[test]
    fn signed_duration_parsing_matches_time_parse_duration() {
        assert_eq!(parse_signed_go_duration("-5s"), Ok(-5_000_000_000));
        assert_eq!(parse_signed_go_duration("-1m30s"), Ok(-90_000_000_000));
        assert_eq!(parse_signed_go_duration("-0s"), Ok(0));
        assert_eq!(parse_signed_go_duration("0s"), Ok(0));
        assert_eq!(parse_signed_go_duration("10s"), Ok(10_000_000_000));
        assert_eq!(
            parse_signed_go_duration("-bogus"),
            Err(r#"time: invalid duration "-bogus""#.to_owned())
        );
        assert_eq!(
            parse_signed_go_duration("5"),
            Err(r#"time: missing unit in duration "5""#.to_owned())
        );
    }

    #[test]
    fn go_users_formats_like_percent_v() {
        assert_eq!(
            go_users(&[
                UserIdWithExternalAccountId {
                    user_id: 7,
                    external_account_id: 0,
                },
                UserIdWithExternalAccountId {
                    user_id: 8,
                    external_account_id: 0,
                },
            ]),
            "[{7 0} {8 0}]"
        );
    }
}
