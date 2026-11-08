#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/uploads_janitor.go`: the
//! `codeintel-upload-janitor` job.
//!
//! The job owns `uploads.JanitorConfigInst` and returns the fourteen routines
//! built by `uploads.NewJanitor` (`internal/codeintel/shared/background`):
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.uploads.janitor.unknown-repository` | cleanup | delete uploads of deleted/blocked repos |
//! | `codeintel.uploads.janitor.unknown-commit` | cleanup | resolve upload commits via gitserver |
//! | `codeintel.uploads.janitor.abandoned` | cleanup | delete uploads stuck in `uploading` |
//! | `codeintel.uploads.expirer.unreferenced` | cleanup | soft-delete expired unreferenced uploads |
//! | `codeintel.uploads.expirer.unreferenced-graph` | cleanup | soft-delete an expired reference tree |
//! | `codeintel.uploads.hard-deleter` | cleanup | delete data of `deleted` uploads |
//! | `codeintel.uploads.janitor.audit-logs` | cleanup | delete old `lsif_uploads_audit_logs` |
//! | `codeintel.uploads.janitor.scip-documents` | cleanup | delete unreferenced SCIP documents |
//! | `codeintel.uploads.janitor.abandoned-schema-versions-records` | 24h | delete orphan schema-version rows |
//! | `codeintel.autoindexing.janitor.unknown-repository` | cleanup | delete indexes of deleted/blocked repos |
//! | `codeintel.autoindexing.janitor.unknown-commit` | cleanup | resolve index commits via gitserver |
//! | `codeintel.autoindexing.janitor.expired` | cleanup | delete old failed index records |
//! | `codeintel.uploads.reconciler.scip-metadata` | cleanup | count metadata without code-intel data |
//! | `codeintel.uploads.reconciler.scip-data` | cleanup | delete code-intel data without metadata |
//!
//! `CODEINTEL_UPLOAD_JANITOR_DISABLED_SUB_JOBS` (comma separated) drops
//! routines by the names above, with a warning each, exactly like the
//! baseline. `uploads.NewJanitor` returns them from a Go map, so their order is
//! unspecified; this implementation uses the source order.
//!
//! Not ported: the `JanitorMetrics` RED metrics and the two
//! `src_<routine>_records_{scanned,altered}_total` counters (workerutil
//! metrics slice), and the `actor.WithInternalActor` context (the Rust SQL
//! omits the authz `TRUE` condition, see `sg_db::codeintel_uploads`).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `uploadJanitorJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `CODEINTEL_UPLOAD_JANITOR_DISABLED_SUB_JOBS` (sic, no `S` in `UPLOAD`).
pub const DISABLED_SUB_JOBS_VAR: &str = "CODEINTEL_UPLOAD_JANITOR_DISABLED_SUB_JOBS";

/// `expiredUploadsBatchSize`.
pub const EXPIRED_UPLOADS_BATCH_SIZE: i64 = 1000;
/// `expiredUploadsMaxTraversal`.
pub const EXPIRED_UPLOADS_MAX_TRAVERSAL: i64 = 100;
/// `uploadsBatchSize` of the hard deleter.
pub const HARD_DELETE_UPLOADS_BATCH_SIZE: i64 = 100;

pub const ROUTINE_DELETED_REPOSITORY: &str = "codeintel.uploads.janitor.unknown-repository";
pub const ROUTINE_UNKNOWN_COMMIT: &str = "codeintel.uploads.janitor.unknown-commit";
pub const ROUTINE_ABANDONED: &str = "codeintel.uploads.janitor.abandoned";
pub const ROUTINE_EXPIRED_UPLOADS: &str = "codeintel.uploads.expirer.unreferenced";
pub const ROUTINE_EXPIRED_UPLOADS_GRAPH: &str = "codeintel.uploads.expirer.unreferenced-graph";
pub const ROUTINE_HARD_DELETER: &str = "codeintel.uploads.hard-deleter";
pub const ROUTINE_AUDIT_LOGS: &str = "codeintel.uploads.janitor.audit-logs";
pub const ROUTINE_SCIP_DOCUMENTS: &str = "codeintel.uploads.janitor.scip-documents";
pub const ROUTINE_ABANDONED_SCHEMA_VERSIONS: &str =
    "codeintel.uploads.janitor.abandoned-schema-versions-records";
pub const ROUTINE_UNKNOWN_REPOSITORY: &str = "codeintel.autoindexing.janitor.unknown-repository";
pub const ROUTINE_UNKNOWN_COMMIT2: &str = "codeintel.autoindexing.janitor.unknown-commit";
pub const ROUTINE_EXPIRED_RECORDS: &str = "codeintel.autoindexing.janitor.expired";
pub const ROUTINE_FRONTEND_RECONCILER: &str = "codeintel.uploads.reconciler.scip-metadata";
pub const ROUTINE_CODEINTEL_RECONCILER: &str = "codeintel.uploads.reconciler.scip-data";

pub const DESCRIPTION_DELETED_REPOSITORY: &str =
    "Removes upload records associated with an unknown repository.";
pub const DESCRIPTION_UNKNOWN_COMMIT: &str =
    "Removes upload records associated with an unknown commit.";
pub const DESCRIPTION_ABANDONED: &str =
    "Removes upload records that did did not receive a full payload from the user.";
pub const DESCRIPTION_EXPIRED_UPLOADS: &str =
    "Soft-deletes unreferenced upload records that are not protected by any data retention policy.";
pub const DESCRIPTION_EXPIRED_UPLOADS_GRAPH: &str =
    "Soft-deletes a tree of externally unreferenced upload records that are not protected by any data retention policy.";
pub const DESCRIPTION_HARD_DELETER: &str =
    "Deleted data associated with soft-deleted upload records.";
pub const DESCRIPTION_AUDIT_LOGS: &str = "Deletes sufficiently old upload audit log records.";
pub const DESCRIPTION_SCIP_DOCUMENTS: &str =
    "Deletes SCIP document payloads that are not referenced by any index.";
pub const DESCRIPTION_ABANDONED_SCHEMA_VERSIONS: &str =
    "Deletes schema version metadata records for indexes that no longer exist.";
pub const DESCRIPTION_UNKNOWN_REPOSITORY: &str =
    "Removes index records associated with an unknown repository.";
pub const DESCRIPTION_UNKNOWN_COMMIT2: &str =
    "Removes index records associated with an unknown commit.";
pub const DESCRIPTION_EXPIRED_RECORDS: &str = "Removes old index records";
pub const DESCRIPTION_FRONTEND_RECONCILER: &str =
    "Counts SCIP metadata records for which there is no data in the codeintel-db schema.";
pub const DESCRIPTION_CODEINTEL_RECONCILER: &str =
    "Removes SCIP data records for which there is no known associated metadata in the frontend schema.";

/// `janitor.Config`.
#[derive(Debug, Default)]
pub struct UploadJanitorConfig {
    interval: Mutex<Duration>,
    abandoned_schema_versions_interval: Mutex<Duration>,
    minimum_time_since_last_check: Mutex<Duration>,
    commit_resolver_batch_size: Mutex<i64>,
    audit_log_max_age: Mutex<Duration>,
    unreferenced_document_batch_size: Mutex<i64>,
    unreferenced_document_max_age: Mutex<Duration>,
    commit_resolver_maximum_commit_lag: Mutex<Duration>,
    upload_timeout: Mutex<Duration>,
    reconciler_batch_size: Mutex<i64>,
    failed_index_batch_size: Mutex<i64>,
    failed_index_max_age: Mutex<Duration>,
    errs: Mutex<Vec<String>>,
}

macro_rules! getter {
    ($name:ident, $field:ident, $ty:ty) => {
        pub fn $name(&self) -> $ty {
            *self
                .$field
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    };
}

impl UploadJanitorConfig {
    getter!(interval, interval, Duration);
    getter!(
        abandoned_schema_versions_interval,
        abandoned_schema_versions_interval,
        Duration
    );
    getter!(
        minimum_time_since_last_check,
        minimum_time_since_last_check,
        Duration
    );
    getter!(commit_resolver_batch_size, commit_resolver_batch_size, i64);
    getter!(audit_log_max_age, audit_log_max_age, Duration);
    getter!(
        unreferenced_document_batch_size,
        unreferenced_document_batch_size,
        i64
    );
    getter!(
        unreferenced_document_max_age,
        unreferenced_document_max_age,
        Duration
    );
    getter!(
        commit_resolver_maximum_commit_lag,
        commit_resolver_maximum_commit_lag,
        Duration
    );
    getter!(upload_timeout, upload_timeout, Duration);
    getter!(reconciler_batch_size, reconciler_batch_size, i64);
    getter!(failed_index_batch_size, failed_index_batch_size, i64);
    getter!(failed_index_max_age, failed_index_max_age, Duration);
}

impl JobConfig for UploadJanitorConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        // `env.ChooseFallbackVariableName`: the first name backed by a
        // non-empty variable wins.
        let minimum_time_since_last_check_name = choose_fallback(&[
            "CODEINTEL_UPLOADS_MINIMUM_TIME_SINCE_LAST_CHECK",
            "PRECISE_CODE_INTEL_COMMIT_RESOLVER_MINIMUM_TIME_SINCE_LAST_CHECK",
        ]);
        let commit_resolver_batch_size_name = choose_fallback(&[
            "CODEINTEL_UPLOADS_COMMIT_RESOLVER_BATCH_SIZE",
            "PRECISE_CODE_INTEL_COMMIT_RESOLVER_BATCH_SIZE",
        ]);
        let audit_log_max_age_name = choose_fallback(&[
            "CODEINTEL_UPLOADS_AUDIT_LOG_MAX_AGE",
            "PRECISE_CODE_INTEL_AUDIT_LOG_MAX_AGE",
        ]);
        let commit_resolver_maximum_commit_lag_name = choose_fallback(&[
            "CODEINTEL_UPLOADS_COMMIT_RESOLVER_MAXIMUM_COMMIT_LAG",
            "PRECISE_CODE_INTEL_COMMIT_RESOLVER_MAXIMUM_COMMIT_LAG",
        ]);
        let upload_timeout_name = choose_fallback(&[
            "CODEINTEL_UPLOADS_UPLOAD_TIMEOUT",
            "PRECISE_CODE_INTEL_UPLOAD_TIMEOUT",
        ]);

        *self.lock_interval() = get_interval(
            env,
            "CODEINTEL_UPLOADS_CLEANUP_INTERVAL",
            "1m",
            "How frequently to run the updater janitor routine.",
            &mut errs,
        );
        *self.lock_abandoned_schema_versions_interval() = get_interval(
            env,
            "CODEINTEL_UPLOADS_ABANDONED_SCHEMA_VERSIONS_CLEANUP_INTERVAL",
            "24h",
            "How frequently to run the query to clean up *_schema_version records that are not tracked by foreign key.",
            &mut errs,
        );
        *self.lock_minimum_time_since_last_check() = get_interval(
            env,
            &minimum_time_since_last_check_name,
            "24h",
            "The minimum time the commit resolver will re-check an upload or index record.",
            &mut errs,
        );
        *self.lock_commit_resolver_batch_size() = get_int(
            env,
            &commit_resolver_batch_size_name,
            "100",
            "The maximum number of unique commits to resolve at a time.",
            &mut errs,
        );
        *self.lock_audit_log_max_age() = get_interval(
            env,
            &audit_log_max_age_name,
            "720h",
            "The maximum time a code intel audit log record can remain on the database.",
            &mut errs,
        );
        *self.lock_unreferenced_document_batch_size() = get_int(
            env,
            "CODEINTEL_UPLOADS_UNREFERENCED_DOCUMENT_BATCH_SIZE",
            "100",
            "The number of unreferenced SCIP documents to consider for deletion at a time.",
            &mut errs,
        );
        *self.lock_unreferenced_document_max_age() = get_interval(
            env,
            "CODEINTEL_UPLOADS_UNREFERENCED_DOCUMENT_MAX_AGE",
            "24h",
            "The maximum time an unreferenced SCIP document should remain in the database.",
            &mut errs,
        );
        *self.lock_commit_resolver_maximum_commit_lag() = get_interval(
            env,
            &commit_resolver_maximum_commit_lag_name,
            "0s",
            "The maximum acceptable delay between accepting an upload and its commit becoming resolvable. Be cautious about setting this to a large value, as uploads for unresolvable commits will be retried periodically during this interval.",
            &mut errs,
        );
        *self.lock_upload_timeout() = get_interval(
            env,
            &upload_timeout_name,
            "24h",
            "The maximum time an upload can be in the 'uploading' state.",
            &mut errs,
        );
        *self.lock_reconciler_batch_size() = get_int(
            env,
            "CODEINTEL_UPLOADS_RECONCILER_BATCH_SIZE",
            "1000",
            "The number of uploads to reconcile in one cleanup routine invocation.",
            &mut errs,
        );
        *self.lock_failed_index_batch_size() = get_int(
            env,
            "CODEINTEL_AUTOINDEXING_FAILED_INDEX_BATCH_SIZE",
            "1000",
            "The number of old, failed index records to delete at once.",
            &mut errs,
        );
        *self.lock_failed_index_max_age() = get_interval(
            env,
            "CODEINTEL_AUTOINDEXING_FAILED_INDEX_MAX_AGE",
            "730h",
            "The maximum age a non-relevant failed index record will remain queryable.",
            &mut errs,
        );

        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::new(combine_errors(&errs).unwrap_or_default()))
        }
    }
}

impl UploadJanitorConfig {
    fn lock_interval(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_abandoned_schema_versions_interval(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.abandoned_schema_versions_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_minimum_time_since_last_check(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.minimum_time_since_last_check
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_commit_resolver_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.commit_resolver_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_audit_log_max_age(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.audit_log_max_age
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_unreferenced_document_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.unreferenced_document_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_unreferenced_document_max_age(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.unreferenced_document_max_age
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_commit_resolver_maximum_commit_lag(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.commit_resolver_maximum_commit_lag
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_upload_timeout(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.upload_timeout
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_reconciler_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.reconciler_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_failed_index_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.failed_index_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_failed_index_max_age(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.failed_index_max_age
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// `env.Get`/`env.GetInt` for integers.
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
        Err(_) => {
            errors.push(format!(
                "parsing environment variable {name:?}. Expected valid integer, got {raw:?}"
            ));
            0
        }
    }
}

fn get_interval(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> Duration {
    let raw = env.get(name, default, description);
    match sg_core::parse_go_duration(&raw) {
        Ok(duration) => duration,
        Err(message) => {
            errors.push(format!("invalid duration {raw:?} for {name}: {message}"));
            Duration::ZERO
        }
    }
}

/// `env.ChooseFallbackVariableName`: the first name whose environment variable
/// is non-empty; otherwise the first name.
fn choose_fallback(names: &[&str]) -> String {
    names
        .iter()
        .find(|name| {
            std::env::var(name)
                .map(|value| !value.is_empty())
                .unwrap_or(false)
        })
        .copied()
        .unwrap_or(names[0])
        .to_owned()
}

/// `uploadJanitorJob`.
pub struct UploadJanitor {
    config: Arc<UploadJanitorConfig>,
}

impl UploadJanitor {
    pub fn new() -> Self {
        Self {
            config: Arc::new(UploadJanitorConfig::default()),
        }
    }

    pub fn config(&self) -> &UploadJanitorConfig {
        &self.config
    }
}

impl Default for UploadJanitor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for UploadJanitor {
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
        // `codeintel.InitServices`: the code-intel DB comes from the frontend
        // configuration (`ServiceConnections.CodeIntelPostgresDSN`), and the
        // gitserver client from `ServiceConnections.GitServers`.
        let unified = deps.live_config.current();
        if unified
            .service_connections
            .code_intel_postgres_dsn
            .is_empty()
        {
            return Err("code_intel_postgres_dsn is not configured by the frontend".to_owned());
        }
        let codeintel = sg_db::connect(&unified.service_connections.code_intel_postgres_dsn, 10)
            .await
            .map_err(|err| format!("connecting to the codeintel database: {err}"))?;
        let git = Arc::new(sg_git::GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));

        let config = &self.config;
        let disabled: HashSet<String> = std::env::var(DISABLED_SUB_JOBS_VAR)
            .unwrap_or_default()
            .split(',')
            .map(str::to_owned)
            .collect();

        let deps_inner = Arc::new(JanitorDeps {
            frontend: deps.frontend.clone(),
            codeintel,
            git,
            minimum_time_since_last_check: config.minimum_time_since_last_check(),
            commit_resolver_maximum_commit_lag: config.commit_resolver_maximum_commit_lag(),
            commit_resolver_batch_size: config.commit_resolver_batch_size(),
            audit_log_max_age: config.audit_log_max_age(),
            unreferenced_document_batch_size: config.unreferenced_document_batch_size(),
            unreferenced_document_max_age: config.unreferenced_document_max_age(),
            upload_timeout: config.upload_timeout(),
            reconciler_batch_size: config.reconciler_batch_size(),
            failed_index_batch_size: config.failed_index_batch_size(),
            failed_index_max_age: config.failed_index_max_age(),
        });

        let interval = config.interval();
        let mut routines = Vec::with_capacity(14);
        let add = |name: &str,
                   description: &str,
                   interval: Duration,
                   task: JanitorTask|
         -> Option<BackgroundRoutine> {
            if disabled.contains(name) {
                tracing::warn!(name, "DISABLING CODE INTEL UPLOAD JANITOR SUB-JOB");
                return None;
            }
            Some(
                PeriodicRoutine::new(Arc::new(JanitorHandler {
                    deps: Arc::clone(&deps_inner),
                    task,
                }))
                .with_name(name)
                .with_description(description)
                .with_interval(interval)
                .into(),
            )
        };

        routines.extend(add(
            ROUTINE_DELETED_REPOSITORY,
            DESCRIPTION_DELETED_REPOSITORY,
            interval,
            JanitorTask::DeletedRepository,
        ));
        routines.extend(add(
            ROUTINE_UNKNOWN_COMMIT,
            DESCRIPTION_UNKNOWN_COMMIT,
            interval,
            JanitorTask::UnknownCommit,
        ));
        routines.extend(add(
            ROUTINE_ABANDONED,
            DESCRIPTION_ABANDONED,
            interval,
            JanitorTask::Abandoned,
        ));
        routines.extend(add(
            ROUTINE_EXPIRED_UPLOADS,
            DESCRIPTION_EXPIRED_UPLOADS,
            interval,
            JanitorTask::ExpiredUploads,
        ));
        routines.extend(add(
            ROUTINE_EXPIRED_UPLOADS_GRAPH,
            DESCRIPTION_EXPIRED_UPLOADS_GRAPH,
            interval,
            JanitorTask::ExpiredUploadsGraph,
        ));
        routines.extend(add(
            ROUTINE_HARD_DELETER,
            DESCRIPTION_HARD_DELETER,
            interval,
            JanitorTask::HardDeleter,
        ));
        routines.extend(add(
            ROUTINE_AUDIT_LOGS,
            DESCRIPTION_AUDIT_LOGS,
            interval,
            JanitorTask::AuditLogs,
        ));
        routines.extend(add(
            ROUTINE_SCIP_DOCUMENTS,
            DESCRIPTION_SCIP_DOCUMENTS,
            interval,
            JanitorTask::ScipDocuments,
        ));
        routines.extend(add(
            ROUTINE_ABANDONED_SCHEMA_VERSIONS,
            DESCRIPTION_ABANDONED_SCHEMA_VERSIONS,
            config.abandoned_schema_versions_interval(),
            JanitorTask::AbandonedSchemaVersions,
        ));
        routines.extend(add(
            ROUTINE_UNKNOWN_REPOSITORY,
            DESCRIPTION_UNKNOWN_REPOSITORY,
            interval,
            JanitorTask::UnknownRepository,
        ));
        routines.extend(add(
            ROUTINE_UNKNOWN_COMMIT2,
            DESCRIPTION_UNKNOWN_COMMIT2,
            interval,
            JanitorTask::UnknownCommit2,
        ));
        routines.extend(add(
            ROUTINE_EXPIRED_RECORDS,
            DESCRIPTION_EXPIRED_RECORDS,
            interval,
            JanitorTask::ExpiredRecords,
        ));
        routines.extend(add(
            ROUTINE_FRONTEND_RECONCILER,
            DESCRIPTION_FRONTEND_RECONCILER,
            interval,
            JanitorTask::FrontendDbReconciler,
        ));
        routines.extend(add(
            ROUTINE_CODEINTEL_RECONCILER,
            DESCRIPTION_CODEINTEL_RECONCILER,
            interval,
            JanitorTask::CodeIntelDbReconciler,
        ));

        Ok(routines)
    }
}

/// Values captured from the config when the routines are built.
struct JanitorDeps {
    frontend: PgPool,
    codeintel: PgPool,
    git: Arc<sg_git::GitserverClient>,
    minimum_time_since_last_check: Duration,
    commit_resolver_maximum_commit_lag: Duration,
    commit_resolver_batch_size: i64,
    audit_log_max_age: Duration,
    unreferenced_document_batch_size: i64,
    unreferenced_document_max_age: Duration,
    upload_timeout: Duration,
    reconciler_batch_size: i64,
    failed_index_batch_size: i64,
    failed_index_max_age: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JanitorTask {
    DeletedRepository,
    UnknownCommit,
    Abandoned,
    ExpiredUploads,
    ExpiredUploadsGraph,
    HardDeleter,
    AuditLogs,
    ScipDocuments,
    AbandonedSchemaVersions,
    UnknownRepository,
    UnknownCommit2,
    ExpiredRecords,
    FrontendDbReconciler,
    CodeIntelDbReconciler,
}

/// The `CleanupFunc` of every sub-job; the baseline ignores the counters
/// except for the metrics counters, which are not ported.
struct JanitorHandler {
    deps: Arc<JanitorDeps>,
    task: JanitorTask,
}

#[async_trait]
impl Handler for JanitorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let deps = &self.deps;
        let now = OffsetDateTime::now_utc();
        let error = |err: String| HandlerError::new(err);
        let sql = |err: sqlx::Error| HandlerError::new(err.to_string());

        let _counts: (i64, i64) = match self.task {
            JanitorTask::DeletedRepository => {
                sg_db::codeintel_uploads::delete_uploads_without_repository(&deps.frontend, now)
                    .await
                    .map_err(sql)?
            }
            JanitorTask::UnknownCommit => {
                let deleter = UploadCommitDeleter {
                    git: Arc::clone(&deps.git),
                };
                sg_db::codeintel_uploads::process_sourced_commits(
                    &deps.frontend,
                    deps.minimum_time_since_last_check,
                    deps.commit_resolver_maximum_commit_lag,
                    deps.commit_resolver_batch_size,
                    &deleter,
                    now,
                )
                .await
                .map_err(error)?
            }
            JanitorTask::Abandoned => {
                let uploaded_before = now - deps.upload_timeout;
                sg_db::codeintel_uploads::delete_uploads_stuck_uploading(
                    &deps.frontend,
                    uploaded_before,
                )
                .await
                .map_err(sql)?
            }
            JanitorTask::ExpiredUploads => sg_db::codeintel_uploads::soft_delete_expired_uploads(
                &deps.frontend,
                EXPIRED_UPLOADS_BATCH_SIZE,
            )
            .await
            .map_err(sql)?,
            JanitorTask::ExpiredUploadsGraph => {
                sg_db::codeintel_uploads::soft_delete_expired_uploads_via_traversal(
                    &deps.frontend,
                    EXPIRED_UPLOADS_MAX_TRAVERSAL,
                )
                .await
                .map_err(sql)?
            }
            JanitorTask::HardDeleter => {
                let mut count = 0;
                loop {
                    let (ids, total) = sg_db::codeintel_uploads::get_deleted_upload_ids(
                        &deps.frontend,
                        HARD_DELETE_UPLOADS_BATCH_SIZE,
                    )
                    .await
                    .map_err(sql)?;
                    sg_db::codeintel_codegraph::delete_lsif_data_by_upload_ids(
                        &deps.codeintel,
                        &ids,
                    )
                    .await
                    .map_err(sql)?;
                    sg_db::codeintel_uploads::hard_delete_uploads_by_ids(&deps.frontend, &ids)
                        .await
                        .map_err(sql)?;

                    count += ids.len() as i64;
                    if count >= total {
                        break;
                    }
                }
                (count, count)
            }
            JanitorTask::AuditLogs => sg_db::codeintel_uploads::delete_old_audit_logs(
                &deps.frontend,
                deps.audit_log_max_age,
                now,
            )
            .await
            .map_err(sql)?,
            JanitorTask::ScipDocuments => {
                sg_db::codeintel_codegraph::delete_unreferenced_documents(
                    &deps.codeintel,
                    deps.unreferenced_document_batch_size,
                    deps.unreferenced_document_max_age,
                    now,
                )
                .await
                .map_err(sql)?
            }
            JanitorTask::AbandonedSchemaVersions => {
                let count = sg_db::codeintel_codegraph::delete_abandoned_schema_versions_records(
                    &deps.codeintel,
                )
                .await
                .map_err(sql)?;
                (count, count)
            }
            JanitorTask::UnknownRepository => {
                sg_db::codeintel_uploads::delete_auto_index_jobs_without_repository(
                    &deps.frontend,
                    now,
                )
                .await
                .map_err(sql)?
            }
            JanitorTask::UnknownCommit2 => {
                let deleter = UploadCommitDeleter {
                    git: Arc::clone(&deps.git),
                };
                sg_db::codeintel_uploads::process_stale_sourced_commits(
                    &deps.frontend,
                    deps.minimum_time_since_last_check,
                    deps.commit_resolver_batch_size,
                    &deleter,
                    now,
                )
                .await
                .map_err(error)?
            }
            JanitorTask::ExpiredRecords => sg_db::codeintel_uploads::expire_failed_records(
                &deps.frontend,
                deps.failed_index_batch_size,
                deps.failed_index_max_age,
                now,
            )
            .await
            .map_err(sql)?,
            JanitorTask::FrontendDbReconciler => {
                let candidates = sg_db::codeintel_uploads::reconcile_candidates(
                    &deps.frontend,
                    deps.reconciler_batch_size,
                )
                .await
                .map_err(sql)?;
                reconcile(
                    &deps.frontend,
                    &deps.codeintel,
                    ReconcileMode::FrontendDb,
                    candidates,
                )
                .await
                .map_err(HandlerError::new)?
            }
            JanitorTask::CodeIntelDbReconciler => {
                let candidates = sg_db::codeintel_codegraph::reconcile_candidates(
                    &deps.codeintel,
                    deps.reconciler_batch_size,
                    now,
                )
                .await
                .map_err(sql)?;
                reconcile(
                    &deps.frontend,
                    &deps.codeintel,
                    ReconcileMode::CodeIntelDb,
                    candidates,
                )
                .await
                .map_err(HandlerError::new)?
            }
        };

        Ok(())
    }
}

/// `newReconciler`'s `CleanupFunc`: candidates → existing → prune the missing
/// ids. Returns `(numScanned, numAltered)`.
///
/// `FrontendDb` counts SCIP metadata rows that have no code-intel data (prune
/// is a no-op, matching the baseline's `storeWrapper.Prune`); `CodeIntelDb`
/// deletes code-intel data rows that have no metadata.
async fn reconcile(
    frontend: &PgPool,
    codeintel: &PgPool,
    mode: ReconcileMode,
    candidates: Vec<i64>,
) -> Result<(i64, i64), String> {
    let existing = match mode {
        ReconcileMode::FrontendDb => {
            sg_db::codeintel_codegraph::ids_with_meta(codeintel, &candidates)
                .await
                .map_err(|err| err.to_string())?
        }
        ReconcileMode::CodeIntelDb => {
            sg_db::codeintel_uploads::existing_upload_ids(frontend, &candidates)
                .await
                .map_err(|err| err.to_string())?
        }
    };
    let existing: HashSet<i64> = existing.into_iter().collect();
    let missing: Vec<i64> = candidates
        .iter()
        .copied()
        .filter(|id| !existing.contains(id))
        .collect();

    match mode {
        ReconcileMode::FrontendDb => {}
        ReconcileMode::CodeIntelDb => {
            sg_db::codeintel_codegraph::delete_lsif_data_by_upload_ids(codeintel, &missing)
                .await
                .map_err(|err| err.to_string())?;
        }
    }

    Ok((candidates.len() as i64, missing.len() as i64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileMode {
    FrontendDb,
    CodeIntelDb,
}

/// `shouldDeleteRecordsForCommit`: delete when the commit does not resolve,
/// keep when the repository does not exist (a separate janitor removes those
/// records), propagate anything else.
struct UploadCommitDeleter {
    git: Arc<sg_git::GitserverClient>,
}

#[async_trait]
impl sg_db::codeintel_uploads::CommitDeleter for UploadCommitDeleter {
    async fn should_delete(
        &self,
        _repository_id: i64,
        repository_name: &str,
        commit: &str,
    ) -> Result<bool, String> {
        match self
            .git
            .resolve_revision(repository_name, commit, true)
            .await
        {
            Ok(_) => Ok(false),
            Err(sg_git::Error::RepoNotFound { .. }) => Ok(false),
            Err(err) if err.is_revision_not_found() => Ok(true),
            Err(err) => Err(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, PoisonError};

    /// `invalid_durations_are_reported` sets a process environment variable
    /// that `config_defaults_match_the_baseline` reads; cargo runs the two
    /// tests in parallel threads, so they must not overlap.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = UploadJanitor::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = UploadJanitorConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.interval(), Duration::from_secs(60));
        assert_eq!(
            config.abandoned_schema_versions_interval(),
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(
            config.minimum_time_since_last_check(),
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(config.commit_resolver_batch_size(), 100);
        assert_eq!(
            config.audit_log_max_age(),
            Duration::from_secs(720 * 60 * 60)
        );
        assert_eq!(config.unreferenced_document_batch_size(), 100);
        assert_eq!(
            config.unreferenced_document_max_age(),
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(config.commit_resolver_maximum_commit_lag(), Duration::ZERO);
        assert_eq!(config.upload_timeout(), Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.reconciler_batch_size(), 1000);
        assert_eq!(config.failed_index_batch_size(), 1000);
        assert_eq!(
            config.failed_index_max_age(),
            Duration::from_secs(730 * 60 * 60)
        );
    }

    #[test]
    fn invalid_durations_are_reported() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = UploadJanitorConfig::default();
        std::env::set_var("CODEINTEL_UPLOADS_CLEANUP_INTERVAL", "soon");
        let mut env = EnvReader::new();
        config.load(&mut env);
        std::env::remove_var("CODEINTEL_UPLOADS_CLEANUP_INTERVAL");
        let err = config.validate().expect_err("invalid interval");
        assert_eq!(
            err.to_string(),
            "invalid duration \"soon\" for CODEINTEL_UPLOADS_CLEANUP_INTERVAL: time: invalid duration \"soon\""
        );
    }
}
