#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/uploads_backfiller.go`: the
//! `codeintel-upload-backfiller` job.
//!
//! The job owns `uploads.BackfillerConfigInst` (`backfiller.Config`) and
//! returns the single routine built by `uploads.NewCommittedAtBackfillerJob`:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.committed-at-backfiller` | `CODEINTEL_UPLOAD_BACKFILLER_INTERVAL` (30s) | fill `codeintel_commit_dates.committed_at` for completed uploads |
//!
//! Each invocation runs `BackfillCommittedAtBatch` inside one transaction:
//!
//! 1. `SourcedCommitsWithoutCommittedAt` reads a batch of
//!    `(repository_id, repository_name, commit)` triples for completed uploads
//!    whose commit has no `codeintel_commit_dates` row;
//! 2. every commit is resolved through gitserver (`GetCommit`); a
//!    `RevisionNotFound` becomes the `-infinity` sentinel so the row is not
//!    retried forever;
//! 3. `UpdateCommittedAt` inserts `(repository_id, commit_bytea, committed_at)`
//!    with `ON CONFLICT DO NOTHING`;
//! 4. `SetRepositoryAsDirty` bumps `lsif_dirty_repositories` so the commit
//!    graph is recalculated with the fresh dates.
//!
//! The commit date is formatted with Go's `time.RFC3339` layout; gitserver's
//! protobuf timestamp is UTC, so the truncation to whole seconds is the only
//! transformation (`2020-01-02T03:04:05Z`).
//!
//! Not ported: the `backfiller` RED metrics / `observation` spans (metrics
//! slice) and the `actor.WithInternalActor` context (the SQL has no authz
//! condition; see `sg_db::codeintel_uploads`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_uploads;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `uploadBackfillerJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `backfiller.NewCommittedAtBackfiller`'s routine metadata.
pub const ROUTINE_NAME: &str = "codeintel.committed-at-backfiller";
pub const ROUTINE_DESCRIPTION: &str = "backfills the committed_at column for code-intel uploads";

/// `Config.Interval` / `Config.BatchSize`.
pub const INTERVAL_VAR: &str = "CODEINTEL_UPLOAD_BACKFILLER_INTERVAL";
pub const BATCH_SIZE_VAR: &str = "CODEINTEL_UPLOAD_BACKFILLER_BATCH_SIZE";
const INTERVAL_DESCRIPTION: &str =
    "The frequency with which to run periodic codeintel backfiller tasks.";
const BATCH_SIZE_DESCRIPTION: &str =
    "The number of upload to populate an unset `commited_at` field per batch.";

/// `backfiller.Config` (`uploads.BackfillerConfigInst`).
#[derive(Debug, Default)]
pub struct BackfillerConfig {
    interval: Mutex<Duration>,
    batch_size: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl BackfillerConfig {
    pub fn interval(&self) -> Duration {
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn batch_size(&self) -> i64 {
        *self
            .batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobConfig for BackfillerConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            get_interval(env, INTERVAL_VAR, "30s", INTERVAL_DESCRIPTION, &mut errs);
        *self
            .batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_int(
            env,
            BATCH_SIZE_VAR,
            "100",
            BATCH_SIZE_DESCRIPTION,
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

/// `env.GetInt` (`BaseConfig.GetInt`).
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

/// `cmd/worker/internal/codeintel.NewUploadBackfillerJob`.
#[derive(Debug, Default)]
pub struct UploadBackfiller {
    config: Arc<BackfillerConfig>,
}

impl UploadBackfiller {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(&self) -> &BackfillerConfig {
        &self.config
    }
}

#[async_trait]
impl Job for UploadBackfiller {
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
        // `codeintel.InitServices`' `GitserverClient` is built from
        // `ServiceConnections.GitServers`; no code-intel database is needed.
        let unified = deps.live_config.current();
        let git = Arc::new(sg_git::GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));

        let routine = PeriodicRoutine::new(Arc::new(BackfillHandler {
            frontend: deps.frontend.clone(),
            resolver: Arc::new(GitserverCommitResolver { git }),
            batch_size: self.config.batch_size(),
        }))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(self.config.interval());

        Ok(vec![routine.into()])
    }
}

/// `backfiller.NewCommittedAtBackfiller`'s handler.
struct BackfillHandler {
    frontend: PgPool,
    resolver: Arc<dyn CommitDateResolver>,
    batch_size: i64,
}

#[async_trait]
impl Handler for BackfillHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        backfill_committed_at_batch(&self.frontend, &*self.resolver, self.batch_size)
            .await
            .map_err(HandlerError::new)
    }
}

/// `gitserverClient.GetCommit`'s result, reduced to what the backfiller uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitDate {
    /// `commit.Committer.Date` (UTC seconds + nanos).
    Timestamp { seconds: i64, nanos: i32 },
    /// `gitdomain.RevisionNotFoundError`.
    RevisionNotFound,
}

/// The gitserver lookup behind `BackfillCommittedAtBatch`; a trait so the
/// differential harness can stub it, exactly like the janitor's
/// `CommitDeleter`.
#[async_trait]
pub trait CommitDateResolver: Send + Sync {
    async fn resolve(&self, repository_name: &str, commit: &str) -> Result<CommitDate, String>;
}

/// Production resolver: `gitserver.Client.GetCommit`.
struct GitserverCommitResolver {
    git: Arc<sg_git::GitserverClient>,
}

#[async_trait]
impl CommitDateResolver for GitserverCommitResolver {
    async fn resolve(&self, repository_name: &str, commit: &str) -> Result<CommitDate, String> {
        match self.git.get_commit(repository_name, commit).await {
            Ok(commit) => match commit.committer_date.as_ref() {
                Some(timestamp) => Ok(CommitDate::Timestamp {
                    seconds: timestamp.seconds,
                    nanos: timestamp.nanos,
                }),
                // `commit.Committer.Date` would panic in the baseline;
                // report an error instead.
                None => Err("commit has no committer date".to_owned()),
            },
            Err(err) if err.is_revision_not_found() => Ok(CommitDate::RevisionNotFound),
            Err(err) => Err(err.to_string()),
        }
    }
}

/// `backfiller.BackfillCommittedAtBatch`: the whole batch runs in a single
/// transaction, matching `store.WithTransaction`.
pub async fn backfill_committed_at_batch(
    pool: &PgPool,
    resolver: &dyn CommitDateResolver,
    batch_size: i64,
) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let batch = codeintel_uploads::sourced_commits_without_committed_at(&mut tx, batch_size)
        .await
        .map_err(|err| format!("store.SourcedCommitsWithoutCommittedAt: {err}"))?;

    for sourced in &batch {
        for commit in &sourced.commits {
            let commit_date = match resolver.resolve(&sourced.repository_name, commit).await? {
                CommitDate::Timestamp { seconds, nanos } => format_commit_date(seconds, nanos)?,
                CommitDate::RevisionNotFound => {
                    // Sentinel filtered out by the query side, so the same
                    // failing batch is not retried forever.
                    "-infinity".to_owned()
                }
            };

            codeintel_uploads::update_committed_at(
                &mut tx,
                sourced.repository_id,
                commit,
                &commit_date,
            )
            .await
            .map_err(|err| format!("store.UpdateCommittedAt: {err}"))?;
        }

        codeintel_uploads::set_repository_as_dirty(&mut tx, sourced.repository_id)
            .await
            .map_err(|err| format!("store.SetRepositoryAsDirty: {err}"))?;
    }

    tx.commit().await.map_err(|err| err.to_string())
}

/// `commit.Committer.Date.Format(time.RFC3339)`: gitserver's protobuf
/// timestamp is UTC, and the layout truncates fractional seconds.
fn format_commit_date(seconds: i64, _nanos: i32) -> Result<String, String> {
    let date = time::OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|err| err.to_string())?
        .replace_nanosecond(0)
        .map_err(|err| err.to_string())?;
    date.format(&time::format_description::well_known::Rfc3339)
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, PoisonError};

    /// `config_rejects_malformed_values` sets process environment variables
    /// that `config_defaults_match_the_baseline` reads; cargo runs the two
    /// tests in parallel threads, so they must not overlap.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = UploadBackfiller::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = BackfillerConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.interval(), Duration::from_secs(30));
        assert_eq!(config.batch_size(), 100);
    }

    #[test]
    fn config_rejects_malformed_values() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = BackfillerConfig::default();
        let mut env = EnvReader::new();
        std::env::set_var(INTERVAL_VAR, "not-a-duration");
        std::env::set_var(BATCH_SIZE_VAR, "not-an-int");
        config.load(&mut env);
        std::env::remove_var(INTERVAL_VAR);
        std::env::remove_var(BATCH_SIZE_VAR);
        assert!(config.validate().is_err());
    }

    #[test]
    fn commit_dates_use_the_go_rfc3339_layout() {
        // Fractional seconds are truncated by `time.RFC3339`.
        assert_eq!(
            format_commit_date(1_577_934_245, 123_000_000).unwrap(),
            "2020-01-02T03:04:05Z"
        );
        assert_eq!(format_commit_date(0, 0).unwrap(), "1970-01-01T00:00:00Z");
    }
}
