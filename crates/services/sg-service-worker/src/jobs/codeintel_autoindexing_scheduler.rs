#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/background/scheduler/
//! job_scheduler.go` (the periodic half of the
//! `codeintel-autoindexing-scheduler` job) and its config.
//!
//! When `codeIntelAutoIndexing.enabled` is set, every
//! `CODEINTEL_AUTOINDEXING_SCHEDULER_INTERVAL` (2m) the scheduler
//!
//! 1. claims a batch of repositories with `GetRepositoriesForIndexScan`
//!    (`lsif_last_index_scan`, the precise layout) ordered by
//!    `last_index_scan_at NULLS FIRST, id`;
//! 2. for every claimed repository, reads its configuration policies page by
//!    page (`ForPreciseIndexing=true`, `PolicyBatchSize` rows), matches the
//!    commits those policies describe ([`PolicyMatcher`] over
//!    [`Extractor::Indexing`], `includeTipOfDefaultBranch=false`,
//!    `filterByCreatedDate=true`) and enqueues every `(repository, commit)`
//!    pair through the injected [`IndexEnqueuer`];
//! 3. accumulates per-repository failures with `errors.Append` and returns
//!    them once the whole batch finished.
//!
//! Baseline quirks reproduced here:
//!
//! * the semaphore is acquired in the producer loop with the configured
//!   `InferenceConcurrency`; with a non-positive value the baseline blocks
//!   forever on its first acquire, while this port clamps to one worker and
//!   documents the divergence;
//! * the baseline's `if !errors.As(err, &inference.LimitError{})` checks the
//!   *outer* (nil) `err` instead of `repositoryErr`, so every repository
//!   failure is appended. This port appends every failure;
//! * the aggregate error's order is the goroutines' completion order. The
//!   differential harness pins `InferenceConcurrency=1` for the cases with
//!   more than one failure.
//!
//! Not ported: the on-demand scheduler (`NewOnDemandScheduler`, the
//! `codeintel_autoindex_queue` table) and the `IndexEnqueuer` production
//! implementation (`internal/codeintel/autoindexing/internal/enqueuer` and
//! its inference pipeline). Because of the latter the
//! `codeintel-autoindexing-scheduler` job is still not registered.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_policies::{self, GetConfigurationPoliciesOptions};
use sg_db::repos::RepoStore;
use sg_db::reposcheduler::{RepoScanStore, RepositoryBatchOptions};
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::policies_matcher::{Extractor, MatcherGit, PolicyMatcher};
use super::{get_interval, JobConfig};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `goroutine.WithName("codeintel.autoindexing-background-scheduler")`.
pub const ROUTINE_NAME: &str = "codeintel.autoindexing-background-scheduler";

/// `goroutine.WithDescription(...)`.
pub const ROUTINE_DESCRIPTION: &str =
    "schedule autoindexing jobs in the background using defined or inferred configurations";

pub const INTERVAL_VAR: &str = "CODEINTEL_AUTOINDEXING_SCHEDULER_INTERVAL";
pub const REPOSITORY_PROCESS_DELAY_VAR: &str =
    "CODEINTEL_AUTOINDEXING_SCHEDULER_REPOSITORY_PROCESS_DELAY";
pub const REPOSITORY_BATCH_SIZE_VAR: &str =
    "CODEINTEL_AUTOINDEXING_SCHEDULER_REPOSITORY_BATCH_SIZE";
pub const POLICY_BATCH_SIZE_VAR: &str = "CODEINTEL_AUTOINDEXING_SCHEDULER_POLICY_BATCH_SIZE";
pub const INFERENCE_CONCURRENCY_VAR: &str = "CODEINTEL_AUTOINDEXING_INFERENCE_CONCURRENCY";
pub const ON_DEMAND_INTERVAL_VAR: &str = "CODEINTEL_AUTOINDEXING_ON_DEMAND_SCHEDULER_INTERVAL";
pub const ON_DEMAND_BATCH_SIZE_VAR: &str = "CODEINTEL_AUTOINDEXING_ON_DEMAND_SCHEDULER_BATCH_SIZE";

/// The baseline's legacy variable names, used when the new name is unset:
/// `env.ChooseFallbackVariableName(first, legacy)`.
pub const LEGACY_INTERVAL_VAR: &str = "PRECISE_CODE_INTEL_AUTO_INDEXING_TASK_INTERVAL";
pub const LEGACY_REPOSITORY_PROCESS_DELAY_VAR: &str =
    "PRECISE_CODE_INTEL_AUTO_INDEXING_REPOSITORY_PROCESS_DELAY";
pub const LEGACY_REPOSITORY_BATCH_SIZE_VAR: &str =
    "PRECISE_CODE_INTEL_AUTO_INDEXING_REPOSITORY_BATCH_SIZE";
pub const LEGACY_POLICY_BATCH_SIZE_VAR: &str = "PRECISE_CODE_INTEL_AUTO_INDEXING_POLICY_BATCH_SIZE";

const INTERVAL_DESCRIPTION: &str = "How frequently to run the auto-indexing scheduling routine.";
const REPOSITORY_PROCESS_DELAY_DESCRIPTION: &str =
    "The minimum frequency that the same repository can be considered for auto-index scheduling.";
const REPOSITORY_BATCH_SIZE_DESCRIPTION: &str =
    "The number of repositories to consider for auto-indexing scheduling at a time.";
const POLICY_BATCH_SIZE_DESCRIPTION: &str =
    "The number of policies to consider for auto-indexing scheduling at a time.";
const INFERENCE_CONCURRENCY_DESCRIPTION: &str =
    "The number of inference jobs running in parallel in the background scheduler.";
const ON_DEMAND_INTERVAL_DESCRIPTION: &str =
    "How frequently to run the on-demand auto-indexing scheduling routine.";
const ON_DEMAND_BATCH_SIZE_DESCRIPTION: &str =
    "The number of repo/rev pairs to consider for on-demand auto-indexing scheduling at a time.";

/// `scheduler.Config`.
#[derive(Debug, Default)]
pub struct SchedulerConfig {
    scheduler_interval: Mutex<Duration>,
    repository_process_delay: Mutex<Duration>,
    repository_batch_size: Mutex<i64>,
    policy_batch_size: Mutex<i64>,
    inference_concurrency: Mutex<i64>,
    on_demand_scheduler_interval: Mutex<Duration>,
    on_demand_batch_size: Mutex<i64>,
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

impl SchedulerConfig {
    getter!(scheduler_interval, scheduler_interval, Duration);
    getter!(repository_process_delay, repository_process_delay, Duration);
    getter!(repository_batch_size, repository_batch_size, i64);
    getter!(policy_batch_size, policy_batch_size, i64);
    getter!(inference_concurrency, inference_concurrency, i64);
    getter!(
        on_demand_scheduler_interval,
        on_demand_scheduler_interval,
        Duration
    );
    getter!(on_demand_batch_size, on_demand_batch_size, i64);
}

impl JobConfig for SchedulerConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        *self.lock_scheduler_interval() = get_interval_named(
            env,
            INTERVAL_VAR,
            LEGACY_INTERVAL_VAR,
            "2m",
            INTERVAL_DESCRIPTION,
            &mut errs,
        );
        *self.lock_repository_process_delay() = get_interval_named(
            env,
            REPOSITORY_PROCESS_DELAY_VAR,
            LEGACY_REPOSITORY_PROCESS_DELAY_VAR,
            "24h",
            REPOSITORY_PROCESS_DELAY_DESCRIPTION,
            &mut errs,
        );
        *self.lock_repository_batch_size() = get_int_named(
            env,
            REPOSITORY_BATCH_SIZE_VAR,
            LEGACY_REPOSITORY_BATCH_SIZE_VAR,
            "2500",
            REPOSITORY_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.lock_policy_batch_size() = get_int_named(
            env,
            POLICY_BATCH_SIZE_VAR,
            LEGACY_POLICY_BATCH_SIZE_VAR,
            "100",
            POLICY_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.lock_inference_concurrency() = get_int(
            env,
            INFERENCE_CONCURRENCY_VAR,
            "16",
            INFERENCE_CONCURRENCY_DESCRIPTION,
            &mut errs,
        );
        *self.lock_on_demand_scheduler_interval() = get_interval(
            env,
            ON_DEMAND_INTERVAL_VAR,
            "30s",
            ON_DEMAND_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        *self.lock_on_demand_batch_size() = get_int(
            env,
            ON_DEMAND_BATCH_SIZE_VAR,
            "100",
            ON_DEMAND_BATCH_SIZE_DESCRIPTION,
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

impl SchedulerConfig {
    fn lock_scheduler_interval(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.scheduler_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_repository_process_delay(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.repository_process_delay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_repository_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.repository_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_policy_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.policy_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_inference_concurrency(&self) -> std::sync::MutexGuard<'_, i64> {
        self.inference_concurrency
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_on_demand_scheduler_interval(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.on_demand_scheduler_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_on_demand_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.on_demand_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The subset of [`SchedulerConfig`] a scheduling pass reads; a plain snapshot
/// so the async code does not hold the config locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerSettings {
    pub repository_process_delay: Duration,
    pub repository_batch_size: i64,
    pub policy_batch_size: i64,
    pub inference_concurrency: i64,
}

impl SchedulerConfig {
    pub fn settings(&self) -> SchedulerSettings {
        SchedulerSettings {
            repository_process_delay: self.repository_process_delay(),
            repository_batch_size: self.repository_batch_size(),
            policy_batch_size: self.policy_batch_size(),
            inference_concurrency: self.inference_concurrency(),
        }
    }
}

/// `BaseConfig.GetInterval` over `env.ChooseFallbackVariableName(first, legacy)`.
fn get_interval_named(
    env: &mut EnvReader,
    name: &str,
    legacy: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> Duration {
    let raw = env.get_with_fallback(name, &[legacy], default, description);
    match sg_core::parse_go_duration(&raw) {
        Ok(duration) => duration,
        Err(message) => {
            errors.push(format!("invalid duration {raw:?} for {name}: {message}"));
            Duration::ZERO
        }
    }
}

/// `BaseConfig.GetInt` over `env.ChooseFallbackVariableName(first, legacy)`.
fn get_int_named(
    env: &mut EnvReader,
    name: &str,
    legacy: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get_with_fallback(name, &[legacy], default, description);
    parse_int(name, raw, errors)
}

/// `BaseConfig.GetInt`.
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    parse_int(name, raw, errors)
}

/// `strconv.ParseInt(raw, 10, 64)` with the baseline's error text.
fn parse_int(name: &str, raw: String, errors: &mut Vec<String>) -> i64 {
    match raw.parse::<i64>() {
        Ok(value) => value,
        Err(err) => {
            let reason = match err.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            errors.push(format!(
                "invalid int {raw:?} for {name}: strconv.ParseInt: parsing {raw:?}: {reason}"
            ));
            0
        }
    }
}

/// `conf.CodeIntelAutoIndexingEnabled`: `codeIntelAutoIndexing.enabled`,
/// `nil` meaning disabled.
pub fn auto_indexing_enabled(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("codeIntelAutoIndexing.enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// `conf.CodeIntelAutoIndexingAllowGlobalPolicies`:
/// `codeIntelAutoIndexing.allowGlobalPolicies`, `nil` meaning false.
pub fn auto_indexing_allow_global_policies(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("codeIntelAutoIndexing.allowGlobalPolicies")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// `IndexEnqueuer.QueueAutoIndexJobs` failures the scheduler distinguishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueAutoIndexJobsError {
    /// `*gitdomain.RevisionNotFoundError`: the commit is skipped.
    RevisionNotFound,
    /// Any other error, wrapped in `indexEnqueuer.QueueAutoIndexJobs`.
    Other(String),
}

/// The scheduler's view of `autoindexing.IndexEnqueuer`.
///
/// The baseline ignores the returned `[]AutoIndexJob` list, so the trait
/// returns `()`. `configuration` is always `""`, `force` and `bypass_limit`
/// are always `false` on this path.
#[async_trait]
pub trait IndexEnqueuer: Send + Sync {
    async fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<(), QueueAutoIndexJobsError>;
}

/// `indexSchedulerJob.handleScheduler`.
pub async fn handle_scheduler(
    frontend: &PgPool,
    site: &SiteConfiguration,
    git: Arc<dyn MatcherGit>,
    enqueuer: Arc<dyn IndexEnqueuer>,
    settings: &SchedulerSettings,
    now: OffsetDateTime,
) -> Result<(), String> {
    if !auto_indexing_enabled(site) {
        return Ok(());
    }

    // `var repositoryMatchLimit *int; if val := conf...; val != -1`
    let repository_match_limit = match site.codeintel_autoindexing_policy_repository_match_limit() {
        -1 => None,
        value => Some(value),
    };

    let repositories = RepoScanStore::precise(frontend.clone())
        .get_repositories_for_index_scan(
            &RepositoryBatchOptions {
                process_delay: settings.repository_process_delay,
                allow_global_policies: auto_indexing_allow_global_policies(site),
                global_policy_repositories_match_limit: repository_match_limit,
                limit: settings.repository_batch_size,
            },
            now,
        )
        .await
        .map_err(|err| format!("uploadSvc.GetRepositoriesForIndexScan: {err}"))?;
    if repositories.is_empty() {
        // All repositories updated recently enough.
        return Ok(());
    }

    let now = OffsetDateTime::now_utc();

    // `semaphore.NewWeighted(InferenceConcurrency)`. Non-positive values
    // deadlock the baseline; clamp so the tick makes progress.
    let concurrency = settings.inference_concurrency.max(1) as usize;
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();

    for repository_id in repositories {
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .map_err(|_| "acquiring semaphore: semaphore closed".to_owned())?;
        let frontend = frontend.clone();
        let git = Arc::clone(&git);
        let enqueuer = Arc::clone(&enqueuer);
        let settings = *settings;
        tasks.spawn(async move {
            let _permit = permit;
            handle_repository(
                &frontend,
                git.as_ref(),
                enqueuer.as_ref(),
                repository_id,
                settings.policy_batch_size,
                now,
            )
            .await
        });
    }

    let mut errs: Vec<String> = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => errs.push(err),
            // The baseline's goroutines cannot panic into a return value.
            Err(err) => errs.push(err.to_string()),
        }
    }

    match combine_errors(&errs) {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// `indexSchedulerJob.handleRepository`.
async fn handle_repository(
    frontend: &PgPool,
    git: &dyn MatcherGit,
    enqueuer: &dyn IndexEnqueuer,
    repository_id: i32,
    policy_batch_size: i64,
    now: OffsetDateTime,
) -> Result<(), String> {
    let repo = RepoStore::new(frontend.clone())
        .get_by_id(repository_id)
        .await
        .map_err(|err| err.to_string())?;
    let repo = match repo {
        Some(repo) => repo,
        None => return Err(format!("repo not found: id={repository_id}")),
    };

    let matcher = PolicyMatcher::new(git, Extractor::Indexing, false, true);
    let mut offset: i64 = 0;

    loop {
        // Retrieve the set of configuration policies that affect indexing
        // for this repository.
        let (policies, total_count) = codeintel_policies::get_configuration_policies(
            frontend,
            &GetConfigurationPoliciesOptions {
                repository_id,
                for_precise_indexing: Some(true),
                limit: policy_batch_size,
                offset,
                ..Default::default()
            },
        )
        .await
        .map_err(|err| format!("policySvc.GetConfigurationPolicies: {err}"))?;
        offset += policies.len() as i64;

        let commit_map = matcher
            .commits_described_by_policy(repository_id, &repo.name, &policies, now, &[])
            .await
            .map_err(|err| format!("policies.CommitsDescribedByPolicy: {err}"))?;

        for (commit, policy_matches) in &commit_map {
            if policy_matches.is_empty() {
                continue;
            }

            match enqueuer
                .queue_auto_index_jobs(repository_id, commit, "", false, false)
                .await
            {
                Ok(()) => {}
                Err(QueueAutoIndexJobsError::RevisionNotFound) => continue,
                Err(QueueAutoIndexJobsError::Other(err)) => {
                    return Err(format!("indexEnqueuer.QueueAutoIndexJobs: {err}"))
                }
            }
        }

        if policies.is_empty() || offset >= total_count {
            return Ok(());
        }
    }
}

/// The periodic handler's `HandlerFunc`.
pub struct SchedulerHandler {
    frontend: PgPool,
    live_config: Arc<LiveConfig>,
    git: Arc<dyn MatcherGit>,
    enqueuer: Arc<dyn IndexEnqueuer>,
    settings: SchedulerSettings,
}

impl SchedulerHandler {
    pub fn new(
        frontend: PgPool,
        live_config: Arc<LiveConfig>,
        git: Arc<dyn MatcherGit>,
        enqueuer: Arc<dyn IndexEnqueuer>,
        settings: SchedulerSettings,
    ) -> Self {
        Self {
            frontend,
            live_config,
            git,
            enqueuer,
            settings,
        }
    }
}

#[async_trait]
impl Handler for SchedulerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        handle_scheduler(
            &self.frontend,
            &self.live_config.current().site,
            Arc::clone(&self.git),
            Arc::clone(&self.enqueuer),
            &self.settings,
            OffsetDateTime::now_utc(),
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `scheduler.NewScheduler`: the periodic routine (the on-demand scheduler is
/// not ported yet).
pub fn new_scheduler_routine(
    frontend: PgPool,
    live_config: Arc<LiveConfig>,
    git: Arc<dyn MatcherGit>,
    enqueuer: Arc<dyn IndexEnqueuer>,
    settings: SchedulerSettings,
    interval: Duration,
) -> BackgroundRoutine {
    PeriodicRoutine::new(Arc::new(SchedulerHandler::new(
        frontend,
        live_config,
        git,
        enqueuer,
        settings,
    )))
    .with_name(ROUTINE_NAME)
    .with_description(ROUTINE_DESCRIPTION)
    .with_interval(interval)
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::PoisonError;

    /// The tests below read process environment variables; serialize them
    /// like the other job config tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_vars() {
        for name in [
            INTERVAL_VAR,
            REPOSITORY_PROCESS_DELAY_VAR,
            REPOSITORY_BATCH_SIZE_VAR,
            POLICY_BATCH_SIZE_VAR,
            INFERENCE_CONCURRENCY_VAR,
            ON_DEMAND_INTERVAL_VAR,
            ON_DEMAND_BATCH_SIZE_VAR,
            LEGACY_INTERVAL_VAR,
            LEGACY_REPOSITORY_PROCESS_DELAY_VAR,
            LEGACY_REPOSITORY_BATCH_SIZE_VAR,
            LEGACY_POLICY_BATCH_SIZE_VAR,
        ] {
            std::env::remove_var(name);
        }
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        clear_vars();
        let config = SchedulerConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.scheduler_interval(), Duration::from_secs(2 * 60));
        assert_eq!(
            config.repository_process_delay(),
            Duration::from_secs(24 * 3600)
        );
        assert_eq!(config.repository_batch_size(), 2500);
        assert_eq!(config.policy_batch_size(), 100);
        assert_eq!(config.inference_concurrency(), 16);
        assert_eq!(
            config.on_demand_scheduler_interval(),
            Duration::from_secs(30)
        );
        assert_eq!(config.on_demand_batch_size(), 100);
    }

    #[test]
    fn legacy_variable_names_are_fallbacks() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        clear_vars();
        std::env::set_var(LEGACY_INTERVAL_VAR, "7m");
        std::env::set_var(LEGACY_POLICY_BATCH_SIZE_VAR, "9");
        let config = SchedulerConfig::default();
        config.load(&mut EnvReader::new());
        assert_eq!(config.scheduler_interval(), Duration::from_secs(7 * 60));
        assert_eq!(config.policy_batch_size(), 9);

        // The new name wins when both are set.
        std::env::set_var(INTERVAL_VAR, "3m");
        let config = SchedulerConfig::default();
        config.load(&mut EnvReader::new());
        assert_eq!(config.scheduler_interval(), Duration::from_secs(3 * 60));
        clear_vars();
    }

    #[test]
    fn invalid_configuration_is_reported() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        clear_vars();
        std::env::set_var(INFERENCE_CONCURRENCY_VAR, "many");
        let config = SchedulerConfig::default();
        config.load(&mut EnvReader::new());
        clear_vars();
        let err = config.validate().expect_err("invalid int");
        assert_eq!(
            err.to_string(),
            "invalid int \"many\" for CODEINTEL_AUTOINDEXING_INFERENCE_CONCURRENCY: strconv.ParseInt: parsing \"many\": invalid syntax"
        );
    }

    #[test]
    fn gates_default_to_disabled() {
        let site = SiteConfiguration::parse("{}").expect("valid site config");
        assert!(!auto_indexing_enabled(&site));
        assert!(!auto_indexing_allow_global_policies(&site));
        assert_eq!(
            site.codeintel_autoindexing_policy_repository_match_limit(),
            -1
        );

        let site = SiteConfiguration::parse(
            r#"{
                "codeIntelAutoIndexing.enabled": true,
                "codeIntelAutoIndexing.allowGlobalPolicies": true,
                "codeIntelAutoIndexing.policyRepositoryMatchLimit": 12
            }"#,
        )
        .expect("valid site config");
        assert!(auto_indexing_enabled(&site));
        assert!(auto_indexing_allow_global_policies(&site));
        assert_eq!(
            site.codeintel_autoindexing_policy_repository_match_limit(),
            12
        );
    }

    #[test]
    fn match_limit_below_minus_one_is_unlimited() {
        let site =
            SiteConfiguration::parse(r#"{"codeIntelAutoIndexing.policyRepositoryMatchLimit": -2}"#)
                .expect("valid site config");
        assert_eq!(
            site.codeintel_autoindexing_policy_repository_match_limit(),
            -1
        );
    }
}
