#![forbid(unsafe_code)]

//! `internal/codeintel/syntactic_indexing/{scheduler.go,scheduler_job.go,
//! scheduler_config.go,enqueuer.go}` and `internal/codeintel/reposcheduler`:
//! the `codeintel-syntactic-indexing-scheduler` job.
//!
//! Every `CODEINTEL_SYNTACTIC_INDEXING_SCHEDULER_INTERVAL` (2m) the handler
//! checks `experimentalFeatures.codeintelSyntacticIndexingEnabled`; when the
//! flag is off the tick is a no-op. Otherwise [`schedule`]:
//!
//! 1. `GetRepositoriesForIndexScan` (`syntactic_scip_last_index_scan`) claims
//!    the repositories matched by a syntactic-indexing policy (global,
//!    repository-attached, or via the pattern lookup);
//! 2. for every claimed repository, `PolicyIterator` pages through
//!    `GetConfigurationPolicies(RepositoryID, ForSyntacticIndexing=true,
//!    ForPreciseIndexing=false)` and collects the commits the policy matcher
//!    describes ([`PolicyMatcher`] over [`Extractor::Indexing`],
//!    `includeTipOfDefaultBranch=true`, `filterByCreatedDate=true`);
//! 3. for every `(repository, commit)`, the enqueuer skips pairs that are
//!    already queued (`IsQueued`) and otherwise inserts a `queued` row with
//!    `enqueuer_user_id = 0` (the internal actor's UID).
//!
//! The whole scheduler runs as the internal actor, so the jobstore's authz
//! condition is `TRUE`.
//!
//! Baseline quirks reproduced here (see the M9.8ai evidence for the errata):
//!
//! * `PolicyIterator` resets `options.Offset = 0` on every iteration and
//!   stops once the accumulated batch length reaches `totalCount`, so with
//!   more policies than `PolicyBatchSize` the first page is processed more
//!   than once. The duplicates collapse in the commit set;
//! * `RepoStore.Get`'s error is ignored and `repo.Name` is used regardless
//!   (a failed lookup therefore leaves the zero value: id 0, empty name);
//! * Go iterates the `commitsToSchedule` map in random order, so the enqueue
//!   order is non-deterministic; this port iterates a `BTreeMap`/`BTreeSet`
//!   and is deterministic.
//!
//! Not ported: the RED metrics/spans, the skipped-jobs counter, and the log
//! lines. The code-intel database connection `BootstrapSyntacticJobScheduler`
//! initialises is unused on this path (policies, jobs and the scheduling
//! tables all live in the frontend database), so the job only uses
//! `deps.frontend`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_policies::{self, GetConfigurationPoliciesOptions};
use sg_db::repos::RepoStore;
use sg_db::reposcheduler::{RepoScanStore, RepositoryBatchOptions};
use sg_db::syntactic_indexing_jobs::{self, NewSyntacticIndexingJob, SyntacticIndexingJob, QUEUED};
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::policies_matcher::{Extractor, GitserverGit, MatcherGit, PolicyMatcher};
use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `syntacticIndexingSchedulerJob.Description`.
pub const JOB_DESCRIPTION: &str = "Scheduler job for codeintel syntactic indexing";

/// `goroutine.WithName("codeintel.syntactic-indexing-background-scheduler")`.
pub const ROUTINE_NAME: &str = "codeintel.syntactic-indexing-background-scheduler";

/// `goroutine.WithDescription(...)`.
pub const ROUTINE_DESCRIPTION: &str = "schedule syntactic indexing jobs in the background";

pub const INTERVAL_VAR: &str = "CODEINTEL_SYNTACTIC_INDEXING_SCHEDULER_INTERVAL";
pub const REPOSITORY_PROCESS_DELAY_VAR: &str =
    "CODEINTEL_SYNTACTIC_INDEXING_SCHEDULER_REPOSITORY_PROCESS_DELAY";
pub const REPOSITORY_BATCH_SIZE_VAR: &str =
    "CODEINTEL_SYNTACTIC_INDEXING_SCHEDULER_REPOSITORY_BATCH_SIZE";
pub const POLICY_BATCH_SIZE_VAR: &str = "CODEINTEL_SYNTACTIC_INDEXING_SCHEDULER_POLICY_BATCH_SIZE";

const INTERVAL_DESCRIPTION: &str = "How frequently to run syntactic indexing scheduling routine.";
const REPOSITORY_PROCESS_DELAY_DESCRIPTION: &str =
    "The minimum frequency that the same repository can be considered for syntactic index scheduling.";
const REPOSITORY_BATCH_SIZE_DESCRIPTION: &str =
    "The number of repositories to consider for syntactic indexing scheduling at a time.";
const POLICY_BATCH_SIZE_DESCRIPTION: &str =
    "The number of policies to consider for syntactic indexing scheduling at a time.";

/// `syntactic_indexing.SchedulerConfig`.
#[derive(Debug, Default)]
pub struct SchedulerConfig {
    scheduler_interval: Mutex<Duration>,
    repository_process_delay: Mutex<Duration>,
    repository_batch_size: Mutex<i64>,
    policy_batch_size: Mutex<i64>,
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
}

impl JobConfig for SchedulerConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        *self.lock_scheduler_interval() =
            get_interval(env, INTERVAL_VAR, "2m", INTERVAL_DESCRIPTION, &mut errs);
        *self.lock_repository_process_delay() = get_interval(
            env,
            REPOSITORY_PROCESS_DELAY_VAR,
            "24h",
            REPOSITORY_PROCESS_DELAY_DESCRIPTION,
            &mut errs,
        );
        *self.lock_repository_batch_size() = get_int(
            env,
            REPOSITORY_BATCH_SIZE_VAR,
            "2500",
            REPOSITORY_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.lock_policy_batch_size() = get_int(
            env,
            POLICY_BATCH_SIZE_VAR,
            "100",
            POLICY_BATCH_SIZE_DESCRIPTION,
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
}

/// The subset of [`SchedulerConfig`] a scheduling pass reads; a plain snapshot
/// so the async code does not hold the config locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerSettings {
    pub repository_process_delay: Duration,
    pub repository_batch_size: i64,
    pub policy_batch_size: i64,
}

impl SchedulerConfig {
    pub fn settings(&self) -> SchedulerSettings {
        SchedulerSettings {
            repository_process_delay: self.repository_process_delay(),
            repository_batch_size: self.repository_batch_size(),
            policy_batch_size: self.policy_batch_size(),
        }
    }
}

/// `BaseConfig.GetInt`: `strconv.ParseInt(raw, 10, 64)`, record
/// `invalid int %q for %s: %s` and return 0 on failure.
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

/// `EnqueueOptions{force: false}`: the enqueuer skips an already-queued pair.
pub async fn queue_indexing_job(
    pool: &PgPool,
    repository_id: i32,
    commit: &str,
) -> Result<Vec<SyntacticIndexingJob>, String> {
    if syntactic_indexing_jobs::is_queued(pool, repository_id, commit)
        .await
        .map_err(|err| format!("dbstore.IsQueued: {err}"))?
    {
        // `indexingJobsSkippedCounter` is a metric; only the insert is state.
        return Ok(Vec::new());
    }

    syntactic_indexing_jobs::insert_indexing_jobs(
        pool,
        &[NewSyntacticIndexingJob {
            state: QUEUED.to_owned(),
            commit: commit.to_owned(),
            repository_id,
            // `actor.FromContext(ctx).UID` for the internal actor.
            enqueuer_user_id: 0,
        }],
    )
    .await
}

/// `syntacticJobScheduler.Schedule`: claims repositories, collects the
/// commits described by a syntactic-indexing policy, and enqueues them.
pub async fn schedule(
    frontend: &PgPool,
    matcher_git: &dyn MatcherGit,
    settings: &SchedulerSettings,
    now: OffsetDateTime,
) -> Result<(), String> {
    // `reposcheduler.NewBatchOptions(RepositoryProcessDelay, true,
    // &PolicyBatchSize, RepositoryBatchSize)`: the global-policy match limit
    // is the *policy* batch size, not the repository batch size.
    let repos = RepoScanStore::syntactic(frontend.clone())
        .get_repositories_for_index_scan(
            &RepositoryBatchOptions {
                process_delay: settings.repository_process_delay,
                allow_global_policies: true,
                global_policy_repositories_match_limit: Some(settings.policy_batch_size),
                limit: settings.repository_batch_size,
            },
            now,
        )
        .await
        .map_err(|err| format!("reposcheduler.GetRepositoriesForIndexScan: {err}"))?;

    let repo_store = RepoStore::new(frontend.clone());
    let matcher = PolicyMatcher::new(matcher_git, Extractor::Indexing, true, true);

    let mut commits_to_schedule: BTreeMap<i32, BTreeSet<String>> = BTreeMap::new();
    let mut errs: Vec<String> = Vec::new();

    for repo_id in repos {
        // `repo, _ := s.RepoStore.Get(ctx, api.RepoID(repoToIndex.ID))`: the
        // error is ignored in the baseline; a missing row leaves the zero
        // value, so the matcher receives an empty repo name.
        let repo = repo_store.get_by_id(repo_id).await.ok().flatten();
        let (schedule_id, repo_name) = match repo {
            Some(repo) => (repo.id, repo.name),
            None => (0, String::new()),
        };

        let result = for_each_policies_batch(
            frontend,
            &matcher,
            repo_id,
            &repo_name,
            settings.policy_batch_size,
            now,
            &mut commits_to_schedule,
            schedule_id,
        )
        .await;
        if let Err(err) = result {
            errs.push(format!(
                "Failed to discover commits eligible for syntactic indexing for repo [{repo_name}]: {err}"
            ));
        }
    }

    for (repo_id, commits) in &commits_to_schedule {
        for commit in commits {
            if let Err(err) = queue_indexing_job(frontend, *repo_id, commit).await {
                errs.push(format!(
                    "Failed to schedule syntactic indexing of repo [ID={repo_id}], commit [{commit}]: {err}"
                ));
            }
        }
    }

    match combine_errors(&errs) {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// `PolicyIterator.ForEachPoliciesBatch` for `SyntacticIndexing`.
///
/// Note the deliberate `offset: 0`: the baseline resets it at the top of
/// every iteration and only uses the accumulated length to decide when to
/// stop.
#[allow(clippy::too_many_arguments)]
async fn for_each_policies_batch<G: MatcherGit + ?Sized>(
    frontend: &PgPool,
    matcher: &PolicyMatcher<'_, G>,
    repository_id: i32,
    repo_name: &str,
    batch_size: i64,
    now: OffsetDateTime,
    commits_to_schedule: &mut BTreeMap<i32, BTreeSet<String>>,
    schedule_id: i32,
) -> Result<(), String> {
    let mut offset: i64 = 0;
    loop {
        let (policies_batch, total_count) = codeintel_policies::get_configuration_policies(
            frontend,
            &GetConfigurationPoliciesOptions {
                repository_id,
                for_syntactic_indexing: Some(true),
                for_precise_indexing: Some(false),
                limit: batch_size,
                offset: 0,
                ..Default::default()
            },
        )
        .await?;

        if policies_batch.is_empty() {
            break;
        }

        let commit_map = matcher
            .commits_described_by_policy(repository_id, repo_name, &policies_batch, now, &[])
            .await?;
        for (commit, policy_matches) in commit_map {
            if policy_matches.is_empty() {
                continue;
            }
            commits_to_schedule
                .entry(schedule_id)
                .or_default()
                .insert(commit);
        }

        offset += policies_batch.len() as i64;
        if offset >= total_count {
            break;
        }
    }
    Ok(())
}

/// The periodic handler's `HandlerFunc`: gate on the experimental flag.
pub async fn run_scheduler_tick(
    frontend: &PgPool,
    site: &SiteConfiguration,
    matcher_git: &dyn MatcherGit,
    settings: &SchedulerSettings,
    now: OffsetDateTime,
) -> Result<(), String> {
    if !site.codeintel_syntactic_indexing_enabled() {
        return Ok(());
    }
    schedule(frontend, matcher_git, settings, now).await
}

/// The `goroutine.HandlerFunc` closure.
pub struct SchedulerHandler {
    frontend: PgPool,
    live_config: Arc<LiveConfig>,
    git: Arc<GitserverGit>,
    settings: SchedulerSettings,
}

impl SchedulerHandler {
    pub fn new(
        frontend: PgPool,
        live_config: Arc<LiveConfig>,
        git: Arc<GitserverGit>,
        settings: SchedulerSettings,
    ) -> Self {
        Self {
            frontend,
            live_config,
            git,
            settings,
        }
    }
}

#[async_trait]
impl Handler for SchedulerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        run_scheduler_tick(
            &self.frontend,
            &self.live_config.current().site,
            self.git.as_ref(),
            &self.settings,
            OffsetDateTime::now_utc(),
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `syntactic_indexing.NewSyntacticindexingSchedulerJob`.
#[derive(Debug, Default)]
pub struct SyntacticIndexingScheduler {
    config: Arc<SchedulerConfig>,
}

impl SyntacticIndexingScheduler {
    pub fn new() -> Self {
        Self {
            config: Arc::new(SchedulerConfig::default()),
        }
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }
}

#[async_trait]
impl Job for SyntacticIndexingScheduler {
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
        let unified = deps.live_config.current();
        let git = Arc::new(GitserverGit::new(Arc::new(sg_git::GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ))));

        let routine = PeriodicRoutine::new(Arc::new(SchedulerHandler::new(
            deps.frontend.clone(),
            deps.live_config.clone(),
            git,
            self.config.settings(),
        )))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(self.config.scheduler_interval());

        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::PoisonError;

    /// The tests below read process environment variables; serialize them
    /// like the other job config tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = SyntacticIndexingScheduler::new();
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert_eq!(job.configs().len(), 1);
        assert_eq!(
            ROUTINE_NAME,
            "codeintel.syntactic-indexing-background-scheduler"
        );
        assert_eq!(
            ROUTINE_DESCRIPTION,
            "schedule syntactic indexing jobs in the background"
        );
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
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
    }

    #[test]
    fn invalid_configuration_is_reported() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = SchedulerConfig::default();
        std::env::set_var(REPOSITORY_BATCH_SIZE_VAR, "many");
        config.load(&mut EnvReader::new());
        std::env::remove_var(REPOSITORY_BATCH_SIZE_VAR);
        let err = config.validate().expect_err("invalid int");
        assert_eq!(
            err.to_string(),
            "invalid int \"many\" for CODEINTEL_SYNTACTIC_INDEXING_SCHEDULER_REPOSITORY_BATCH_SIZE: strconv.ParseInt: parsing \"many\": invalid syntax"
        );
    }

    #[test]
    fn gate_defaults_to_disabled() {
        let site = SiteConfiguration::parse("{}").expect("valid site config");
        assert!(!site.codeintel_syntactic_indexing_enabled());

        let site = SiteConfiguration::parse(
            r#"{"experimentalFeatures": {"codeintelSyntacticIndexingEnabled": true}}"#,
        )
        .expect("valid site config");
        assert!(site.codeintel_syntactic_indexing_enabled());
    }
}
