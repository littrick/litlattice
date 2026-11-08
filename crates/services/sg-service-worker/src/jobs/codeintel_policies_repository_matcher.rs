#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/policies.go`: the
//! `codeintel-policies-repository-matcher` job.
//!
//! The job owns `policies.RepositoryMatcherConfigInst`
//! (`repository_matcher.Config`) and returns the single routine built by
//! `policies.NewRepositoryMatcherRoutines` →
//! `background.PolicyMatcherJobs` → `repomatcher.NewRepositoryMatcher`:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.policies-matcher` | `CODEINTEL_POLICIES_REPO_MATCHER_INTERVAL` (1m) | recompute the repository membership of a batch of auto-indexing / retention policies |
//!
//! Each invocation (`handleRepositoryMatcherBatch`):
//!
//! 1. `SelectPoliciesForRepositoryMembershipUpdate(batchSize)` takes the
//!    `batchSize` least recently resolved policies (`last_resolved_at NULLS
//!    FIRST, id`), locks them and stamps `last_resolved_at = now`;
//! 2. for every policy:
//!    - `conf.CodeIntelAutoIndexingPolicyRepositoryMatchLimit()` (site
//!      setting `codeIntelAutoIndexing.policyRepositoryMatchLimit`, read once
//!      per policy like the baseline) becomes the optional `LIMIT` unless its
//!      value is `-1` (unlimited);
//!    - `UpdateReposMatchingPatterns` rewrites
//!      `lsif_configuration_policies_repository_pattern_lookup` for that
//!      policy: `lower(name) LIKE pattern` (every `*` becomes `%`, no
//!      escaping), deleted/blocked repositories excluded, ordered by
//!      `stars DESC NULLS LAST, id`; missing matches are inserted and stale
//!      ones deleted in one statement. Empty/NULL patterns match no
//!      repositories, so they clear the policy's rows.
//!
//! Not ported: the `src_codeintel_background_policies_updated_total` counter
//! (metrics slice), the RED metrics/spans, and the `actor.WithInternalActor`
//! context (the SQL has no authz condition, so the Rust port needs none).
//! The baseline's `codeintel.InitServices` also opens the code-intel database;
//! this job never reads it, so the Rust job does not require that DSN.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_policies;
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `policiesRepositoryMatcherJob.Description`.
pub const JOB_DESCRIPTION: &str = "code-intel policies repository matcher";

/// `repomatcher.NewRepositoryMatcher`'s routine metadata.
pub const ROUTINE_NAME: &str = "codeintel.policies-matcher";
pub const ROUTINE_DESCRIPTION: &str = "match repositories to autoindexing+retention policies";

/// `Config.Interval` / `Config.ConfigurationPolicyMembershipBatchSize`.
pub const INTERVAL_VAR: &str = "CODEINTEL_POLICIES_REPO_MATCHER_INTERVAL";
const INTERVAL_DESCRIPTION: &str = "How frequently to run the policies repository matcher routine.";
/// `env.ChooseFallbackVariableName` candidates, in priority order.
pub const BATCH_SIZE_VARS: [&str; 2] = [
    "CODEINTEL_POLICIES_REPO_MATCHER_CONFIGURATION_POLICY_MEMBERSHIP_BATCH_SIZE",
    "PRECISE_CODE_INTEL_CONFIGURATION_POLICY_MEMBERSHIP_BATCH_SIZE",
];
const BATCH_SIZE_DESCRIPTION: &str =
    "The maximum number of policy configurations to update repository membership for at a time.";

/// `repomatcher.Config` (`policies.RepositoryMatcherConfigInst`).
#[derive(Debug, Default)]
pub struct RepositoryMatcherConfig {
    interval: Mutex<Duration>,
    batch_size: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl RepositoryMatcherConfig {
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

impl JobConfig for RepositoryMatcherConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        let batch_size_name = choose_fallback(&BATCH_SIZE_VARS);
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            get_interval(env, INTERVAL_VAR, "1m", INTERVAL_DESCRIPTION, &mut errs);
        *self
            .batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_int(
            env,
            &batch_size_name,
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

/// `env.ChooseFallbackVariableName`: the first name whose environment
/// variable is non-empty; otherwise the first name.
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

/// `cmd/worker/internal/codeintel.NewPoliciesRepositoryMatcherJob`.
#[derive(Debug, Default)]
pub struct PoliciesRepositoryMatcher {
    config: Arc<RepositoryMatcherConfig>,
}

impl PoliciesRepositoryMatcher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(&self) -> &RepositoryMatcherConfig {
        &self.config
    }
}

#[async_trait]
impl Job for PoliciesRepositoryMatcher {
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
        let deps = Arc::new(MatcherDeps {
            frontend: deps.frontend.clone(),
            live_config: deps.live_config.clone(),
            batch_size: self.config.batch_size(),
        });

        let routine = PeriodicRoutine::new(Arc::new(MatcherHandler { deps }))
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(self.config.interval());

        Ok(vec![routine.into()])
    }
}

struct MatcherDeps {
    frontend: PgPool,
    live_config: Arc<LiveConfig>,
    batch_size: i64,
}

/// `repoMatcher`'s handler.
struct MatcherHandler {
    deps: Arc<MatcherDeps>,
}

#[async_trait]
impl Handler for MatcherHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        apply_matcher_batch(
            &self.deps.frontend,
            &self.deps.live_config,
            self.deps.batch_size,
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `repoMatcher.handleRepositoryMatcherBatch`.
pub async fn apply_matcher_batch(
    pool: &PgPool,
    live_config: &LiveConfig,
    batch_size: i64,
) -> Result<(), String> {
    let now = OffsetDateTime::now_utc();
    let policies =
        codeintel_policies::select_policies_for_repository_membership_update(pool, batch_size, now)
            .await?;

    for policy in &policies {
        let patterns = policy.repository_patterns.clone().unwrap_or_default();

        // `conf.CodeIntelAutoIndexingPolicyRepositoryMatchLimit()` is read
        // inside the loop, so a configuration change applies to the remaining
        // policies of the batch.
        let limit = live_config
            .current()
            .site
            .codeintel_autoindexing_policy_repository_match_limit();
        let repository_match_limit = if limit != -1 { Some(limit) } else { None };

        // Always call this even if patterns are not supplied: the policy may
        // have lookup rows left over from a previous pattern set.
        codeintel_policies::update_repos_matching_patterns(
            pool,
            &patterns,
            policy.id,
            repository_match_limit,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::PoisonError;

    /// `invalid_configuration_is_reported` sets a process environment variable
    /// `config_defaults_match_the_baseline` reads; cargo runs the two tests in
    /// parallel threads, so they must not overlap.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = PoliciesRepositoryMatcher::new();
        assert_eq!(job.description(), "code-intel policies repository matcher");
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = RepositoryMatcherConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.interval(), Duration::from_secs(60));
        assert_eq!(config.batch_size(), 100);
    }

    #[test]
    fn invalid_configuration_is_reported() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = RepositoryMatcherConfig::default();
        std::env::set_var(INTERVAL_VAR, "soon");
        config.load(&mut EnvReader::new());
        std::env::remove_var(INTERVAL_VAR);
        let err = config.validate().expect_err("invalid interval");
        assert_eq!(
            err.to_string(),
            "invalid duration \"soon\" for CODEINTEL_POLICIES_REPO_MATCHER_INTERVAL: time: invalid duration \"soon\""
        );
    }
}
