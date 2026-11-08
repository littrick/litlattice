#![forbid(unsafe_code)]

//! `cmd/worker/internal/embeddings/repo/scheduler.go`: the
//! `repo-embedding-scheduler` job.
//!
//! Every 15 minutes the handler checks `conf.EmbeddingsEnabled()`, asks the
//! `repo_embedding_jobs` store for repos matched by an embeddings policy that
//! have not been queued within the configured interval
//! (`repo.GetEmbeddableRepoOpts`), and then, for every matched repo without a
//! job at the repo's current default-branch revision, enqueues a job:
//!
//! - list the minimal repos by id (`ListMinimalRepos`, internal actor, so
//!   authz is bypassed);
//! - `gitserver.GetDefaultBranch(repo, false)`; any error or an empty ref
//!   means the job is enqueued with an empty revision and the handler decides
//!   later whether it can run;
//! - skip the repo when a job already exists for `(repo_id, revision)`;
//! - otherwise `INSERT INTO repo_embedding_jobs (repo_id, revision)`.
//!
//! The whole scheduling pass runs in one transaction, like
//! `repoEmbeddingJobsStore.Transact`.
//!
//! `conf.GetEmbeddingsConfig` is evaluated per tick from the live site
//! configuration. Only the fields the scheduler reads (`MinimumInterval`,
//! `PolicyRepositoryMatchLimit`) are carried, but the nil/not-nil decision
//! follows the baseline exactly (Cody enabled, `SRC_INTERNAL_EMBEDDINGS_ALLOWED`,
//! the legacy `embeddings.enabled` flag, and per-provider credential checks).
//!
//! Not ported: the RED metrics/observation operations and log lines.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_licensing::license_key_based_access_token;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::env::parse_go_bool;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `repoEmbeddingSchedulerJob.Description`.
pub const JOB_DESCRIPTION: &str = "resolves policies and schedules repos for embedding";

/// `goroutine.WithName("repoEmbeddingSchedulerJob")`.
pub const ROUTINE_NAME: &str = "repoEmbeddingSchedulerJob";

/// `goroutine.WithDescription(...)`.
pub const ROUTINE_DESCRIPTION: &str =
    "resolves embedding policies and schedules jobs to embed repos";

/// `goroutine.WithInterval(15 * time.Minute)`.
pub const INTERVAL: Duration = Duration::from_secs(15 * 60);

/// `SRC_INTERNAL_EMBEDDINGS_ALLOWED`: read once at startup like the
/// baseline's package variable; `strconv.ParseBool` errors mean false.
pub const FORCE_ALLOW_EMBEDDINGS_ENV: &str = "SRC_INTERNAL_EMBEDDINGS_ALLOWED";

const DEFAULT_MINIMUM_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_POLICY_REPOSITORY_MATCH_LIMIT: i64 = 5000;

/// The subset of `conftypes.EmbeddingsConfig` the scheduler reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingsConfig {
    pub minimum_interval: Duration,
    /// `None` or a negative value means unlimited.
    pub policy_repository_match_limit: Option<i64>,
}

/// `conf.ForceAllowEmbeddings`.
pub fn force_allow_embeddings() -> bool {
    static ALLOW: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ALLOW.get_or_init(|| {
        std::env::var(FORCE_ALLOW_EMBEDDINGS_ENV)
            .ok()
            .and_then(|raw| parse_go_bool(&raw).ok())
            .unwrap_or(false)
    })
}

/// `conf.codyEnabled`.
fn cody_enabled(site: &SiteConfiguration) -> bool {
    let raw = site.raw();
    let enabled = raw.get("cody.enabled").and_then(serde_json::Value::as_bool);
    let completions = raw.get("completions").filter(|value| !value.is_null());
    match (enabled, completions) {
        (Some(false), _) => false,
        (Some(true), _) => true,
        // `enabled == nil && completions != nil`: the deprecated
        // `completions.enabled` flag decides (unset means false).
        (None, Some(completions)) => {
            completions
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        }
        (None, None) => false,
    }
}

fn string_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `conf.GetEmbeddingsConfig`: `None` when the baseline returns a nil config.
///
/// `force_allow` is `conf.ForceAllowEmbeddings()`; it is a parameter so tests
/// can exercise both branches without touching process state.
pub fn embeddings_config(site: &SiteConfiguration, force_allow: bool) -> Option<EmbeddingsConfig> {
    if !cody_enabled(site) {
        return None;
    }
    if !force_allow {
        return None;
    }

    let raw = site.raw();
    let embeddings = raw.get("embeddings").filter(|value| !value.is_null());

    // Legacy flag: explicitly disabled embeddings are off.
    if let Some(embeddings) = embeddings {
        if embeddings
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
        {
            return None;
        }
    }

    let mut provider = embeddings
        .map(|value| string_field(value, "provider"))
        .unwrap_or_default();
    let access_token = embeddings
        .map(|value| string_field(value, "accessToken"))
        .unwrap_or_default();

    if provider.is_empty() {
        // An explicit token with no provider would send it to the wrong
        // service; the baseline bails instead of guessing.
        if !access_token.is_empty() {
            return None;
        }
        provider = "sourcegraph".to_owned();
    }

    match provider.as_str() {
        "sourcegraph" => {
            // An explicit token wins, otherwise derive one from the license
            // key; without either, Cody Gateway cannot be authenticated.
            let _token = if !access_token.is_empty() {
                access_token
            } else {
                let license_key = site.license_key();
                if license_key.is_empty() {
                    return None;
                }
                license_key_based_access_token(&license_key)
            };
        }
        "openai" => {
            if access_token.is_empty() {
                return None;
            }
        }
        "azure-openai" => {
            let endpoint = embeddings
                .map(|value| string_field(value, "endpoint"))
                .unwrap_or_default();
            let model = embeddings
                .map(|value| string_field(value, "model"))
                .unwrap_or_default();
            if endpoint.is_empty() || model.is_empty() {
                return None;
            }
        }
        _ => return None,
    }

    let minimum_interval = embeddings
        .map(|value| string_field(value, "minimumInterval"))
        .filter(|value| !value.is_empty())
        .and_then(|value| sg_core::parse_go_duration(&value).ok())
        .unwrap_or(DEFAULT_MINIMUM_INTERVAL);

    let policy_repository_match_limit = embeddings
        .and_then(|value| value.get("policyRepositoryMatchLimit"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(DEFAULT_POLICY_REPOSITORY_MATCH_LIMIT);

    Some(EmbeddingsConfig {
        minimum_interval,
        policy_repository_match_limit: Some(policy_repository_match_limit),
    })
}

/// `gitserver.Client.GetDefaultBranch(repo, false)`, as the scheduler uses it.
#[async_trait]
pub trait DefaultBranchProvider: Send + Sync {
    async fn get_default_branch(&self, repo: &str) -> Result<(String, String), String>;
}

#[async_trait]
impl DefaultBranchProvider for sg_git::GitserverClient {
    async fn get_default_branch(&self, repo: &str) -> Result<(String, String), String> {
        self.default_branch_commit(repo)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `embeddings.ScheduleRepositoriesForPolicy`.
pub async fn schedule_repositories_for_policy(
    pool: &PgPool,
    repo_ids: &[i32],
    git: &dyn DefaultBranchProvider,
) -> Result<(), String> {
    if repo_ids.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    // The baseline lists the repos through `db.Repos()` (not the transaction).
    let repos = sg_db::repo_embeddings::list_minimal_repos(pool, repo_ids)
        .await
        .map_err(|err| err.to_string())?;

    for repo in repos {
        let revision = match git.get_default_branch(&repo.name).await {
            Ok((ref_name, revision)) if !ref_name.is_empty() => revision,
            // Enqueue with an empty revision and let the handler decide
            // whether the job can execute.
            _ => String::new(),
        };

        // Skip repos that already have a job for this revision; failing jobs
        // therefore are not retried endlessly at the same revision.
        let existing = sg_db::repo_embeddings::get_last_repo_embedding_job_for_revision(
            &mut *tx, repo.id, &revision,
        )
        .await
        .map_err(|err| err.to_string())?;
        if existing.is_some() {
            continue;
        }

        sg_db::repo_embeddings::create_repo_embedding_job(&mut *tx, repo.id, &revision)
            .await
            .map_err(|err| err.to_string())?;
    }

    tx.commit().await.map_err(|err| err.to_string())
}

/// The periodic handler's `HandlerFunc`: gate, find embeddable repos, schedule.
pub async fn run_scheduler_tick(
    pool: &PgPool,
    site: &SiteConfiguration,
    git: &dyn DefaultBranchProvider,
) -> Result<(), String> {
    let Some(config) = embeddings_config(site, force_allow_embeddings()) else {
        return Ok(());
    };

    let embeddable = sg_db::repo_embeddings::get_embeddable_repos(
        pool,
        sg_db::repo_embeddings::EmbeddableRepoOpts {
            minimum_interval: config.minimum_interval,
            policy_repository_match_limit: config.policy_repository_match_limit,
        },
    )
    .await
    .map_err(|err| err.to_string())?;
    if embeddable.is_empty() {
        return Ok(());
    }

    let repo_ids: Vec<i32> = embeddable.into_iter().map(|repo| repo.id).collect();
    schedule_repositories_for_policy(pool, &repo_ids, git).await
}

/// The `goroutine.HandlerFunc` closure.
pub struct SchedulerHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    git: Arc<dyn DefaultBranchProvider>,
}

impl SchedulerHandler {
    pub fn new(
        pool: PgPool,
        live_config: Arc<LiveConfig>,
        git: Arc<dyn DefaultBranchProvider>,
    ) -> Self {
        Self {
            pool,
            live_config,
            git,
        }
    }
}

#[async_trait]
impl Handler for SchedulerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        run_scheduler_tick(
            &self.pool,
            &self.live_config.current().site,
            self.git.as_ref(),
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `cmd/worker/internal/embeddings/repo.NewRepoEmbeddingSchedulerJob`.
#[derive(Debug, Default)]
pub struct RepoEmbeddingScheduler;

impl RepoEmbeddingScheduler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for RepoEmbeddingScheduler {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let unified = deps.live_config.current();
        let git = Arc::new(sg_git::GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));

        let routine = PeriodicRoutine::new(Arc::new(SchedulerHandler::new(
            deps.frontend.clone(),
            deps.live_config.clone(),
            git,
        )))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(INTERVAL);

        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn embeddings_require_cody_and_the_internal_flag() {
        let site = parse_site(r#"{"cody.enabled": true, "licenseKey": "key"}"#);
        assert_eq!(embeddings_config(&site, false), None);
        assert!(embeddings_config(&site, true).is_some());

        let site = parse_site(r#"{"cody.enabled": false, "licenseKey": "key"}"#);
        assert_eq!(embeddings_config(&site, true), None);

        // Legacy `completions.enabled` when `cody.enabled` is unset.
        let site = parse_site(r#"{"completions": {"enabled": true}, "licenseKey": "key"}"#);
        assert!(embeddings_config(&site, true).is_some());
        let site = parse_site(r#"{"completions": {"enabled": false}, "licenseKey": "key"}"#);
        assert_eq!(embeddings_config(&site, true), None);
    }

    #[test]
    fn provider_credentials_gate_the_config() {
        // No license key and no token: the sourcegraph provider cannot auth.
        let site = parse_site(r#"{"cody.enabled": true}"#);
        assert_eq!(embeddings_config(&site, true), None);

        // Explicit token with no provider is refused.
        let site = parse_site(r#"{"cody.enabled": true, "embeddings": {"accessToken": "t"}}"#);
        assert_eq!(embeddings_config(&site, true), None);

        // openai requires a token.
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "openai", "accessToken": "t"}}"#,
        );
        assert!(embeddings_config(&site, true).is_some());
        let site = parse_site(r#"{"cody.enabled": true, "embeddings": {"provider": "openai"}}"#);
        assert_eq!(embeddings_config(&site, true), None);

        // azure-openai requires endpoint and model.
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "azure-openai", "endpoint": "https://x", "model": "m"}}"#,
        );
        assert!(embeddings_config(&site, true).is_some());
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "azure-openai", "endpoint": "https://x"}}"#,
        );
        assert_eq!(embeddings_config(&site, true), None);

        // Legacy `enabled: false` disables.
        let site = parse_site(
            r#"{"cody.enabled": true, "licenseKey": "k", "embeddings": {"enabled": false}}"#,
        );
        assert_eq!(embeddings_config(&site, true), None);
    }

    #[test]
    fn interval_and_limit_defaults_match_the_baseline() {
        let site = parse_site(r#"{"cody.enabled": true, "licenseKey": "key"}"#);
        assert_eq!(
            embeddings_config(&site, true),
            Some(EmbeddingsConfig {
                minimum_interval: Duration::from_secs(24 * 60 * 60),
                policy_repository_match_limit: Some(5000),
            })
        );

        let site = parse_site(
            r#"{"cody.enabled": true, "licenseKey": "key", "embeddings": {"minimumInterval": "30s", "policyRepositoryMatchLimit": -1}}"#,
        );
        assert_eq!(
            embeddings_config(&site, true),
            Some(EmbeddingsConfig {
                minimum_interval: Duration::from_secs(30),
                policy_repository_match_limit: Some(-1),
            })
        );

        // An unparseable interval falls back to the default.
        let site = parse_site(
            r#"{"cody.enabled": true, "licenseKey": "key", "embeddings": {"minimumInterval": "nope"}}"#,
        );
        assert_eq!(
            embeddings_config(&site, true).map(|config| config.minimum_interval),
            Some(Duration::from_secs(24 * 60 * 60))
        );
    }

    #[tokio::test]
    async fn job_metadata_matches_the_baseline() {
        let job = RepoEmbeddingScheduler::new();
        assert_eq!(
            job.description(),
            "resolves policies and schedules repos for embedding"
        );
        assert!(job.configs().is_empty());
        assert_eq!(ROUTINE_NAME, "repoEmbeddingSchedulerJob");
        assert_eq!(INTERVAL, Duration::from_secs(15 * 60));
    }
}
