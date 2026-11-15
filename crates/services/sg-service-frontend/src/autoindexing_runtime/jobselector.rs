#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/jobselector/job_selector.go` —
//! the precedence orchestration that decides which auto-index jobs to enqueue
//! for a `(repository, commit)` pair.
//!
//! [`JobSelector::get_jobs`] ports `GetJobs`: scheduling exceptions first,
//! then the four configuration factories in order
//!
//! 1. an explicit configuration passed by the caller,
//! 2. the configuration stored in the database,
//! 3. a `sourcegraph.yaml` committed to the repository,
//! 4. jobs inferred from the repository structure.
//!
//! The first factory that reports "present" wins. A factory that finds
//! *broken* configuration also reports "present" and yields no jobs, so a
//! broken explicit/database/in-repo configuration never falls back to a
//! lower-precedence source (only the inference factory reports "absent" when
//! its result is empty). Errors from the store, the repository lookup, the
//! git file read or the inference call abort the whole selection.
//!
//! [`JobSelector::infer_index_jobs_from_repository_structure`] ports
//! `InferIndexJobsFromRepositoryStructure`, including the inference script
//! precedence (database script, then the `SRC_CODEINTEL_INFERENCE_OVERRIDE_SCRIPT`
//! package variable, then the caller's local override) and the
//! `PRECISE_CODE_INTEL_AUTO_INDEX_MAXIMUM_INDEX_JOBS_PER_INFERRED_CONFIGURATION`
//! threshold (`>`, not `>=`) that suppresses over-large inferences unless
//! `bypass_limit` is set.
//!
//! Unported dependencies are injected as traits:
//!
//! * [`InferenceService`] — `jobselector.InferenceService` (the production
//!   Lua-recognizer pipeline lives in the `inference` package, which is not
//!   ported yet);
//! * [`SourcegraphYamlReader`] — the one `gitserver.Client.NewFileReader`
//!   use of this file. [`FileReadError::NotFound`] is the baseline's
//!   `os.IsNotExist(err)` (fall through); every other read error aborts.
//!
//! Representations:
//!
//! * [`InferenceResult`] mirrors `shared.InferenceResult`; the baseline can
//!   return a nil `*InferenceResult` with a nil error when inference is
//!   disabled for the repository. Rust returns `None` for that case, and
//!   [`JobSelector::get_jobs`] then treats it like an empty inference result
//!   instead of dereferencing nil. The baseline's `inferJobsFromRepoContents`
//!   would nil-pointer panic in that situation (see the erratum in the module
//!   documentation of the round); the port's behaviour is the intended
//!   "no jobs" outcome.
//! * [`new_auto_index_job`] ports `uploadsshared.NewAutoIndexJob`. The
//!   baseline preserves nil `DockerSteps`/`LocalSteps`/`IndexerArgs` slices
//!   until `InsertJobs`, which normalizes them to empty arrays anyway
//!   (see `sg_db::codeintel_autoindexing_enqueuer`), so the port stores empty
//!   vectors. `RequestedEnvVars` keeps its nil/empty distinction (`Option`),
//!   matching the baseline column.

use std::sync::Arc;

use async_trait::async_trait;
use sg_core::EnvReader;
use sg_db::codeintel_autoindexing_config::{
    get_index_configuration_by_repository_id, get_inference_script, repository_exceptions,
};
use sg_db::codeintel_autoindexing_enqueuer::{AutoIndexJob, DockerStep};
use sg_db::repos::{Repo, RepoStore};
use sqlx::PgPool;

use super::autoindex_config::{unmarshal_json, unmarshal_yaml, AutoIndexJobSpec};

/// `uploadsshared.JobStateQueued` (`StateQueued`).
pub const JOB_STATE_QUEUED: &str = "queued";

/// `SRC_CODEINTEL_INFERENCE_OVERRIDE_SCRIPT` — read once by the baseline at
/// package initialization (`os.Getenv`).
pub const OVERRIDE_SCRIPT_VAR: &str = "SRC_CODEINTEL_INFERENCE_OVERRIDE_SCRIPT";

/// `PRECISE_CODE_INTEL_AUTO_INDEX_MAXIMUM_INDEX_JOBS_PER_INFERRED_CONFIGURATION`
/// and its default.
pub const MAXIMUM_INDEX_JOBS_VAR: &str =
    "PRECISE_CODE_INTEL_AUTO_INDEX_MAXIMUM_INDEX_JOBS_PER_INFERRED_CONFIGURATION";
pub const DEFAULT_MAXIMUM_INDEX_JOBS: i64 = 50;

/// `shared.InferenceResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InferenceResult {
    pub index_jobs: Vec<AutoIndexJobSpec>,
    pub inference_output: String,
}

/// `jobselector.InferenceService`.
#[async_trait]
pub trait InferenceService: Send + Sync {
    async fn infer_index_jobs(
        &self,
        repo: &str,
        commit: &str,
        override_script: &str,
    ) -> Result<InferenceResult, String>;
}

/// The error classes of `gitserver.Client.NewFileReader` that the selector
/// distinguishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileReadError {
    /// `os.IsNotExist(err)`: the file is not present at the commit; the
    /// selector falls through to inference.
    NotFound,
    /// Any other read failure; aborts the selection.
    Other(String),
}

impl std::fmt::Display for FileReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileReadError::NotFound => f.write_str("file not found"),
            FileReadError::Other(error) => f.write_str(error),
        }
    }
}

/// The single `gitserver.Client.NewFileReader` use of the job selector: read
/// `sourcegraph.yaml` from the repository at the given commit.
#[async_trait]
pub trait SourcegraphYamlReader: Send + Sync {
    async fn read(&self, repo: &str, commit: &str) -> Result<String, FileReadError>;
}

/// The baseline's package-level configuration
/// (`overrideScript` and `MaximumIndexJobsPerInferredConfiguration`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSelectorSettings {
    pub maximum_index_jobs_per_inferred_configuration: i64,
    pub override_script: String,
}

impl Default for JobSelectorSettings {
    fn default() -> Self {
        Self {
            maximum_index_jobs_per_inferred_configuration: DEFAULT_MAXIMUM_INDEX_JOBS,
            override_script: String::new(),
        }
    }
}

impl JobSelectorSettings {
    /// Reads the environment the way package initialization does:
    /// `env.MustGetInt` for the threshold (invalid values are an error instead
    /// of a panic) and `os.Getenv` for the override script.
    pub fn from_env(env: &mut EnvReader) -> Result<Self, String> {
        let raw = env.get(
            MAXIMUM_INDEX_JOBS_VAR,
            &DEFAULT_MAXIMUM_INDEX_JOBS.to_string(),
            "Repositories with a number of inferred auto-index jobs exceeding this threshold will not be auto-indexed.",
        );
        let maximum_index_jobs_per_inferred_configuration = raw
            .parse::<i64>()
            .map_err(|err| format!("invalid value for {MAXIMUM_INDEX_JOBS_VAR}: {raw:?}: {err}"))?;
        Ok(Self {
            maximum_index_jobs_per_inferred_configuration,
            override_script: std::env::var(OVERRIDE_SCRIPT_VAR).unwrap_or_default(),
        })
    }
}

/// `jobselector.JobSelector`, with the store, repository store, inference
/// service and git file reader injected.
pub struct JobSelector {
    pool: PgPool,
    repo_store: RepoStore,
    inference_svc: Arc<dyn InferenceService>,
    yaml_reader: Arc<dyn SourcegraphYamlReader>,
    settings: JobSelectorSettings,
}

impl JobSelector {
    pub fn new(
        pool: PgPool,
        inference_svc: Arc<dyn InferenceService>,
        yaml_reader: Arc<dyn SourcegraphYamlReader>,
        settings: JobSelectorSettings,
    ) -> Self {
        Self {
            repo_store: RepoStore::new(pool.clone()),
            pool,
            inference_svc,
            yaml_reader,
            settings,
        }
    }

    /// `InferIndexJobsFromRepositoryStructure`. Returns `None` for the
    /// baseline's `(nil, nil)` when inference is disabled for the repository.
    pub async fn infer_index_jobs_from_repository_structure(
        &self,
        repository_id: i32,
        commit: &str,
        local_override_script: &str,
        bypass_limit: bool,
    ) -> Result<Option<InferenceResult>, String> {
        let repo = self.repo_for_id(repository_id).await?;

        let store_script = get_inference_script(&self.pool)
            .await
            .map_err(|err| format!("failed to fetch inference script from database: {err}"))?;
        let script = resolve_script(
            store_script,
            &self.settings.override_script,
            local_override_script,
        );

        let (_, can_infer) = repository_exceptions(&self.pool, repository_id)
            .await
            .map_err(|err| err.to_string())?;
        if !can_infer {
            tracing::warn!(
                repository_id,
                repo_name = %repo.name,
                "Auto-indexing job inference for this repo is disabled"
            );
            return Ok(None);
        }

        let mut result = self
            .inference_svc
            .infer_index_jobs(&repo.name, commit, &script)
            .await?;

        if !bypass_limit
            && result.index_jobs.len() as i64
                > self.settings.maximum_index_jobs_per_inferred_configuration
        {
            tracing::info!(
                repository_id,
                "Too many inferred roots. Scheduling no index jobs for repository."
            );
            result.index_jobs.clear();
        }

        Ok(Some(result))
    }

    /// `GetJobs`. An empty result means "schedule nothing"; a configuration
    /// that is present but broken also yields an empty result instead of
    /// falling through to the next source.
    pub async fn get_jobs(
        &self,
        repository_id: i32,
        commit: &str,
        configuration: &str,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJob>, String> {
        let (can_schedule, _) = repository_exceptions(&self.pool, repository_id)
            .await
            .map_err(|err| err.to_string())?;
        if !can_schedule {
            tracing::warn!(
                repository_id,
                "Auto-indexing scheduling for this repo is disabled"
            );
            return Ok(Vec::new());
        }

        if let Some(jobs) =
            self.explicit_configuration_factory(configuration, repository_id, commit)
        {
            return Ok(jobs);
        }
        if let Some(jobs) = self
            .jobs_from_config_in_database(repository_id, commit)
            .await?
        {
            return Ok(jobs);
        }
        if let Some(jobs) = self.jobs_from_config_in_repo(repository_id, commit).await? {
            return Ok(jobs);
        }
        match self
            .infer_jobs_from_repo_contents(repository_id, commit, bypass_limit)
            .await?
        {
            Some(jobs) => Ok(jobs),
            None => Ok(Vec::new()),
        }
    }

    /// `makeExplicitConfigurationFactory`. `None` means the factory did not
    /// apply (no configuration was supplied); `Some(empty)` means a supplied
    /// configuration was broken and must not fall back.
    fn explicit_configuration_factory(
        &self,
        configuration: &str,
        repository_id: i32,
        commit: &str,
    ) -> Option<Vec<AutoIndexJob>> {
        if configuration.is_empty() {
            return None;
        }

        match unmarshal_json(configuration) {
            Ok(configuration) => Some(make_queued_jobs(
                configuration.job_specs.as_deref(),
                repository_id,
                commit,
            )),
            Err(err) => {
                tracing::warn!(
                    repository_id,
                    error = %err,
                    "Failed to unmarshal index configuration"
                );
                Some(Vec::new())
            }
        }
    }

    /// `getJobsFromConfigInDatabase`.
    async fn jobs_from_config_in_database(
        &self,
        repository_id: i32,
        commit: &str,
    ) -> Result<Option<Vec<AutoIndexJob>>, String> {
        let record = get_index_configuration_by_repository_id(&self.pool, repository_id)
            .await
            .map_err(|err| format!("dbstore.GetIndexConfigurationByRepositoryID: {err}"))?;
        let record = match record {
            Some(record) => record,
            None => return Ok(None),
        };

        // Stored `lsif_index_configuration.data` is `jsonb`; the baseline
        // hands the raw bytes to `jsonx`/`encoding/json`. `jsonb` text is
        // always valid UTF-8.
        match unmarshal_json(&String::from_utf8_lossy(&record.data)) {
            Ok(configuration) => Ok(Some(make_queued_jobs(
                configuration.job_specs.as_deref(),
                repository_id,
                commit,
            ))),
            Err(err) => {
                tracing::warn!(
                    repository_id,
                    error = %err,
                    "Failed to unmarshal index configuration"
                );
                Ok(Some(Vec::new()))
            }
        }
    }

    /// `getJobsFromConfigInRepo`.
    async fn jobs_from_config_in_repo(
        &self,
        repository_id: i32,
        commit: &str,
    ) -> Result<Option<Vec<AutoIndexJob>>, String> {
        let repo = self.repo_for_id(repository_id).await?;

        let content = match self.yaml_reader.read(&repo.name, commit).await {
            Ok(content) => content,
            Err(FileReadError::NotFound) => return Ok(None),
            Err(FileReadError::Other(err)) => return Err(err),
        };

        match unmarshal_yaml(&content) {
            Ok(configuration) => Ok(Some(make_queued_jobs(
                configuration.job_specs.as_deref(),
                repository_id,
                commit,
            ))),
            Err(err) => {
                tracing::warn!(
                    repository_id,
                    error = %err,
                    "Failed to unmarshal index configuration"
                );
                Ok(Some(Vec::new()))
            }
        }
    }

    /// `inferJobsFromRepoContents`. `None` from the inference step (disabled
    /// inference or an empty result) means the factory does not apply.
    async fn infer_jobs_from_repo_contents(
        &self,
        repository_id: i32,
        commit: &str,
        bypass_limit: bool,
    ) -> Result<Option<Vec<AutoIndexJob>>, String> {
        let result = self
            .infer_index_jobs_from_repository_structure(repository_id, commit, "", bypass_limit)
            .await?;
        match result {
            Some(result) if !result.index_jobs.is_empty() => Ok(Some(make_queued_jobs(
                Some(&result.index_jobs),
                repository_id,
                commit,
            ))),
            _ => Ok(None),
        }
    }

    /// `repoStore.Get` (with `api.RepoID`): not-found and blocked
    /// repositories are errors, like the baseline.
    async fn repo_for_id(&self, repository_id: i32) -> Result<Repo, String> {
        let repo = self
            .repo_store
            .get_by_id(repository_id)
            .await
            .map_err(|err| err.to_string())?;
        match repo {
            Some(repo) if repo.blocked.is_none() => Ok(repo),
            Some(_) => Err("repository is blocked".to_owned()),
            None => Err(format!("repository not found: id={repository_id}")),
        }
    }
}

/// The script precedence of `InferIndexJobsFromRepositoryStructure`: the
/// database script wins; when it is empty the process-wide override applies;
/// a non-empty local override wins over both.
pub fn resolve_script(
    store_script: String,
    override_script: &str,
    local_override_script: &str,
) -> String {
    let mut script = store_script;
    if script.is_empty() {
        script = override_script.to_owned();
    }
    if !local_override_script.is_empty() {
        script = local_override_script.to_owned();
    }
    script
}

/// `makeQueuedJobs`.
pub fn make_queued_jobs(
    index_jobs: Option<&[AutoIndexJobSpec]>,
    repo_id: i32,
    commit: &str,
) -> Vec<AutoIndexJob> {
    index_jobs
        .unwrap_or_default()
        .iter()
        .map(|job| new_auto_index_job(job, repo_id, commit, JOB_STATE_QUEUED))
        .collect()
}

/// `uploadsshared.NewAutoIndexJob`.
pub fn new_auto_index_job(
    job: &AutoIndexJobSpec,
    repository_id: i32,
    commit: &str,
    state: &str,
) -> AutoIndexJob {
    let docker_steps = job
        .steps
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|step| DockerStep {
            root: step.root.clone(),
            image: step.image.clone(),
            commands: step.commands.clone().unwrap_or_default(),
        })
        .collect();

    AutoIndexJob {
        commit: commit.to_owned(),
        repository_id,
        state: state.to_owned(),
        docker_steps,
        local_steps: job.local_steps.clone().unwrap_or_default(),
        root: job.root.clone(),
        indexer: job.indexer.clone(),
        indexer_args: job.indexer_args.clone().unwrap_or_default(),
        outfile: job.outfile.clone(),
        requested_env_vars: job.requested_env_vars.clone(),
        ..AutoIndexJob::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoindexing_runtime::autoindex_config::{
        AutoIndexJobSpec, DockerStep as ConfigDockerStep,
    };

    fn spec() -> AutoIndexJobSpec {
        AutoIndexJobSpec {
            steps: Some(vec![
                ConfigDockerStep {
                    root: "cmd/".to_owned(),
                    image: "golang:1.22".to_owned(),
                    commands: Some(vec!["go build ./...".to_owned()]),
                },
                ConfigDockerStep {
                    root: String::new(),
                    image: "alpine".to_owned(),
                    commands: None,
                },
            ]),
            local_steps: Some(vec!["go mod download".to_owned()]),
            root: "web/".to_owned(),
            indexer: "sourcegraph/lsif-go@sha256:abc".to_owned(),
            indexer_args: Some(vec!["--no-animation".to_owned()]),
            outfile: "dump.lsif".to_owned(),
            requested_env_vars: Some(vec!["GOFLAGS".to_owned()]),
        }
    }

    // `NewAutoIndexJob` copies the raw indexer image (no
    // `extractIndexerName`), maps docker steps and keeps nil/empty
    // distinctions only where the store columns preserve them.
    #[test]
    fn new_auto_index_job_matches_the_baseline_mapping() {
        let job = new_auto_index_job(&spec(), 42, "deadbeef", JOB_STATE_QUEUED);
        assert_eq!(job.commit, "deadbeef");
        assert_eq!(job.repository_id, 42);
        assert_eq!(job.state, "queued");
        assert_eq!(
            job.docker_steps,
            vec![
                DockerStep {
                    root: "cmd/".to_owned(),
                    image: "golang:1.22".to_owned(),
                    commands: vec!["go build ./...".to_owned()],
                },
                DockerStep {
                    root: String::new(),
                    image: "alpine".to_owned(),
                    commands: Vec::new(),
                },
            ]
        );
        assert_eq!(job.local_steps, vec!["go mod download".to_owned()]);
        assert_eq!(job.root, "web/");
        assert_eq!(job.indexer, "sourcegraph/lsif-go@sha256:abc");
        assert_eq!(job.indexer_args, vec!["--no-animation".to_owned()]);
        assert_eq!(job.outfile, "dump.lsif");
        assert_eq!(job.requested_env_vars, Some(vec!["GOFLAGS".to_owned()]));
        // Fields the job selector leaves at the zero value.
        assert_eq!(job.id, 0);
        assert_eq!(job.queued_at, time::OffsetDateTime::UNIX_EPOCH);
        assert_eq!(job.num_resets, 0);
        assert_eq!(job.execution_logs, None);
        assert!(!job.should_reindex);
    }

    #[test]
    fn nil_slices_become_empty_like_insert_jobs_normalizes_them() {
        let empty = new_auto_index_job(&AutoIndexJobSpec::default(), 1, "c", JOB_STATE_QUEUED);
        assert!(empty.docker_steps.is_empty());
        assert!(empty.local_steps.is_empty());
        assert!(empty.indexer_args.is_empty());
        assert_eq!(empty.requested_env_vars, None);

        assert!(make_queued_jobs(None, 1, "c").is_empty());
        assert!(make_queued_jobs(Some(&[]), 1, "c").is_empty());
        assert_eq!(make_queued_jobs(Some(&[spec()]), 7, "c").len(), 1);
    }

    #[test]
    fn script_precedence_matches_the_baseline() {
        assert_eq!(
            resolve_script("db".to_owned(), "env", ""),
            "db",
            "the database script wins over the process override"
        );
        assert_eq!(
            resolve_script(String::new(), "env", ""),
            "env",
            "the process override fills an empty database script"
        );
        assert_eq!(
            resolve_script("db".to_owned(), "env", "local"),
            "local",
            "a local override wins over both"
        );
        assert_eq!(
            resolve_script(String::new(), "env", "local"),
            "local",
            "a local override fills an empty database script"
        );
        assert_eq!(resolve_script(String::new(), "", ""), "");
    }

    #[test]
    fn settings_default_and_parsing() {
        let defaults = JobSelectorSettings::default();
        assert_eq!(
            defaults.maximum_index_jobs_per_inferred_configuration,
            DEFAULT_MAXIMUM_INDEX_JOBS
        );
        assert_eq!(defaults.maximum_index_jobs_per_inferred_configuration, 50);
        assert_eq!(defaults.override_script, "");
    }
}
