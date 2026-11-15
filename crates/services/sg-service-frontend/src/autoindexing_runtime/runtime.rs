#![forbid(unsafe_code)]

//! Production [`CodeIntelAutoindexingJobsRuntime`] (M9.44c).
//!
//! Binds the ported inference/selection/enqueue chain to the GraphQL surface so
//! `inferAutoIndexJobsForRepo` and `queueAutoIndexJobsForRepo` no longer report
//! "auto-indexing jobs runtime is not configured":
//!
//! * `InferIndexConfiguration` ([service.go:74](../../../../../origin/repo/internal/codeintel/autoindexing/service.go#L74)):
//!   repository lookup, default-branch/commit resolution via gitserver, then
//!   `JobSelector.InferIndexJobsFromRepositoryStructure`.
//! * `QueueAutoIndexJobs` ([service.go:121](../../../../../origin/repo/internal/codeintel/autoindexing/service.go#L121)):
//!   `IndexEnqueuer.QueueAutoIndexJobs`, which writes the existing
//!   `lsif_indexes` table.
//!
//! Errors follow the baseline's wording. Deviations:
//!
//! * When the default branch has no commit the baseline returns `(nil, nil)`
//!   and its GraphQL resolver dereferences the nil result (panic); this port
//!   returns an empty outcome, i.e. "nothing to infer" (recorded erratum).
//! * `QueueAutoIndexJobsForPackage` is not exposed by GraphQL (the Go worker's
//!   dependency index scheduler calls it); this runtime carries the same
//!   enqueuer with the shared [`DefaultPackageRepoInference`] and exposes
//!   [`ProductionAutoindexingJobsRuntime::queue_auto_index_jobs_for_package`]
//!   so the frontend process and the worker use one implementation.
//! * Newly inserted index records always have `execution_logs = NULL`, so the
//!   returned rows carry an empty execution-log list.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;

use sg_config::SiteConfiguration;
use sg_core::EnvReader;
use sg_db::codeintel_autoindexing_enqueuer::AutoIndexJob;
use sg_db::repos::{Repo, RepoStore};
use sg_git::GitserverClient;
use sg_luasandbox::Service as LuaSandboxService;

use crate::autoindexing_runtime::autoindex_config::{AutoIndexJobSpec, DockerStep as ConfigDockerStep};
use crate::autoindexing_runtime::enqueuer::{
    IndexEnqueuer, QueueAutoIndexJobsError, VersionedPackageRepo,
};
use crate::autoindexing_runtime::inference_production::{
    inference_environment, GitserverGitService, GitserverRevisionResolver, GitserverYamlReader,
    InferenceAdapter, TokenBucketLimiter,
};
use crate::autoindexing_runtime::inference::{InferenceService, SandboxService};
use crate::autoindexing_runtime::jobselector::{
    InferenceService as JobSelectorInferenceService, JobSelector, JobSelectorSettings,
    SourcegraphYamlReader,
};
use crate::autoindexing_runtime::package_inference::DefaultPackageRepoInference;
use crate::codeintel_autoindexing_graphql::{
    CodeIntelAutoindexingJobsRuntime, InferenceOutcome, InferredJobRow,
};
use crate::codeintel_uploads_graphql::{AutoIndexJobRow, DockerStep};
use crate::codeintel_uploads_graphql_store::block_on;

/// The `gitserver.Client` calls `Service.InferIndexConfiguration` makes, split
/// from the trait so tests can supply a fake without a gitserver.
#[async_trait]
pub trait RepositoryGit: Send + Sync {
    /// `gitserver.Client.GetDefaultBranch(repo, false)`.
    async fn default_branch_commit(&self, repo: &str) -> Result<(String, String), String>;

    /// `gitserver.Client.GetCommit(repo, commit)`; only the existence check
    /// matters.
    async fn get_commit(&self, repo: &str, commit: &str) -> Result<(), CommitCheckError>;
}

/// The error classes `Service.InferIndexConfiguration` distinguishes for
/// `GetCommit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitCheckError {
    /// `*gitdomain.RevisionNotFoundError`.
    RevisionNotFound,
    /// Any other failure.
    Other(String),
}

/// [`RepositoryGit`] backed by a real gitserver client.
pub struct GitserverRepositoryGit {
    client: Arc<GitserverClient>,
}

impl GitserverRepositoryGit {
    pub fn new(client: Arc<GitserverClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RepositoryGit for GitserverRepositoryGit {
    async fn default_branch_commit(&self, repo: &str) -> Result<(String, String), String> {
        self.client
            .default_branch_commit(repo)
            .await
            .map_err(|error| error.to_string())
    }

    async fn get_commit(&self, repo: &str, commit: &str) -> Result<(), CommitCheckError> {
        match self.client.get_commit(repo, commit).await {
            Ok(_) => Ok(()),
            Err(sg_git::Error::RevisionNotFound { .. }) => Err(CommitCheckError::RevisionNotFound),
            Err(error) => Err(CommitCheckError::Other(error.to_string())),
        }
    }
}

/// The production runtime injected into `PgCodeIntelAutoindexingStore`.
pub struct ProductionAutoindexingJobsRuntime {
    repo_store: RepoStore,
    git: Arc<dyn RepositoryGit>,
    selector: JobSelector,
    enqueuer: Arc<IndexEnqueuer>,
}

impl ProductionAutoindexingJobsRuntime {
    pub fn new(
        pool: PgPool,
        git: Arc<dyn RepositoryGit>,
        selector: JobSelector,
        enqueuer: Arc<IndexEnqueuer>,
    ) -> Self {
        Self {
            repo_store: RepoStore::new(pool),
            git,
            selector,
            enqueuer,
        }
    }

    /// Builds the chain from the live site configuration:
    /// `inference.NewService` (Lua sandbox + gitserver adapters + rate limiter),
    /// the production job selector (gitserver `sourcegraph.yaml` reader and
    /// inference adapter) and the `lsif_indexes` enqueuer.
    pub fn from_site_configuration(
        pool: PgPool,
        client: Arc<GitserverClient>,
        site: &SiteConfiguration,
    ) -> Result<Self, String> {
        let mut env = EnvReader::new();
        let environment = inference_environment(site, &mut env)?;
        let settings = JobSelectorSettings::from_env(&mut env)?;

        let sandbox: Arc<dyn SandboxService> = Arc::new(LuaSandboxService::new());
        let inference_service = Arc::new(InferenceService::new(
            sandbox,
            Arc::new(GitserverGitService::new(Arc::clone(&client))),
            Arc::new(TokenBucketLimiter::new(
                environment.gitserver_request_rate_limit,
            )),
            environment.settings,
        ));
        let selection: Arc<dyn JobSelectorInferenceService> =
            Arc::new(InferenceAdapter::new(inference_service));
        let yaml_reader: Arc<dyn SourcegraphYamlReader> =
            Arc::new(GitserverYamlReader::new(Arc::clone(&client)));

        let make_selector = || {
            JobSelector::new(
                pool.clone(),
                Arc::clone(&selection),
                Arc::clone(&yaml_reader),
                settings.clone(),
            )
        };
        let selector = make_selector();
        let enqueuer = Arc::new(IndexEnqueuer::new(
            pool.clone(),
            Arc::new(GitserverRevisionResolver::new(Arc::clone(&client))),
            Arc::new(DefaultPackageRepoInference),
            make_selector(),
        ));

        Ok(Self::new(
            pool,
            Arc::new(GitserverRepositoryGit::new(client)),
            selector,
            enqueuer,
        ))
    }

    async fn repository(&self, repository_id: i32) -> Result<Repo, String> {
        match self
            .repo_store
            .get_by_id(repository_id)
            .await
            .map_err(|error| error.to_string())?
        {
            Some(repo) if repo.blocked.is_none() => Ok(repo),
            Some(_) => Err("repository is blocked".to_owned()),
            None => Err(format!("repository not found: id={repository_id}")),
        }
    }

    /// `Service.InferIndexConfiguration`'s commit resolution. `Ok(None)` means
    /// the default branch has no commit (nothing to infer).
    async fn resolve_commit(
        &self,
        repository_id: i32,
        commit: &str,
    ) -> Result<Option<String>, String> {
        let repo = self.repository(repository_id).await?;
        if commit.is_empty() {
            let (_, sha) = self
                .git
                .default_branch_commit(&repo.name)
                .await
                .map_err(|error| {
                    format!(
                        "gitserver.GetDefaultBranch: error resolving HEAD for {repository_id}: {error}"
                    )
                })?;
            if sha.is_empty() {
                return Ok(None);
            }
            Ok(Some(sha))
        } else {
            match self.git.get_commit(&repo.name, commit).await {
                Ok(()) => Ok(Some(commit.to_owned())),
                Err(CommitCheckError::RevisionNotFound) => Err(format!(
                    "revision {commit} not found for {repository_id}"
                )),
                Err(CommitCheckError::Other(error)) => Err(format!(
                    "gitserver.CommitExists: error checking {commit} for {repository_id}: {error}"
                )),
            }
        }
    }

    async fn infer(
        &self,
        repository_id: i32,
        commit: &str,
        local_override_script: &str,
        bypass_limit: bool,
    ) -> Result<InferenceOutcome, String> {
        let Some(commit) = self.resolve_commit(repository_id, commit).await? else {
            return Ok(InferenceOutcome::default());
        };
        let result = self
            .selector
            .infer_index_jobs_from_repository_structure(
                repository_id,
                &commit,
                local_override_script,
                bypass_limit,
            )
            .await?;
        Ok(inference_outcome(result))
    }

    async fn queue(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJobRow>, String> {
        let jobs = self
            .enqueuer
            .queue_auto_index_jobs(repository_id, rev, configuration, force, bypass_limit)
            .await
            .map_err(|error| match error {
                QueueAutoIndexJobsError::Other(message) => message,
                QueueAutoIndexJobsError::RevisionNotFound => "revision not found".to_owned(),
            })?;
        Ok(jobs.into_iter().map(auto_index_job_row).collect())
    }

    /// `Service.QueueAutoIndexJobsForPackage`. The Go baseline reaches this
    /// through the worker's dependency index scheduler, not GraphQL; the
    /// inherent method keeps the same implementation available to the
    /// frontend process and the differential harness. `Ok(())` covers both
    /// "no repository mapping" and "revision not found", matching the
    /// baseline's silent no-ops; every other failure is an error and no job is
    /// fabricated.
    pub fn queue_auto_index_jobs_for_package(
        &self,
        package: &VersionedPackageRepo,
    ) -> Result<(), String> {
        block_on(self.enqueuer.queue_auto_index_jobs_for_package(package))
    }
}

impl CodeIntelAutoindexingJobsRuntime for ProductionAutoindexingJobsRuntime {
    fn infer_index_configuration(
        &self,
        repository_id: i32,
        commit: &str,
        local_override_script: &str,
        bypass_limit: bool,
    ) -> Result<InferenceOutcome, String> {
        block_on(self.infer(
            repository_id,
            commit,
            local_override_script,
            bypass_limit,
        ))
    }

    fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJobRow>, String> {
        block_on(self.queue(
            repository_id,
            rev,
            configuration,
            force,
            bypass_limit,
        ))
    }
}

fn inference_outcome(result: Option<crate::autoindexing_runtime::jobselector::InferenceResult>) -> InferenceOutcome {
    let Some(result) = result else {
        return InferenceOutcome::default();
    };
    InferenceOutcome {
        jobs: result
            .index_jobs
            .iter()
            .map(inferred_job_row)
            .collect(),
        inference_output: result.inference_output,
    }
}

fn inferred_job_row(spec: &AutoIndexJobSpec) -> InferredJobRow {
    InferredJobRow {
        steps: spec
            .steps
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|step: &ConfigDockerStep| DockerStep {
                root: step.root.clone(),
                image: step.image.clone(),
                commands: step.commands.clone().unwrap_or_default(),
            })
            .collect(),
        local_steps: spec.local_steps.clone().unwrap_or_default(),
        root: spec.root.clone(),
        indexer: spec.indexer.clone(),
        indexer_args: spec.indexer_args.clone().unwrap_or_default(),
        outfile: spec.outfile.clone(),
        requested_env_vars: spec.requested_env_vars.clone(),
    }
}

fn auto_index_job_row(job: AutoIndexJob) -> AutoIndexJobRow {
    AutoIndexJobRow {
        id: job.id,
        commit: job.commit,
        queued_at: job.queued_at,
        state: job.state,
        failure_message: job.failure_message,
        started_at: job.started_at,
        finished_at: job.finished_at,
        repository_id: job.repository_id,
        docker_steps: job
            .docker_steps
            .into_iter()
            .map(|step| DockerStep {
                root: step.root,
                image: step.image,
                commands: step.commands,
            })
            .collect(),
        root: job.root,
        indexer: job.indexer,
        indexer_args: job.indexer_args,
        outfile: job.outfile,
        // A freshly inserted index record has `execution_logs = NULL`.
        execution_logs: Vec::new(),
        rank: job.rank.map(|rank| rank as i32),
        local_steps: job.local_steps,
        associated_upload_id: job.associated_upload_id,
        should_reindex: job.should_reindex,
        requested_env_vars: job.requested_env_vars.unwrap_or_default(),
    }
}

