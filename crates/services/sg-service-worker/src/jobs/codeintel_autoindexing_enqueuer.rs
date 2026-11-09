#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/enqueuer/enqueuer.go` — the
//! production `IndexEnqueuer` orchestration on top of the job selector and
//! the `lsif_indexes` store.
//!
//! [`IndexEnqueuer::queue_auto_index_jobs`] ports `QueueAutoIndexJobs`:
//! repository lookup, `ResolveRevision` (`EnsureRevision=true`), then
//! [`IndexEnqueuer::queue_jobs_for_repo_and_commit`]. With `force=false` an
//! existing upload/index record for the commit (`IsQueued`) makes the call a
//! no-op; without `force` every selected job is additionally filtered through
//! `IsQueuedRootIndexer`, and the surviving jobs are inserted (`InsertJobs`,
//! internal actor). `force=true` skips both checks.
//!
//! [`IndexEnqueuer::queue_auto_index_jobs_for_package`] ports
//! `QueueAutoIndexJobsForPackage`: map the package to a repository/revision
//! (the `inference` package's `InferRepositoryAndRevision`), resolve the
//! revision (`errcode.IsNotFound` is a silent no-op), then enqueue with no
//! explicit configuration and `force=false, bypassLimit=false`.
//!
//! Unported dependencies are injected as traits:
//!
//! * [`RevisionResolver`] — `gitserver.Client.ResolveRevision` called with
//!   `ResolveRevisionOptions{EnsureRevision: true}` (the only option this file
//!   uses). [`ResolveRevisionError::NotFound`] models `errcode.IsNotFound`,
//!   which only the package method special-cases.
//! * [`PackageRepoInference`] — `inference.InferRepositoryAndRevision` (the
//!   recognizer table lives in the unported `inference` package; the
//!   per-scheme `reposource` parsing is not ported yet).
//!
//! Representation notes:
//!
//! * `InsertJobs` returns nil for an empty input; the port returns an empty
//!   `Vec`. As in the job selector, nil and empty are indistinguishable to
//!   callers (length checks only).
//! * Not-found and blocked repositories are errors, like
//!   `database.RepoStore.Get` / `GetByName`.
//! * `InsertJobs` is called with `enqueuer_user_id = 0` (the internal actor;
//!   the baseline reads it from the context).

use std::sync::Arc;

use async_trait::async_trait;
use sg_core::EnvReader;
use sg_db::codeintel_autoindexing_enqueuer::{
    insert_jobs, is_queued, is_queued_root_indexer, AutoIndexJob,
};
use sg_db::repos::{Repo, RepoStore};
use sqlx::PgPool;

use super::codeintel_autoindexing_jobselector::{JobSelector, JobSelectorSettings};

/// `dependencies.MinimialVersionedPackageRepo`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionedPackageRepo {
    pub scheme: String,
    pub name: String,
    pub version: String,
}

/// The error classes of `gitserver.Client.ResolveRevision` that the enqueuer
/// distinguishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveRevisionError {
    /// `errcode.IsNotFound(err)`: the package method treats it as "nothing to
    /// do"; `QueueAutoIndexJobs` propagates it.
    NotFound,
    /// Any other resolution failure; always propagated.
    Other(String),
}

impl std::fmt::Display for ResolveRevisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveRevisionError::NotFound => f.write_str("revision not found"),
            ResolveRevisionError::Other(error) => f.write_str(error),
        }
    }
}

/// `gitserver.Client.ResolveRevision(…, ResolveRevisionOptions{EnsureRevision:
/// true})`.
#[async_trait]
pub trait RevisionResolver: Send + Sync {
    async fn resolve_revision(&self, repo: &str, rev: &str)
        -> Result<String, ResolveRevisionError>;
}

/// `inference.InferRepositoryAndRevision`.
pub trait PackageRepoInference: Send + Sync {
    fn infer_repository_and_revision(
        &self,
        package: &VersionedPackageRepo,
    ) -> Option<(String, String)>;
}

/// `enqueuer.IndexEnqueuer`.
pub struct IndexEnqueuer {
    pool: PgPool,
    repo_store: RepoStore,
    revision_resolver: Arc<dyn RevisionResolver>,
    package_inference: Arc<dyn PackageRepoInference>,
    job_selector: JobSelector,
}

impl IndexEnqueuer {
    pub fn new(
        pool: PgPool,
        revision_resolver: Arc<dyn RevisionResolver>,
        package_inference: Arc<dyn PackageRepoInference>,
        job_selector: JobSelector,
    ) -> Self {
        Self {
            repo_store: RepoStore::new(pool.clone()),
            pool,
            revision_resolver,
            package_inference,
            job_selector,
        }
    }

    /// Builds the production wiring from the environment (the job selector
    /// reads its own threshold/override variables).
    pub fn from_env(
        pool: PgPool,
        revision_resolver: Arc<dyn RevisionResolver>,
        package_inference: Arc<dyn PackageRepoInference>,
        inference_svc: Arc<dyn super::codeintel_autoindexing_jobselector::InferenceService>,
        yaml_reader: Arc<dyn super::codeintel_autoindexing_jobselector::SourcegraphYamlReader>,
        env: &mut EnvReader,
    ) -> Result<Self, String> {
        let settings = JobSelectorSettings::from_env(env)?;
        let selector = JobSelector::new(pool.clone(), inference_svc, yaml_reader, settings);
        Ok(Self::new(
            pool,
            revision_resolver,
            package_inference,
            selector,
        ))
    }
}

impl IndexEnqueuer {
    /// `QueueAutoIndexJobs`.
    pub async fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJob>, String> {
        let repo = self.repo_for_id(repository_id).await?;

        let commit = self
            .revision_resolver
            .resolve_revision(&repo.name, rev)
            .await
            .map_err(|err| format!("gitserver.ResolveRevision: {err}"))?;

        self.queue_jobs_for_repo_and_commit(
            repository_id,
            &commit,
            configuration,
            force,
            bypass_limit,
        )
        .await
    }

    /// `QueueAutoIndexJobsForPackage`.
    pub async fn queue_auto_index_jobs_for_package(
        &self,
        package: &VersionedPackageRepo,
    ) -> Result<(), String> {
        let (repo_name, revision) = match self
            .package_inference
            .infer_repository_and_revision(package)
        {
            Some(mapping) => mapping,
            None => return Ok(()),
        };

        let repo = self
            .repo_store
            .get_by_name(&repo_name)
            .await
            .map_err(|err| format!("store.Repos.GetByName: {err}"))?;
        let repo = match repo {
            Some(repo) if repo.blocked.is_none() => repo,
            Some(_) => return Err("repository is blocked".to_owned()),
            None => return Err(format!("repository not found: {repo_name}")),
        };

        let commit = match self
            .revision_resolver
            .resolve_revision(&repo_name, &revision)
            .await
        {
            Ok(commit) => commit,
            Err(ResolveRevisionError::NotFound) => return Ok(()),
            Err(ResolveRevisionError::Other(err)) => {
                return Err(format!("gitserverClient.ResolveRevision: {err}"))
            }
        };

        self.queue_jobs_for_repo_and_commit(repo.id, &commit, "", false, false)
            .await?;
        Ok(())
    }

    /// `queueJobsForRepoAndCommit`.
    async fn queue_jobs_for_repo_and_commit(
        &self,
        repository_id: i32,
        commit: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJob>, String> {
        if !force {
            let queued = is_queued(&self.pool, repository_id, commit)
                .await
                .map_err(|err| format!("dbstore.IsQueued: {err}"))?;
            if queued {
                return Ok(Vec::new());
            }
        }

        let indexes = self
            .job_selector
            .get_jobs(repository_id, commit, configuration, bypass_limit)
            .await?;
        if indexes.is_empty() {
            return Ok(Vec::new());
        }

        let jobs_to_insert = if force {
            indexes
        } else {
            let mut jobs = Vec::new();
            for index in indexes {
                let queued = is_queued_root_indexer(
                    &self.pool,
                    repository_id,
                    commit,
                    &index.root,
                    &index.indexer,
                )
                .await
                .map_err(|err| format!("dbstore.IsQueuedRootIndexer: {err}"))?;
                if !queued {
                    jobs.push(index);
                }
            }
            jobs
        };

        let inserted = insert_jobs(&self.pool, &jobs_to_insert, 0)
            .await
            .map_err(|err| err.to_string())?;
        Ok(inserted.unwrap_or_default())
    }

    /// `repoStore.Get`: not-found and blocked repositories are errors.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::codeintel_autoindexing_jobselector::DEFAULT_MAXIMUM_INDEX_JOBS;

    struct NoInference;

    impl PackageRepoInference for NoInference {
        fn infer_repository_and_revision(
            &self,
            _package: &VersionedPackageRepo,
        ) -> Option<(String, String)> {
            None
        }
    }

    struct UnusedResolver;

    #[async_trait]
    impl RevisionResolver for UnusedResolver {
        async fn resolve_revision(
            &self,
            _repo: &str,
            _rev: &str,
        ) -> Result<String, ResolveRevisionError> {
            panic!("resolver must not be called");
        }
    }

    struct UnusedInference;

    #[async_trait]
    impl super::super::codeintel_autoindexing_jobselector::InferenceService for UnusedInference {
        async fn infer_index_jobs(
            &self,
            _repo: &str,
            _commit: &str,
            _override_script: &str,
        ) -> Result<super::super::codeintel_autoindexing_jobselector::InferenceResult, String>
        {
            panic!("inference must not be called");
        }
    }

    struct UnusedYaml;

    #[async_trait]
    impl super::super::codeintel_autoindexing_jobselector::SourcegraphYamlReader for UnusedYaml {
        async fn read(
            &self,
            _repo: &str,
            _commit: &str,
        ) -> Result<String, super::super::codeintel_autoindexing_jobselector::FileReadError>
        {
            panic!("yaml reader must not be called");
        }
    }

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://sourcegraph:sourcegraph@127.0.0.1:5432/sourcegraph")
            .expect("lazy pool")
    }

    fn selector(pool: PgPool) -> JobSelector {
        JobSelector::new(
            pool,
            Arc::new(UnusedInference),
            Arc::new(UnusedYaml),
            JobSelectorSettings {
                maximum_index_jobs_per_inferred_configuration: DEFAULT_MAXIMUM_INDEX_JOBS,
                override_script: String::new(),
            },
        )
    }

    // `InferRepositoryAndRevision` returning false is a silent no-op and the
    // store is never touched.
    #[tokio::test]
    async fn package_without_repository_mapping_is_a_noop() {
        let pool = lazy_pool();
        let enqueuer = IndexEnqueuer::new(
            pool.clone(),
            Arc::new(UnusedResolver),
            Arc::new(NoInference),
            selector(pool),
        );
        let package = VersionedPackageRepo {
            scheme: "nonsense".to_owned(),
            name: "x".to_owned(),
            version: "1".to_owned(),
        };
        enqueuer
            .queue_auto_index_jobs_for_package(&package)
            .await
            .expect("no-op");
    }
}
