#![forbid(unsafe_code)]

//! Database adapters for the RPC layer and the scheduler.

use std::time::SystemTime;

use async_trait::async_trait;
use sg_db::gitserver_repos::{GitserverReposStore, IterateRepoGitserverStatusOptions};
use sg_db::repos::RepoStore;

/// Minimal repository identity (`Repos.List` with `Names`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoName {
    pub id: i32,
    pub name: String,
}

/// The repo table subset used by `EnqueueRepoUpdate`/`RecloneRepository`.
#[async_trait]
pub trait RepoNameStore: Send + Sync {
    async fn list_by_names(&self, names: &[String]) -> Result<Vec<RepoName>, String>;
}

pub struct DbRepoNameStore {
    repos: RepoStore,
}

impl DbRepoNameStore {
    pub fn new(repos: RepoStore) -> Self {
        Self { repos }
    }
}

#[async_trait]
impl RepoNameStore for DbRepoNameStore {
    async fn list_by_names(&self, names: &[String]) -> Result<Vec<RepoName>, String> {
        self.repos
            .list_by_names(names)
            .await
            .map(|repos| {
                repos
                    .into_iter()
                    .map(|repo| RepoName {
                        id: repo.id,
                        name: repo.name,
                    })
                    .collect()
            })
            .map_err(|err| err.to_string())
    }
}

pub struct DbSchedulerStore {
    gitserver_repos: GitserverReposStore,
}

impl DbSchedulerStore {
    pub fn new(gitserver_repos: GitserverReposStore) -> Self {
        Self { gitserver_repos }
    }
}

#[async_trait]
impl crate::scheduler::SchedulerStore for DbSchedulerStore {
    async fn iterate_repo_gitserver_status(
        &self,
        cursor: i32,
        batch_size: i64,
    ) -> Result<(Vec<crate::scheduler::RepoStatus>, i32), String> {
        let options = IterateRepoGitserverStatusOptions {
            next_cursor: cursor,
            batch_size,
            only_without_shard: false,
            include_deleted: false,
        };
        let (statuses, next_cursor) = self
            .gitserver_repos
            .iterate_repo_gitserver_status(&options)
            .await
            .map_err(|err| err.to_string())?;
        let statuses = statuses
            .into_iter()
            .map(|status| crate::scheduler::RepoStatus {
                id: status.id,
                name: status.name,
                last_fetched: sg_db::offset_to_system_time(status.gitserver_repo.last_fetched),
                last_changed: sg_db::offset_to_system_time(status.gitserver_repo.last_changed),
            })
            .collect();
        Ok((statuses, next_cursor))
    }

    async fn set_last_fetched(
        &self,
        repo: &str,
        last_fetched: SystemTime,
        last_changed: SystemTime,
    ) -> Result<(), String> {
        self.gitserver_repos
            .set_last_fetched(
                repo,
                sg_db::system_time_to_offset(last_fetched),
                sg_db::system_time_to_offset(last_changed),
            )
            .await
            .map_err(|err| err.to_string())
    }
}
