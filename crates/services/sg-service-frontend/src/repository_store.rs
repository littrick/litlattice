#![forbid(unsafe_code)]

//! Production adapter for [`crate::graphql_resolvers::RepositoryLookup`]:
//! `repoStore.GetByName` (`internal/database/repos.go:206-240`) with the
//! repository permission predicate (`internal/database/repos_perm.go`), and
//! `repoStore.Get` by ID for `Query.node` (`internal/database/repos.go:145-166`).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;

use crate::graphql_resolvers::{RepoLookup, RepositoryData, RepositoryLookup};

/// The repository-page UI's gitserver rev resolution (`backend.Repos.ResolveRev`
/// → `gitserver.Client.ResolveRevision(EnsureRevision=true)`), reduced to the
/// outcomes `cmd/frontend/internal/app/ui/handlers.go` branches on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoRevResolution {
    /// The revision (or `HEAD` when none was requested) resolved to a commit.
    Resolved { commit: String },
    /// `gitdomain.RevisionNotFoundError` (also the empty-repository case for an
    /// unqualified `HEAD`).
    RevisionNotFound,
    /// `gitdomain.RepoNotExistError`; `clone_in_progress` mirrors
    /// `gitdomain.IsCloneInProgress` (`RepoNotFoundPayload`).
    RepoNotExist { clone_in_progress: bool },
    /// Any other gitserver failure (connection, protocol, …).
    Error(String),
}

/// The gitserver half of [`RepoPagesWiring`]. Production uses
/// [`GitserverRepoRevResolver`]; the UI unit tests inject fakes.
#[async_trait]
pub trait RepoRevResolver: Send + Sync {
    async fn resolve(&self, repo: &str, rev: Option<&str>) -> RepoRevResolution;
}

/// The dependencies the repository-page SPA shell needs (M9.49d):
/// `repoStore.GetByName` (with the authz predicate) and gitserver
/// `ResolveRevision`.
pub struct RepoPagesWiring {
    pub repositories: Arc<dyn RepositoryLookup>,
    /// `None` means no gitserver is configured; the page then stays a 200
    /// shell for existing repositories instead of being misclassified as 404.
    pub resolver: Option<Arc<dyn RepoRevResolver>>,
}

/// Production [`RepoRevResolver`] over the live site configuration's gitserver
/// shards (the same shard selection as [`crate::git_graphql::GitserverGitObjects`]).
pub struct GitserverRepoRevResolver {
    live: Arc<sg_config::LiveConfig>,
    clients: tokio::sync::Mutex<HashMap<Vec<String>, Arc<sg_git::GitserverClient>>>,
}

impl GitserverRepoRevResolver {
    pub fn new(live: Arc<sg_config::LiveConfig>) -> Self {
        Self {
            live,
            clients: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn client_for(
        &self,
        repo: &str,
    ) -> Result<(Arc<sg_git::GitserverClient>, String), String> {
        let unified = self.live.current();
        let addresses = sg_git::addresses::GitserverAddresses::from_unified(&unified);
        let addr = addresses
            .addr_for_repo(repo)
            .ok_or_else(|| "no gitserver configured".to_owned())?
            .to_owned();
        let mut clients = self.clients.lock().await;
        let client = match clients.get(&addresses.addresses) {
            Some(client) => Arc::clone(client),
            None => {
                let client = Arc::new(sg_git::GitserverClient::new(addresses.addresses.clone()));
                clients.insert(addresses.addresses.clone(), Arc::clone(&client));
                client
            }
        };
        Ok((client, addr))
    }
}

#[async_trait]
impl RepoRevResolver for GitserverRepoRevResolver {
    async fn resolve(&self, repo: &str, rev: Option<&str>) -> RepoRevResolution {
        let (client, addr) = match self.client_for(repo).await {
            Ok(pair) => pair,
            Err(error) => return RepoRevResolution::Error(error),
        };
        // `backend.Repos.ResolveRev` always passes `EnsureRevision: true` and
        // leaves the spec empty for the default branch (`ResolveRevision` maps
        // that to `HEAD`, like the baseline).
        match client
            .resolve_revision_at(&addr, repo, rev.unwrap_or(""), true)
            .await
        {
            Ok(commit) => RepoRevResolution::Resolved { commit },
            Err(sg_git::Error::RevisionNotFound { .. }) => RepoRevResolution::RevisionNotFound,
            Err(sg_git::Error::RepoNotFound {
                clone_in_progress, ..
            }) => RepoRevResolution::RepoNotExist { clone_in_progress },
            Err(error) => RepoRevResolution::Error(error.to_string()),
        }
    }
}

pub struct PgRepositoryStore {
    pool: PgPool,
}

impl PgRepositoryStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn to_repo_data(record: sg_db::repository_lookup::RepoLookupRecord) -> RepositoryData {
    RepositoryData {
        id: record.id,
        name: record.name,
        uri: record.uri,
        description: record.description,
        fork: record.fork,
        archived: record.archived,
        private: record.private,
        stars: record.stars,
        external_id: record.external_id,
        external_service_type: record.external_service_type,
        external_service_id: record.external_service_id,
    }
}

#[async_trait]
impl RepositoryLookup for PgRepositoryStore {
    async fn lookup(
        &self,
        name_or_uri: &str,
        authenticated_user_id: i32,
        bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        let record = sg_db::repository_lookup::find_by_name_or_uri(
            &self.pool,
            name_or_uri,
            authenticated_user_id,
            bypass_authz,
        )
        .await
        .map_err(|error| error.to_string())?;
        let Some(record) = record else {
            return Ok(RepoLookup::NotFound);
        };
        if let Some(reason) = record.blocked_reason {
            return Ok(RepoLookup::Blocked {
                name: record.name,
                reason,
            });
        }
        Ok(RepoLookup::Found(to_repo_data(record)))
    }

    /// `repoStore.Get`: a missing (or soft-deleted, or authz-filtered)
    /// repository is the `RepoNotFoundErr` resolver error
    /// (`repo not found: id=<n>`), and a blocked repository surfaces
    /// `BlockedRepoError`.
    async fn by_id(
        &self,
        repo_id: i32,
        authenticated_user_id: i32,
        bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        let record = sg_db::repository_lookup::find_by_id(
            &self.pool,
            repo_id,
            authenticated_user_id,
            bypass_authz,
        )
        .await
        .map_err(|error| error.to_string())?;
        let Some(record) = record else {
            return Ok(RepoLookup::Error(format!("repo not found: id={repo_id}")));
        };
        if let Some(reason) = record.blocked_reason {
            return Ok(RepoLookup::Blocked {
                name: record.name,
                reason,
            });
        }
        Ok(RepoLookup::Found(to_repo_data(record)))
    }

    /// `repoStore.GetByHashedName`: a missing repository is the
    /// `RepoNotFoundErr` resolver error (`repo not found`), and a blocked
    /// repository surfaces `BlockedRepoError`.
    async fn by_hashed_name(
        &self,
        hashed_name: &str,
        authenticated_user_id: i32,
        bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        let record = sg_db::repository_lookup::find_by_hashed_name(
            &self.pool,
            hashed_name,
            authenticated_user_id,
            bypass_authz,
        )
        .await
        .map_err(|error| error.to_string())?;
        let Some(record) = record else {
            return Ok(RepoLookup::Error("repo not found".to_owned()));
        };
        if let Some(reason) = record.blocked_reason {
            return Ok(RepoLookup::Blocked {
                name: record.name,
                reason,
            });
        }
        Ok(RepoLookup::Found(to_repo_data(record)))
    }
}
