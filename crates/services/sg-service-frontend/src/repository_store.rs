#![forbid(unsafe_code)]

//! Production adapter for [`crate::graphql_resolvers::RepositoryLookup`]:
//! `repoStore.GetByName` (`internal/database/repos.go:206-240`) with the
//! repository permission predicate (`internal/database/repos_perm.go`).

use async_trait::async_trait;
use sqlx::PgPool;

use crate::graphql_resolvers::{RepoLookup, RepositoryData, RepositoryLookup};

pub struct PgRepositoryStore {
    pool: PgPool,
}

impl PgRepositoryStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
        Ok(RepoLookup::Found(RepositoryData {
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
        }))
    }
}
