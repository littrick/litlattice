#![forbid(unsafe_code)]

//! Production adapter for
//! [`crate::codeintel_policies_graphql::CodeIntelPoliciesDatabase`] (M9.28c).
//!
//! SQL is delegated to [`sg_db::codeintel_policies`]. The git-object preview
//! needs gitserver (`ListRefs`/`GetCommit`), so it goes through the existing
//! [`crate::git_graphql::GitObjectSource`] seam; the harness injects a fake
//! sharing the Go oracle's fixture refs/commits.
//!
//! `database.AuthzQueryConds` is reproduced as the internal-actor form in the
//! SQL; request-actor repository permissions remain a documented gap.

use std::sync::Arc;

use sqlx::PgPool;
use time::OffsetDateTime;

use sg_db::codeintel_policies::{
    self, ConfigurationPolicy, ConfigurationPolicyPatch, GetConfigurationPoliciesOptions,
    NewConfigurationPolicy, RepositoryRow,
};
use sg_graphql::exec::Actor;

use crate::codeintel_policies_graphql::{CodeIntelPoliciesDatabase, GitObjectRow};
use crate::codeintel_policies_matcher::{
    Extractor, GitObjectSourceMatcher, PolicyMatcher, GIT_OBJECT_TYPE_COMMIT, GIT_OBJECT_TYPE_TAG,
    GIT_OBJECT_TYPE_TREE,
};
use crate::codeintel_uploads_graphql_store::block_on;
use crate::git_graphql::GitObjectSource;

#[derive(Clone)]
pub struct PgCodeIntelPoliciesStore {
    pool: PgPool,
    git: Arc<dyn GitObjectSource>,
    repository_match_limit: Option<i64>,
}

impl std::fmt::Debug for PgCodeIntelPoliciesStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgCodeIntelPoliciesStore")
            .field("repository_match_limit", &self.repository_match_limit)
            .finish_non_exhaustive()
    }
}

impl PgCodeIntelPoliciesStore {
    pub fn new(pool: PgPool, git: Arc<dyn GitObjectSource>) -> Self {
        Self {
            pool,
            git,
            repository_match_limit: None,
        }
    }

    /// `conf.CodeIntelAutoIndexingPolicyRepositoryMatchLimit()` injection.
    pub fn with_repository_match_limit(mut self, limit: Option<i64>) -> Self {
        self.repository_match_limit = limit;
        self
    }

    /// `database.AuthzQueryConds` for the request actor (batches seam).
    fn authz(&self, actor: Actor) -> Result<String, String> {
        block_on(crate::batches_graphql_store::authz_conds_async(
            &self.pool,
            actor,
        ))
    }
}

impl CodeIntelPoliciesDatabase for PgCodeIntelPoliciesStore {
    fn get_configuration_policies(
        &self,
        opts: &GetConfigurationPoliciesOptions,
    ) -> Result<(Vec<ConfigurationPolicy>, i64), String> {
        block_on(codeintel_policies::get_configuration_policies(&self.pool, opts))
    }

    fn get_configuration_policy_by_id(
        &self,
        id: i32,
        actor: Actor,
    ) -> Result<Option<ConfigurationPolicy>, String> {
        let authz = self.authz(actor)?;
        block_on(codeintel_policies::get_configuration_policy_by_id(
            &self.pool,
            id,
            &authz,
        ))
    }

    fn create_configuration_policy(
        &self,
        policy: &NewConfigurationPolicy,
    ) -> Result<ConfigurationPolicy, String> {
        block_on(codeintel_policies::create_configuration_policy(
            &self.pool,
            policy,
        ))
    }

    fn update_configuration_policy(&self, patch: &ConfigurationPolicyPatch) -> Result<(), String> {
        block_on(codeintel_policies::update_configuration_policy(
            &self.pool,
            patch,
        ))
    }

    fn delete_configuration_policy_by_id(&self, id: i32) -> Result<(), String> {
        block_on(codeintel_policies::delete_configuration_policy_by_id(
            &self.pool, id,
        ))
    }

    fn update_repos_matching_patterns(
        &self,
        patterns: &[String],
        policy_id: i32,
        repository_match_limit: Option<i64>,
    ) -> Result<(), String> {
        block_on(codeintel_policies::update_repos_matching_patterns(
            &self.pool,
            patterns,
            policy_id,
            repository_match_limit,
        ))
        .map(|_| ())
    }

    fn get_repo_ids_by_glob_patterns(
        &self,
        patterns: &[String],
        limit: i64,
        offset: i64,
        actor: Actor,
    ) -> Result<(Vec<i32>, i64), String> {
        let authz = self.authz(actor)?;
        block_on(codeintel_policies::get_repo_ids_by_glob_patterns(
            &self.pool,
            patterns,
            limit,
            offset,
            &authz,
        ))
    }

    fn repo_count(&self) -> Result<i64, String> {
        block_on(codeintel_policies::repo_count(&self.pool))
    }

    fn get_repository(&self, id: i32) -> Result<RepositoryRow, String> {
        block_on(codeintel_policies::get_repository(&self.pool, id))
    }

    fn get_repository_names(&self, ids: &[i32]) -> Result<Vec<RepositoryRow>, String> {
        block_on(codeintel_policies::get_repository_names(&self.pool, ids))
    }

    fn git_objects_described_by_policy(
        &self,
        repository_id: i32,
        repo_name: &str,
        policy_type: &str,
        pattern: &str,
    ) -> Result<Vec<GitObjectRow>, String> {
        // `Service.GetPreviewGitObjectFilter` builds one synthetic policy and
        // runs `CommitsDescribedByPolicy(NoopExtractor, false, false)`.
        if !matches!(
            policy_type,
            GIT_OBJECT_TYPE_COMMIT | GIT_OBJECT_TYPE_TAG | GIT_OBJECT_TYPE_TREE
        ) {
            return Ok(Vec::new());
        }
        let policy = ConfigurationPolicy {
            id: 0,
            repository_id: None,
            repository_patterns: None,
            name: None,
            policy_type: policy_type.to_owned(),
            pattern: pattern.to_owned(),
            protected: false,
            retention_enabled: false,
            retention_duration_hours: None,
            retain_intermediate_commits: false,
            indexing_enabled: false,
            syntactic_indexing_enabled: false,
            index_commit_max_age_hours: None,
            index_intermediate_commits: false,
            embeddings_enabled: false,
        };
        let source = GitObjectSourceMatcher::new(self.git.as_ref());
        let matcher = PolicyMatcher::new(&source, Extractor::Noop, false, false);
        let commit_map = block_on(matcher.commits_described_by_policy(
            repository_id,
            repo_name,
            std::slice::from_ref(&policy),
            OffsetDateTime::now_utc(),
            &[],
        ))?;
        Ok(commit_map
            .into_iter()
            .flat_map(|(rev, matches)| {
                matches
                    .into_iter()
                    .map(move |policy_match| GitObjectRow {
                        name: policy_match.name,
                        rev: rev.clone(),
                        committed_at: policy_match.committed_at,
                    })
            })
            .collect())
    }

    fn current_user_site_admin(&self, actor: Actor) -> Result<bool, String> {
        match actor {
            // `internal/auth`: internal actors bypass the user lookup.
            Actor::Internal => Ok(true),
            Actor::Anonymous => Err("not authenticated".to_owned()),
            Actor::User { uid } => {
                let site_admin: Option<bool> = block_on(async {
                    sqlx::query_scalar(
                        "SELECT site_admin FROM users WHERE id = $1 AND deleted_at IS NULL",
                    )
                    .bind(uid)
                    .fetch_optional(&self.pool)
                    .await
                })
                .map_err(|error| error.to_string())?;
                match site_admin {
                    Some(true) => Ok(true),
                    Some(false) => Err("must be site admin".to_owned()),
                    None => Err("not authenticated".to_owned()),
                }
            }
        }
    }

    fn repository_match_limit(&self) -> Option<i64> {
        self.repository_match_limit
    }
}
