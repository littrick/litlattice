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
use sg_git::RefType;
use sg_graphql::exec::Actor;

use crate::codeintel_policies_graphql::{CodeIntelPoliciesDatabase, GitObjectRow};
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
}

fn timestamp_to_offset(timestamp: Option<prost_types::Timestamp>) -> OffsetDateTime {
    let Some(timestamp) = timestamp else {
        // `(*timestamppb.Timestamp)(nil).AsTime()` is the Unix epoch.
        return OffsetDateTime::UNIX_EPOCH;
    };
    OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        + time::Duration::nanoseconds(i64::from(timestamp.nanos))
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
    ) -> Result<Option<ConfigurationPolicy>, String> {
        block_on(codeintel_policies::get_configuration_policy_by_id(
            &self.pool,
            id,
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
    ) -> Result<(Vec<i32>, i64), String> {
        block_on(codeintel_policies::get_repo_ids_by_glob_patterns(
            &self.pool,
            patterns,
            limit,
            offset,
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
        match policy_type {
            "GIT_COMMIT" => match block_on(self.git.get_commit(repository_id, repo_name, pattern)) {
                // `matchCommitPolicies` skips `RevisionNotFoundError`.
                Err(error) if error.revision_not_found => Ok(Vec::new()),
                Err(error) => Err(error.message),
                Ok(commit) => {
                    let committed_at = timestamp_to_offset(
                        commit.committer.as_ref().and_then(|signature| signature.date),
                    );
                    Ok(vec![GitObjectRow {
                        name: commit.oid,
                        rev: pattern.to_owned(),
                        committed_at,
                    }])
                }
            },
            "GIT_TAG" | "GIT_TREE" => {
                let refs = block_on(self.git.list_refs(repository_id, repo_name, true, true))
                    .map_err(|error| error.message)?;
                let target = if policy_type == "GIT_TAG" {
                    RefType::Tag
                } else {
                    RefType::Branch
                };
                let matcher = GlobMatcher::compile(pattern)?;
                Ok(refs
                    .into_iter()
                    .filter(|reference| reference.ref_type == target)
                    .filter(|reference| matcher.matches(&reference.short_name))
                    .map(|reference| GitObjectRow {
                        name: reference.short_name,
                        rev: reference.target_commit,
                        committed_at: timestamp_to_offset(reference.created_at),
                    })
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
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

//
// Glob matching: the subset of `gobwas/glob` the code-intel patterns use
// (`*`, `?`, `[...]`, `[!...]`, ranges). Alternation (`{a,b}`) is a documented
// gap.
//

enum Token {
    Literal(char),
    AnyOne,
    AnySeq,
    Class { ranges: Vec<(char, char)>, negated: bool },
}

struct GlobMatcher {
    tokens: Vec<Token>,
}

impl GlobMatcher {
    fn compile(pattern: &str) -> Result<Self, String> {
        let mut tokens = Vec::new();
        let mut chars = pattern.chars().peekable();
        while let Some(character) = chars.next() {
            match character {
                '*' => tokens.push(Token::AnySeq),
                '?' => tokens.push(Token::AnyOne),
                '[' => {
                    let mut negated = false;
                    if matches!(chars.peek(), Some('!')) {
                        chars.next();
                        negated = true;
                    }
                    let mut ranges = Vec::new();
                    let mut closed = false;
                    let mut pending: Option<char> = None;
                    while let Some(current) = chars.next() {
                        match current {
                            ']' if pending.is_none() => {
                                closed = true;
                                break;
                            }
                            '-' if pending.is_some() => {
                                if let Some(end) = chars.next() {
                                    ranges.push((pending.take().expect("pending"), end));
                                }
                            }
                            other => {
                                if let Some(start) = pending.take() {
                                    ranges.push((start, start));
                                }
                                pending = Some(other);
                            }
                        }
                    }
                    if !closed {
                        return Err(format!(
                            "failed to compile glob pattern `{pattern}` in configuration policy 0"
                        ));
                    }
                    if let Some(start) = pending {
                        ranges.push((start, start));
                    }
                    tokens.push(Token::Class { ranges, negated });
                }
                other => tokens.push(Token::Literal(other)),
            }
        }
        Ok(Self { tokens })
    }

    fn matches(&self, value: &str) -> bool {
        let chars: Vec<char> = value.chars().collect();
        Self::match_at(&self.tokens, &chars)
    }

    fn match_at(tokens: &[Token], chars: &[char]) -> bool {
        match tokens.first() {
            None => chars.is_empty(),
            Some(Token::Literal(expected)) => {
                chars.first() == Some(expected) && Self::match_at(&tokens[1..], &chars[1..])
            }
            Some(Token::AnyOne) => {
                !chars.is_empty() && Self::match_at(&tokens[1..], &chars[1..])
            }
            Some(Token::AnySeq) => {
                for skip in 0..=chars.len() {
                    if Self::match_at(&tokens[1..], &chars[skip..]) {
                        return true;
                    }
                }
                false
            }
            Some(Token::Class { ranges, negated }) => match chars.first() {
                None => false,
                Some(character) => {
                    let inside = ranges
                        .iter()
                        .any(|(start, end)| start <= character && character <= end);
                    inside != *negated && Self::match_at(&tokens[1..], &chars[1..])
                }
            },
        }
    }
}
