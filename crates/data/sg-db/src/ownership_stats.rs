#![forbid(unsafe_code)]

//! `internal/database/ownership_stats.go` (the parts the Own analytics
//! indexer needs) plus the assigned-ownership reads and the ingested
//! CODEOWNERS lookup used by `internal/own/service.go`.
//!
//! Ported statements:
//!
//! * `UpdateAggregateCounts` (`aggregateCountsUpdateFmtstr`): upsert one
//!   repo-path root row in `ownership_path_stats`. Idempotent because the
//!   `file_path_id` is the conflict target.
//! * `AssignedOwners.ListAssignedOwnersForRepo` /
//!   `AssignedTeams.ListAssignedTeamsForRepo`, reduced to the per-path
//!   presence the analytics indexer's `.Match` predicate needs.
//! * `Codeowners.GetCodeownersForRepo`, reduced to the raw `contents` text
//!   (the Rust port parses the file text directly instead of the protobuf
//!   column; see the overview errata).

use std::collections::HashMap;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

use crate::repo_paths::ensure_repo_paths;

/// `database.PathAggregateCounts`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PathAggregateCounts {
    /// `tree_codeowned_files_count`.
    pub codeowned_file_count: i32,
    /// `tree_assigned_ownership_files_count`.
    pub assigned_ownership_file_count: i32,
    /// `tree_any_ownership_files_count`.
    pub total_owned_file_count: i32,
}

/// `aggregateCountsUpdateFmtstr`:
///
/// ```sql
/// INSERT INTO ownership_path_stats (
///     file_path_id, tree_codeowned_files_count,
///     tree_assigned_ownership_files_count, tree_any_ownership_files_count,
///     last_updated_at)
/// VALUES ($1, $2, $3, $4, $5)
/// ON CONFLICT (file_path_id)
/// DO UPDATE SET ...
/// ```
///
/// Returns the number of affected rows (Go's `UpdateAggregateCounts`), after
/// ensuring the path exists in `repo_paths` (the root path `""`).
pub async fn update_aggregate_counts(
    pool: &PgPool,
    repo_id: i32,
    path: &str,
    counts: PathAggregateCounts,
    timestamp: OffsetDateTime,
) -> Result<i32, String> {
    let path_ids = ensure_repo_paths(pool, &[path.to_owned()], repo_id).await?;
    if path_ids.len() != 1 {
        return Err(format!("want exactly 1 repo path, got {}", path_ids.len()));
    }
    let result = sqlx::query(
        "INSERT INTO ownership_path_stats ( \
             file_path_id, \
             tree_codeowned_files_count, \
             tree_assigned_ownership_files_count, \
             tree_any_ownership_files_count, \
             last_updated_at) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (file_path_id) \
         DO UPDATE SET \
             tree_codeowned_files_count = EXCLUDED.tree_codeowned_files_count, \
             tree_assigned_ownership_files_count = EXCLUDED.tree_assigned_ownership_files_count, \
             tree_any_ownership_files_count = EXCLUDED.tree_any_ownership_files_count, \
             last_updated_at = EXCLUDED.last_updated_at",
    )
    .bind(path_ids[0])
    .bind(counts.codeowned_file_count)
    .bind(counts.assigned_ownership_file_count)
    .bind(counts.total_owned_file_count)
    .bind(timestamp)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(result.rows_affected() as i32)
}

/// `AssignedOwners.ListAssignedOwnersForRepo`: distinct repo-relative paths
/// that have at least one assigned owner. Only presence matters for
/// `AssignedOwners.Match`.
pub async fn assigned_owner_paths(
    pool: &PgPool,
    repo_id: i32,
) -> Result<Vec<String>, String> {
    let rows = sqlx::query(
        "SELECT DISTINCT p.absolute_path \
         FROM assigned_owners AS a \
         INNER JOIN repo_paths AS p ON p.id = a.file_path_id \
         WHERE p.repo_id = $1",
    )
    .bind(repo_id)
    .fetch_all(pool)
    .await
    .map_err(|err| err.to_string())?;
    rows.into_iter()
        .map(|row| sqlx::Row::try_get::<String, _>(&row, "absolute_path"))
        .collect::<Result<_, _>>()
        .map_err(|err| err.to_string())
}

/// `AssignedTeams.ListAssignedTeamsForRepo`: distinct repo-relative paths
/// that have at least one assigned team.
pub async fn assigned_team_paths(
    pool: &PgPool,
    repo_id: i32,
) -> Result<Vec<String>, String> {
    let rows = sqlx::query(
        "SELECT DISTINCT p.absolute_path \
         FROM assigned_teams AS a \
         INNER JOIN repo_paths AS p ON p.id = a.file_path_id \
         WHERE p.repo_id = $1",
    )
    .bind(repo_id)
    .fetch_all(pool)
    .await
    .map_err(|err| err.to_string())?;
    rows.into_iter()
        .map(|row| sqlx::Row::try_get::<String, _>(&row, "absolute_path"))
        .collect::<Result<_, _>>()
        .map_err(|err| err.to_string())
}

/// `Codeowners.GetCodeownersForRepo`, returning the raw `contents` text.
/// `Ok(None)` is `CodeownersFileNotFoundError` (`errcode.IsNotFound`), which
/// makes `RulesetForRepo` fall back to the repository's git files.
pub async fn ingested_codeowners(pool: &PgPool, repo_id: i32) -> Result<Option<String>, String> {
    sqlx::query_scalar("SELECT contents FROM codeowners WHERE repo_id = $1 LIMIT 1")
        .bind(repo_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| err.to_string())
}

/// `AssignedOwners`/`AssignedTeams` keyed by path: the analytics indexer's
/// `.Match` only asks whether the owner list is non-empty.
pub fn path_set(paths: impl IntoIterator<Item = String>) -> HashMap<String, ()> {
    paths.into_iter().map(|path| (path, ())).collect()
}

/// `match[T]` from `internal/own/service.go`: append `assigned[path]` for the
/// full path, every ancestor, and finally the repo root `""`. Returns whether
/// any key in `paths` has a non-empty owner list.
pub fn assigned_match(paths: &HashMap<String, ()>, path: &str) -> bool {
    let mut matched = false;
    let mut path = path;
    let mut last_slash = Some(path.len());
    while let Some(index) = last_slash {
        path = &path[..index];
        if paths.contains_key(path) {
            matched = true;
        }
        last_slash = path.rfind('/');
    }
    if !path.is_empty() && paths.contains_key("") {
        matched = true;
    }
    matched
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assigned_match_walks_the_full_path_ancestors_and_root() {
        let mut paths = HashMap::new();
        paths.insert("a/b".to_owned(), ());
        assert!(assigned_match(&paths, "a/b"));
        assert!(assigned_match(&paths, "a/b/c"));
        assert!(assigned_match(&paths, "a/b/c/d.go"));
        assert!(!assigned_match(&paths, "a/c"));

        let mut root = HashMap::new();
        root.insert(String::new(), ());
        assert!(assigned_match(&root, "anything.go"));
    }
}
