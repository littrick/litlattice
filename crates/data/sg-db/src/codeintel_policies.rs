//! `internal/codeintel/policies/internal/store`: the queries the
//! `codeintel-policies-repository-matcher` job performs.
//!
//! Only the paths the job takes are ported:
//!
//! * `SelectPoliciesForRepositoryMembershipUpdate`: takes a batch of policies
//!   ordered by `last_resolved_at NULLS FIRST, id`, locks them and stamps
//!   `last_resolved_at`, returning the full rows;
//! * `UpdateReposMatchingPatterns`: recomputes the
//!   `lsif_configuration_policies_repository_pattern_lookup` membership of one
//!   policy (`repo.name LIKE` conditions, optional `LIMIT`), inserting new
//!   matches and deleting stale ones in a single statement.
//!
//! SQL is copied from `internal/codeintel/policies/internal/store/
//! repository_matches.go` and `util.go` (baseline `c864f15`). The lookup
//! table has no foreign keys and no `updated_at`, so there is nothing else to
//! maintain.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `shared.ConfigurationPolicy` (only the fields
/// `SelectPoliciesForRepositoryMembershipUpdate` returns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationPolicy {
    /// `lsif_configuration_policies.id` is `integer` (INT4).
    pub id: i32,
    pub repository_id: Option<i32>,
    /// `nil` when the column is NULL or an empty array
    /// (`optionalSlice`).
    pub repository_patterns: Option<Vec<String>>,
    pub name: Option<String>,
    pub policy_type: String,
    pub pattern: String,
    pub protected: bool,
    pub retention_enabled: bool,
    pub retention_duration_hours: Option<i32>,
    pub retain_intermediate_commits: bool,
    /// `ConfigurationPolicy.PreciseIndexingEnabled` (`indexing_enabled`).
    pub indexing_enabled: bool,
    pub syntactic_indexing_enabled: bool,
    pub index_commit_max_age_hours: Option<i32>,
    pub index_intermediate_commits: bool,
    pub embeddings_enabled: bool,
}

/// `Store.SelectPoliciesForRepositoryMembershipUpdate`.
pub async fn select_policies_for_repository_membership_update(
    pool: &PgPool,
    batch_size: i64,
    now: OffsetDateTime,
) -> Result<Vec<ConfigurationPolicy>, String> {
    // `sqlf.Sprintf(selectPoliciesForRepositoryMembershipUpdate, batchSize,
    // timeutil.Now())`: the batch size is inlined with `%d`, the timestamp is
    // bound.
    let query = SELECT_POLICIES_QUERY.replace("{batch_size}", &batch_size.to_string());
    let rows = sqlx::query(&query)
        .bind(now)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;

    rows.into_iter()
        .map(|row| {
            let repository_patterns: Option<Vec<String>> = row
                .try_get("repository_patterns")
                .map_err(|err| err.to_string())?;
            Ok(ConfigurationPolicy {
                id: row.try_get("id").map_err(|err| err.to_string())?,
                repository_id: row
                    .try_get("repository_id")
                    .map_err(|err| err.to_string())?,
                // `optionalSlice`: an empty array is `nil` too.
                repository_patterns: repository_patterns.filter(|values| !values.is_empty()),
                name: row.try_get("name").map_err(|err| err.to_string())?,
                policy_type: row.try_get("type").map_err(|err| err.to_string())?,
                pattern: row.try_get("pattern").map_err(|err| err.to_string())?,
                protected: row.try_get("protected").map_err(|err| err.to_string())?,
                retention_enabled: row
                    .try_get("retention_enabled")
                    .map_err(|err| err.to_string())?,
                retention_duration_hours: row
                    .try_get("retention_duration_hours")
                    .map_err(|err| err.to_string())?,
                retain_intermediate_commits: row
                    .try_get("retain_intermediate_commits")
                    .map_err(|err| err.to_string())?,
                indexing_enabled: row
                    .try_get("indexing_enabled")
                    .map_err(|err| err.to_string())?,
                syntactic_indexing_enabled: row
                    .try_get("syntactic_indexing_enabled")
                    .map_err(|err| err.to_string())?,
                index_commit_max_age_hours: row
                    .try_get("index_commit_max_age_hours")
                    .map_err(|err| err.to_string())?,
                index_intermediate_commits: row
                    .try_get("index_intermediate_commits")
                    .map_err(|err| err.to_string())?,
                embeddings_enabled: row
                    .try_get("embeddings_enabled")
                    .map_err(|err| err.to_string())?,
            })
        })
        .collect()
}

/// `Store.UpdateReposMatchingPatterns`: returns `(num_inserted, num_deleted)`
/// like the statement's final `SELECT` (the baseline ignores both).
pub async fn update_repos_matching_patterns(
    pool: &PgPool,
    patterns: &[String],
    policy_id: i32,
    repository_match_limit: Option<i64>,
) -> Result<(i64, i64), String> {
    // `makePatternCondition(patterns, false)`: no patterns matches NO
    // repositories (`FALSE`), and each pattern is lowercased with `*`
    // replaced by `%`, then bound as a `LIKE` operand.
    let (condition, mut binds) = pattern_condition(patterns);

    // `optionalLimit`: inlined with `%d`.
    let limit = match repository_match_limit {
        Some(limit) => format!("LIMIT {limit}"),
        None => String::new(),
    };

    let next = binds.len() + 1;
    let query = UPDATE_REPOS_MATCHING_PATTERNS_QUERY
        .replace("{condition}", &condition)
        .replace("{limit}", &limit)
        .replace("{policy_id_1}", &placeholder(next))
        .replace("{policy_id_2}", &placeholder(next + 1))
        .replace("{policy_id_3}", &placeholder(next + 2));

    let mut query = sqlx::query(&query);
    for bind in binds.drain(..) {
        query = query.bind(bind);
    }
    let row = query
        .bind(policy_id)
        .bind(policy_id)
        .bind(policy_id)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?;

    Ok((
        row.try_get("num_inserted").map_err(|err| err.to_string())?,
        row.try_get("num_deleted").map_err(|err| err.to_string())?,
    ))
}

/// `makePatternCondition` with `defaultValue=false`.
fn pattern_condition(patterns: &[String]) -> (String, Vec<String>) {
    if patterns.is_empty() {
        return ("FALSE".to_owned(), Vec::new());
    }

    let mut binds = Vec::with_capacity(patterns.len());
    let mut conditions = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        binds.push(pattern.to_lowercase().replace('*', "%"));
        conditions.push(format!("lower(name) LIKE {}", placeholder(binds.len())));
    }

    (conditions.join(" OR "), binds)
}

fn placeholder(index: usize) -> String {
    format!("${index}")
}

const SELECT_POLICIES_QUERY: &str = r#"
WITH
candidate_policies AS (
	SELECT p.id
	FROM lsif_configuration_policies p
	ORDER BY p.last_resolved_at NULLS FIRST, p.id
	LIMIT {batch_size}
),
locked_policies AS (
	SELECT c.id
	FROM candidate_policies c
	ORDER BY c.id FOR UPDATE
)
UPDATE lsif_configuration_policies
SET last_resolved_at = $1
WHERE id IN (SELECT id FROM locked_policies)
RETURNING
	id,
	repository_id,
	repository_patterns,
	name,
	type,
	pattern,
	protected,
	retention_enabled,
	retention_duration_hours,
	retain_intermediate_commits,
	indexing_enabled,
	syntactic_indexing_enabled,
	index_commit_max_age_hours,
	index_intermediate_commits,
	embeddings_enabled
"#;

const UPDATE_REPOS_MATCHING_PATTERNS_QUERY: &str = r#"
WITH
matching_repositories AS (
	SELECT id AS repo_id
	FROM repo
	WHERE
		({condition}) AND
		deleted_at IS NULL AND
		blocked IS NULL
	ORDER BY stars DESC NULLS LAST, id
	{limit}
),
inserted AS (
	-- Insert records that match the policy but don't yet exist
	INSERT INTO lsif_configuration_policies_repository_pattern_lookup(policy_id, repo_id)
	SELECT {policy_id_1}, r.repo_id
	FROM (
		SELECT r.repo_id
		FROM matching_repositories r
		WHERE r.repo_id NOT IN (
			SELECT repo_id
			FROM lsif_configuration_policies_repository_pattern_lookup
			WHERE policy_id = {policy_id_2}
		)
	) r
	ORDER BY r.repo_id
	RETURNING 1
),
locked_outdated_matching_repository_records AS (
	SELECT policy_id, repo_id
	FROM lsif_configuration_policies_repository_pattern_lookup
	WHERE
		policy_id = {policy_id_3} AND
		repo_id NOT IN (SELECT repo_id FROM matching_repositories)
	ORDER BY policy_id, repo_id FOR UPDATE
),
deleted AS (
	-- Delete records that no longer match the policy
	DELETE FROM lsif_configuration_policies_repository_pattern_lookup
	WHERE (policy_id, repo_id) IN (
		SELECT policy_id, repo_id
		FROM locked_outdated_matching_repository_records
	)
	RETURNING 1
)
SELECT
	(SELECT COUNT(*) FROM inserted) AS num_inserted,
	(SELECT COUNT(*) FROM deleted) AS num_deleted
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_condition_matches_the_baseline() {
        assert_eq!(pattern_condition(&[]), ("FALSE".to_owned(), vec![]));

        let (condition, binds) = pattern_condition(&["Darth*".to_owned(), "r1".to_owned()]);
        assert_eq!(
            condition,
            "lower(name) LIKE $1 OR lower(name) LIKE $2".to_owned()
        );
        assert_eq!(binds, vec!["darth%".to_owned(), "r1".to_owned()]);
    }
}
