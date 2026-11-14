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

    rows.iter()
        .map(scan_configuration_policy)
        .collect::<Result<_, String>>()
}

/// `scanConfigurationPolicy`: every query in this module selects the same
/// columns.
fn scan_configuration_policy(row: &sqlx::postgres::PgRow) -> Result<ConfigurationPolicy, String> {
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
}

/// `shared.GetConfigurationPoliciesOptions`: the filter set of
/// `Store.GetConfigurationPolicies`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetConfigurationPoliciesOptions {
    /// `0` disables the repository filter.
    pub repository_id: i32,
    pub term: String,
    pub protected: Option<bool>,
    pub for_data_retention: Option<bool>,
    pub for_precise_indexing: Option<bool>,
    pub for_syntactic_indexing: Option<bool>,
    pub for_embeddings: Option<bool>,
    pub limit: i64,
    pub offset: i64,
}

/// `Store.GetConfigurationPolicies`: returns `(policies, totalCount)`.
///
/// The conditions, the `LEFT JOIN repo`, the `ORDER BY p.name` and the
/// unbounded tie-break behaviour are copied from
/// `internal/codeintel/policies/internal/store/configurations.go`. The select
/// and count queries run in one transaction (one snapshot), like the
/// baseline's `WithTransact`.
pub async fn get_configuration_policies(
    pool: &PgPool,
    opts: &GetConfigurationPoliciesOptions,
) -> Result<(Vec<ConfigurationPolicy>, i64), String> {
    let (condition, binds) = configuration_policy_condition(opts);

    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let count_query = format!(
        "SELECT COUNT(*)\nFROM lsif_configuration_policies p\nLEFT JOIN repo ON repo.id = p.repository_id\nWHERE {condition}"
    );
    let mut count = sqlx::query(&count_query);
    for bind in &binds {
        count = bind_value(count, bind.clone());
    }
    let row = count
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    let total_count: i64 = row.try_get(0).map_err(|err| err.to_string())?;

    // `getConfigurationPoliciesLimitedQuery`: LIMIT/OFFSET are inlined like
    // sqlf's integer parameters.
    let select_query = format!(
        "{GET_CONFIGURATION_POLICIES_SELECT}WHERE {condition}\nORDER BY p.name\nLIMIT {} OFFSET {}",
        opts.limit, opts.offset
    );
    let mut select = sqlx::query(&select_query);
    for bind in &binds {
        select = bind_value(select, bind.clone());
    }
    let rows = select
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    let policies = rows
        .iter()
        .map(scan_configuration_policy)
        .collect::<Result<Vec<_>, String>>()?;

    tx.commit().await.map_err(|err| err.to_string())?;
    Ok((policies, total_count))
}

/// A bound parameter, typed like the Go value `sqlf` binds: the repository id
/// is an `int` (compared against `integer` columns), the search term a
/// string.
#[derive(Debug, Clone)]
enum BindValue {
    Int(i32),
    Text(String),
}

fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: BindValue,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        BindValue::Int(value) => query.bind(value),
        BindValue::Text(value) => query.bind(value),
    }
}

/// `makeConfigurationPolicySearchCondition` + the option conditions of
/// `GetConfigurationPolicies`. Only the repository id and the search term are
/// bound; the boolean filters are inlined like the baseline.
fn configuration_policy_condition(
    opts: &GetConfigurationPoliciesOptions,
) -> (String, Vec<BindValue>) {
    let mut conds = Vec::new();
    let mut binds = Vec::new();

    if opts.repository_id != 0 {
        let first = binds.len() + 1;
        binds.push(BindValue::Int(opts.repository_id));
        let second = binds.len() + 1;
        binds.push(BindValue::Int(opts.repository_id));
        conds.push(format!(
            r#"(
			(p.repository_id IS NULL AND p.repository_patterns IS NULL) OR
			p.repository_id = ${first} OR
			EXISTS (
				SELECT 1
				FROM lsif_configuration_policies_repository_pattern_lookup l
				WHERE l.policy_id = p.id AND l.repo_id = ${second}
			)
		)"#
        ));
    }
    if !opts.term.is_empty() {
        let placeholder = binds.len() + 1;
        binds.push(BindValue::Text(format!("%{}%", opts.term)));
        conds.push(format!("(p.name ILIKE ${placeholder})"));
    }
    for (value, column) in [
        (opts.protected, "p.protected"),
        (opts.for_data_retention, "p.retention_enabled"),
        (opts.for_precise_indexing, "p.indexing_enabled"),
        (opts.for_syntactic_indexing, "p.syntactic_indexing_enabled"),
        (opts.for_embeddings, "p.embeddings_enabled"),
    ] {
        match value {
            Some(true) => conds.push(column.to_owned()),
            Some(false) => conds.push(format!("NOT {column}")),
            None => {}
        }
    }

    if conds.is_empty() {
        ("TRUE".to_owned(), binds)
    } else {
        (conds.join(" AND "), binds)
    }
}

const GET_CONFIGURATION_POLICIES_SELECT: &str = r#"SELECT
	p.id,
	p.repository_id,
	p.repository_patterns,
	p.name,
	p.type,
	p.pattern,
	p.protected,
	p.retention_enabled,
	p.retention_duration_hours,
	p.retain_intermediate_commits,
	p.indexing_enabled,
	p.syntactic_indexing_enabled,
	p.index_commit_max_age_hours,
	p.index_intermediate_commits,
	p.embeddings_enabled
FROM lsif_configuration_policies p
LEFT JOIN repo ON repo.id = p.repository_id
"#;

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

//
// M9.28c: GraphQL-facing queries of the configuration-policy store.
//
// Ported from `internal/codeintel/policies/internal/store/configurations.go`
// (`GetConfigurationPolicyByID`, `CreateConfigurationPolicy`,
// `UpdateConfigurationPolicy`, `DeleteConfigurationPolicyByID`) and
// `repository_matches.go` (`GetRepoIDsByGlobPatterns`), plus `RepoCount`
// (`global_metadata.go`) and the repository row lookup the previews need.
//
// `database.AuthzQueryConds` is reproduced as the internal-actor predicate
// (`TRUE`); request-actor repository permissions are a documented gap in
// `m9_28c_codeintel_policies_overview.md`.
//

/// `Store.CreateConfigurationPolicy` input (`shared.ConfigurationPolicy`,
/// excluding the id/protected fields the insert generates).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewConfigurationPolicy {
    pub repository_id: Option<i32>,
    pub repository_patterns: Option<Vec<String>>,
    pub name: String,
    pub policy_type: String,
    pub pattern: String,
    pub retention_enabled: bool,
    /// `optionalNumHours(policy.RetentionDuration)`: already whole hours.
    pub retention_duration_hours: Option<i32>,
    pub retain_intermediate_commits: bool,
    pub indexing_enabled: bool,
    pub syntactic_indexing_enabled: bool,
    pub index_commit_max_age_hours: Option<i32>,
    pub index_intermediate_commits: bool,
    pub embeddings_enabled: bool,
}

/// `Store.UpdateConfigurationPolicy` input (`shared.ConfigurationPolicyPatch`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationPolicyPatch {
    pub id: i32,
    /// `*[]string`: `None` is SQL NULL, `Some(vec![])` is `{}`.
    pub repository_patterns: Option<Vec<String>>,
    pub name: String,
    pub policy_type: String,
    pub pattern: String,
    pub retention_enabled: bool,
    pub retention_duration_hours: Option<i32>,
    pub retain_intermediate_commits: bool,
    pub indexing_enabled: bool,
    /// `None` keeps the current column value.
    pub syntactic_indexing_enabled: Option<bool>,
    pub index_commit_max_age_hours: Option<i32>,
    pub index_intermediate_commits: bool,
    pub embeddings_enabled: bool,
}

/// Exported error texts of the store, verbatim.
pub const UNKNOWN_CONFIGURATION_POLICY: &str = "unknown configuration policy";
pub const ILLEGAL_CONFIGURATION_POLICY_UPDATE: &str = "protected configuration policies must keep the same names, types, patterns, and retention values (except duration)";
pub const ILLEGAL_CONFIGURATION_POLICY_DELETE: &str =
    "protected configuration policies cannot be deleted";

/// `Store.GetConfigurationPolicyByID`; `None` mirrors the `!exists` return.
pub async fn get_configuration_policy_by_id(
    pool: &PgPool,
    id: i32,
    authz: &str,
) -> Result<Option<ConfigurationPolicy>, String> {
    // `p.repository_id IS NULL OR (authz)`: global policies are visible to
    // everyone; repository-specific policies require repository access.
    let query = format!(
        "{GET_CONFIGURATION_POLICIES_SELECT}WHERE p.id = $1 AND (p.repository_id IS NULL OR ({authz}))"
    );
    let row = sqlx::query(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|err| err.to_string())?;
    match row {
        Some(row) => scan_configuration_policy(&row).map(Some),
        None => Ok(None),
    }
}

/// `Store.CreateConfigurationPolicy`: inserts and returns the hydrated row.
pub async fn create_configuration_policy(
    pool: &PgPool,
    policy: &NewConfigurationPolicy,
) -> Result<ConfigurationPolicy, String> {
    let row = sqlx::query(CREATE_CONFIGURATION_POLICY_QUERY)
        .bind(policy.repository_id)
        .bind(policy.repository_patterns.clone())
        .bind(&policy.name)
        .bind(&policy.policy_type)
        .bind(&policy.pattern)
        .bind(policy.retention_enabled)
        .bind(policy.retention_duration_hours)
        .bind(policy.retain_intermediate_commits)
        .bind(policy.indexing_enabled)
        .bind(policy.syntactic_indexing_enabled)
        .bind(policy.index_commit_max_age_hours)
        .bind(policy.index_intermediate_commits)
        .bind(policy.embeddings_enabled)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?;
    scan_configuration_policy(&row)
}

/// `Store.UpdateConfigurationPolicy`.
pub async fn update_configuration_policy(
    pool: &PgPool,
    patch: &ConfigurationPolicyPatch,
) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let row = sqlx::query(UPDATE_CONFIGURATION_POLICY_SELECT_QUERY)
        .bind(patch.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    let current = match row {
        Some(row) => scan_configuration_policy(&row)?,
        None => {
            return Err(UNKNOWN_CONFIGURATION_POLICY.to_owned());
        }
    };

    if current.protected
        && (patch.name != current.name.as_deref().unwrap_or_default()
            || patch.policy_type != current.policy_type
            || patch.pattern != current.pattern
            || patch.retention_enabled != current.retention_enabled
            || patch.retain_intermediate_commits != current.retain_intermediate_commits)
    {
        return Err(ILLEGAL_CONFIGURATION_POLICY_UPDATE.to_owned());
    }

    let syntactic = patch
        .syntactic_indexing_enabled
        .unwrap_or(current.syntactic_indexing_enabled);

    sqlx::query(UPDATE_CONFIGURATION_POLICY_QUERY)
        .bind(&patch.name)
        .bind(patch.repository_patterns.clone())
        .bind(&patch.policy_type)
        .bind(&patch.pattern)
        .bind(patch.retention_enabled)
        .bind(patch.retention_duration_hours)
        .bind(patch.retain_intermediate_commits)
        .bind(patch.indexing_enabled)
        .bind(syntactic)
        .bind(patch.index_commit_max_age_hours)
        .bind(patch.index_intermediate_commits)
        .bind(patch.embeddings_enabled)
        .bind(patch.id)
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;

    tx.commit().await.map_err(|err| err.to_string())
}

/// `Store.DeleteConfigurationPolicyByID`.
pub async fn delete_configuration_policy_by_id(pool: &PgPool, id: i32) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let protected: Option<bool> = sqlx::query_scalar(DELETE_CONFIGURATION_POLICY_BY_ID_QUERY)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    match protected {
        None => return Err(UNKNOWN_CONFIGURATION_POLICY.to_owned()),
        Some(true) => return Err(ILLEGAL_CONFIGURATION_POLICY_DELETE.to_owned()),
        Some(false) => {}
    }

    sqlx::query(DELETE_CONFIGURATION_POLICIES_REPOSITORY_PATTERN_LOOKUP)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;

    tx.commit().await.map_err(|err| err.to_string())
}

/// `Store.GetRepoIDsByGlobPatterns`: returns `(ids, totalCount)`. The
/// repository `authz` predicate is applied to both the count and the page.
pub async fn get_repo_ids_by_glob_patterns(
    pool: &PgPool,
    patterns: &[String],
    limit: i64,
    offset: i64,
    authz: &str,
) -> Result<(Vec<i32>, i64), String> {
    if patterns.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let (condition, binds) = pattern_condition(patterns);

    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let count_query = format!(
        "SELECT COUNT(*)\nFROM repo\nWHERE\n\t({condition}) AND\n\tdeleted_at IS NULL AND\n\tblocked IS NULL AND\n\t({authz})"
    );
    let mut count = sqlx::query(&count_query);
    for bind in &binds {
        count = count.bind(bind.clone());
    }
    let row = count
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    let total_count: i64 = row.try_get(0).map_err(|err| err.to_string())?;

    let select_query = format!(
        "SELECT id\nFROM repo\nWHERE\n\t({condition}) AND\n\tdeleted_at IS NULL AND\n\tblocked IS NULL AND\n\t({authz})\nORDER BY stars DESC NULLS LAST, id\nLIMIT {limit}\nOFFSET {offset}"
    );
    let mut select = sqlx::query(&select_query);
    for bind in &binds {
        select = select.bind(bind.clone());
    }
    let rows = select
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    let ids = rows
        .iter()
        .map(|row| row.try_get::<i32, _>(0).map_err(|err| err.to_string()))
        .collect::<Result<Vec<_>, String>>()?;

    tx.commit().await.map_err(|err| err.to_string())?;
    Ok((ids, total_count))
}

/// `Store.RepoCount`: `SELECT SUM(total) FROM repo_statistics`. An empty
/// table scans NULL and errors, like the baseline's `ScanFirstInt`.
pub async fn repo_count(pool: &PgPool) -> Result<i64, String> {
    let total: Option<i64> = sqlx::query_scalar("SELECT SUM(total)::bigint FROM repo_statistics")
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?;
    total.ok_or_else(|| {
        "sql: Scan error on column index 0, name \"sum\": converting NULL to int is unsupported"
            .to_owned()
    })
}

/// A repository row the previews need (`repos.Get` semantics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRow {
    pub id: i32,
    pub name: String,
}

/// `repoStore.Get`: excludes deleted repositories, then surfaces the blocked
/// error. Errors are verbatim (`RepoNotFoundErr`, `BlockedRepoError`).
pub async fn get_repository(pool: &PgPool, id: i32) -> Result<RepositoryRow, String> {
    let row = sqlx::query("SELECT id, name, blocked FROM repo WHERE id = $1 AND deleted_at IS NULL")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|err| err.to_string())?;
    let row = match row {
        Some(row) => row,
        None => return Err(format!("repo not found: id={id}")),
    };
    let name: String = row.try_get("name").map_err(|err| err.to_string())?;
    let blocked: Option<serde_json::Value> = row.try_get("blocked").map_err(|err| err.to_string())?;
    if let Some(blocked) = blocked {
        let reason = blocked
            .get("reason")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        return Err(format!(
            "repository {name} has been blocked. reason: {reason}"
        ));
    }
    let id: i32 = row.try_get("id").map_err(|err| err.to_string())?;
    Ok(RepositoryRow { id, name })
}

/// `gitresolvers.NewRepositoryFromID` node lookup (id + name only).
pub async fn get_repository_names(
    pool: &PgPool,
    ids: &[i32],
) -> Result<Vec<RepositoryRow>, String> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query("SELECT id, name FROM repo WHERE id = ANY($1)")
        .bind(ids)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    let mut by_id = std::collections::HashMap::new();
    for row in &rows {
        by_id.insert(
            row.try_get::<i32, _>("id").map_err(|err| err.to_string())?,
            row.try_get::<String, _>("name")
                .map_err(|err| err.to_string())?,
        );
    }
    Ok(ids
        .iter()
        .filter_map(|id| {
            by_id
                .get(id)
                .map(|name| RepositoryRow { id: *id, name: name.clone() })
        })
        .collect())
}

const CREATE_CONFIGURATION_POLICY_QUERY: &str = r#"
INSERT INTO lsif_configuration_policies (
	repository_id,
	repository_patterns,
	name,
	type,
	pattern,
	retention_enabled,
	retention_duration_hours,
	retain_intermediate_commits,
	indexing_enabled,
	syntactic_indexing_enabled,
	index_commit_max_age_hours,
	index_intermediate_commits,
	embeddings_enabled
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
RETURNING
	id,
	repository_id,
	repository_patterns,
	name,
	type,
	pattern,
	false as protected,
	retention_enabled,
	retention_duration_hours,
	retain_intermediate_commits,
	indexing_enabled,
	syntactic_indexing_enabled,
	index_commit_max_age_hours,
	index_intermediate_commits,
	embeddings_enabled
"#;

const UPDATE_CONFIGURATION_POLICY_SELECT_QUERY: &str = r#"
SELECT
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
FROM lsif_configuration_policies
WHERE id = $1
FOR UPDATE
"#;

const UPDATE_CONFIGURATION_POLICY_QUERY: &str = r#"
UPDATE lsif_configuration_policies p SET
	name = $1,
	repository_patterns = $2,
	type = $3,
	pattern = $4,
	retention_enabled = $5,
	retention_duration_hours = $6,
	retain_intermediate_commits = $7,
	indexing_enabled = $8,
	syntactic_indexing_enabled = $9,
	index_commit_max_age_hours = $10,
	index_intermediate_commits = $11,
	embeddings_enabled = $12
WHERE id = $13
"#;

const DELETE_CONFIGURATION_POLICY_BY_ID_QUERY: &str = r#"
WITH
candidate AS (
	SELECT id, protected
	FROM lsif_configuration_policies
	WHERE id = $1
	ORDER BY id FOR UPDATE
),
deleted AS (
	DELETE FROM lsif_configuration_policies WHERE id IN (SELECT id FROM candidate WHERE NOT protected)
)
SELECT protected FROM candidate
"#;

const DELETE_CONFIGURATION_POLICIES_REPOSITORY_PATTERN_LOOKUP: &str =
    "DELETE FROM lsif_configuration_policies_repository_pattern_lookup WHERE policy_id = $1";
