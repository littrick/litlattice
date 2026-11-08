//! `internal/codeintel/reposcheduler/store.go`: the repository scheduling
//! queries used by the precise and syntactic indexing schedulers.
//!
//! [`RepoScanStore::get_repositories_for_index_scan`] returns (and stamps)
//! the repositories that should be considered for an index scan. The query is
//! a three-arm `UNION ALL`:
//!
//! 1. **global policies** — every cloned, live repository when a global
//!    (repository-less) policy with the enablement flag exists
//!    (`global_policy_descriptor` is a `MATERIALIZED` CTE so its single row is
//!    shared);
//! 2. **policies attached to the repository** (`p.repository_id = r.id`);
//! 3. **policies matched through the pattern lookup**
//!    (`lsif_configuration_policies_repository_pattern_lookup`).
//!
//! `last_changed` is only non-NULL for policies that index `HEAD`, so a
//! repository is re-scanned after an update only when a HEAD policy applies.
//! The scan row is upserted into the layout's `last_index_scan_*` table, which
//! is what keeps the same repository out of later scans for
//! `process_delay` seconds.
//!
//! The SQL is copied verbatim from the baseline (`c864f15`); like `sqlf`,
//! every non-query argument is a bind parameter, so the five values in the
//! final query are `$1`–`$5` in source order. `Transact` (which drops the
//! layout in the baseline) is not ported because the scheduler never opens a
//! transaction around this query.

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

/// `reposcheduler.RepositoryBatchOptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryBatchOptions {
    /// Time to wait before making a repository visible again after its
    /// previous scan (superseded when the repository changed meanwhile).
    pub process_delay: Duration,
    /// Whether policies without a repository id or patterns apply to every
    /// repository.
    pub allow_global_policies: bool,
    /// Optional cap on the repositories matched through the global policy.
    pub global_policy_repositories_match_limit: Option<i64>,
    /// The maximum number of repositories returned in a batch.
    pub limit: i64,
}

/// The two column/table names that distinguish the precise store from the
/// syntactic store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepoScanLayout {
    pub policy_enablement_field: &'static str,
    pub last_scan_table: &'static str,
}

/// `reposcheduler.NewPreciseStore`'s layout.
pub const PRECISE_LAYOUT: RepoScanLayout = RepoScanLayout {
    policy_enablement_field: "indexing_enabled",
    last_scan_table: "lsif_last_index_scan",
};

/// `reposcheduler.NewSyntacticStore`'s layout.
pub const SYNTACTIC_LAYOUT: RepoScanLayout = RepoScanLayout {
    policy_enablement_field: "syntactic_indexing_enabled",
    last_scan_table: "syntactic_scip_last_index_scan",
};

/// `reposcheduler.store` with the layout it was built from.
#[derive(Clone)]
pub struct RepoScanStore {
    pool: PgPool,
    layout: RepoScanLayout,
}

impl RepoScanStore {
    /// `reposcheduler.NewSyntacticStore`.
    pub fn syntactic(pool: PgPool) -> Self {
        Self::new(pool, SYNTACTIC_LAYOUT)
    }

    /// `reposcheduler.NewPreciseStore`.
    pub fn precise(pool: PgPool) -> Self {
        Self::new(pool, PRECISE_LAYOUT)
    }

    pub fn new(pool: PgPool, layout: RepoScanLayout) -> Self {
        Self { pool, layout }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `store.GetRepositoriesForIndexScan`: the repositories returned by the
    /// upsert's `RETURNING repository_id`, in the database's order.
    pub async fn get_repositories_for_index_scan(
        &self,
        options: &RepositoryBatchOptions,
        now: OffsetDateTime,
    ) -> Result<Vec<i32>, String> {
        let layout = self.layout;
        let mut binds: Vec<Bind> = Vec::new();

        let mut arms: Vec<String> = Vec::new();
        if options.allow_global_policies {
            // `optionalLimit`: `LIMIT %d`, with the limit bound like any
            // other non-query argument.
            let limit = match options.global_policy_repositories_match_limit {
                Some(limit) => {
                    binds.push(Bind::Int(limit));
                    format!("LIMIT ${}", binds.len())
                }
                None => String::new(),
            };
            arms.push(format!(
                "({})",
                GLOBAL_REPOSITORIES_QUERY.replace("{limit}", &limit)
            ));
        }
        arms.push(format!(
            "({})",
            REPOSITORIES_WITH_POLICY_QUERY.replace("{enablement}", layout.policy_enablement_field)
        ));
        arms.push(format!(
            "({})",
            REPOSITORIES_WITH_POLICY_VIA_PATTERN_QUERY
                .replace("{enablement}", layout.policy_enablement_field)
        ));

        // The final query binds, in source order: the three occurrences of
        // `now` and the process delay (seconds) around `repositories`, then
        // the batch limit, then the two `now` occurrences in the insert.
        binds.push(Bind::Timestamp(now));
        let now_before = binds.len();
        // `int(batchOptions.ProcessDelay / time.Second)`: whole seconds.
        let process_delay_secs = i64::try_from(options.process_delay.as_secs()).unwrap_or(i64::MAX);
        binds.push(Bind::Int(process_delay_secs));
        let delay = binds.len();
        binds.push(Bind::Int(options.limit));
        let limit = binds.len();
        binds.push(Bind::Timestamp(now));
        let insert_now = binds.len();
        binds.push(Bind::Timestamp(now));
        let conflict_now = binds.len();

        let query = get_repositories_for_index_scan_query(layout)
            .replace("{arms}", &arms.join(" UNION ALL "))
            .replace("{last_scan_table}", layout.last_scan_table)
            .replace("{now_before}", &format!("${now_before}"))
            .replace("{delay}", &format!("${delay}"))
            .replace("{limit}", &format!("${limit}"))
            .replace("{insert_now}", &format!("${insert_now}"))
            .replace("{conflict_now}", &format!("${conflict_now}"));

        let mut query = sqlx::query(&query);
        for bind in &binds {
            query = match bind {
                Bind::Int(value) => query.bind(*value),
                Bind::Timestamp(value) => query.bind(*value),
            };
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| err.to_string())?;

        rows.iter()
            .map(|row| {
                use sqlx::Row;
                row.try_get::<i32, _>(0).map_err(|err| err.to_string())
            })
            .collect()
    }
}

/// A bound parameter, typed like the Go value `sqlf` binds.
enum Bind {
    Int(i64),
    Timestamp(OffsetDateTime),
}

fn get_repositories_for_index_scan_query(layout: RepoScanLayout) -> String {
    format!(
        r#"
WITH
-- This CTE will contain a single row if there is at least one global policy, and will return an empty
-- result set otherwise. If any global policy is for HEAD, the value for the column is_head_policy will
-- be true.
global_policy_descriptor AS MATERIALIZED (
	SELECT (p.type = 'GIT_COMMIT' AND p.pattern = 'HEAD') AS is_head_policy
	FROM lsif_configuration_policies p
	WHERE
		p.{enablement} AND
		p.repository_id IS NULL AND
		p.repository_patterns IS NULL
	ORDER BY is_head_policy DESC
	LIMIT 1
),
repositories_matching_policy AS (
	{{arms}}
),
repositories AS (
	SELECT rmp.id
	FROM repositories_matching_policy rmp
	LEFT JOIN {last_scan_table} lrs ON lrs.repository_id = rmp.id
	WHERE
		-- Records that have not been checked within the global reindex threshold are also eligible for
		-- indexing. Note that condition here is true for a record that has never been indexed.

		({{now_before}} - lrs.last_index_scan_at > ({{delay}} * '1 second'::interval)) IS DISTINCT FROM FALSE OR

		-- Records that have received an update since their last scan are also eligible for re-indexing.
		-- Note that last_changed is NULL unless the repository is attached to a policy for HEAD.

		(rmp.last_changed > lrs.last_index_scan_at)
	ORDER BY
		lrs.last_index_scan_at NULLS FIRST,
		rmp.id -- tie breaker
	LIMIT {{limit}}
)
INSERT INTO {last_scan_table} (repository_id, last_index_scan_at)
SELECT DISTINCT r.id, {{insert_now}}::timestamp FROM repositories r
ON CONFLICT (repository_id) DO UPDATE
SET last_index_scan_at = {{conflict_now}}
RETURNING repository_id
"#,
        enablement = layout.policy_enablement_field,
        last_scan_table = layout.last_scan_table,
    )
}

const GLOBAL_REPOSITORIES_QUERY: &str = r#"
SELECT
	r.id,
	CASE
		-- Return non-NULL last_changed only for policies that are attached to a HEAD commit.
		-- We don't want to superfluously return the same repos because they had an update, but
		-- we only (for example) index a branch that doesn't have many active commits.
		WHEN gpd.is_head_policy THEN gr.last_changed
		ELSE NULL
	END AS last_changed
FROM repo r
JOIN gitserver_repos gr ON gr.repo_id = r.id
JOIN global_policy_descriptor gpd ON TRUE
WHERE
	r.deleted_at IS NULL AND
	r.blocked IS NULL AND
	gr.clone_status = 'cloned'
ORDER BY stars DESC NULLS LAST, id
{limit}
"#;

const REPOSITORIES_WITH_POLICY_QUERY: &str = r#"
SELECT
	r.id,
	CASE
		-- Return non-NULL last_changed only for policies that are attached to a HEAD commit.
		-- We don't want to superfluously return the same repos because they had an update, but
		-- we only (for example) index a branch that doesn't have many active commits.
		WHEN p.type = 'GIT_COMMIT' AND p.pattern = 'HEAD' THEN gr.last_changed
		ELSE NULL
	END AS last_changed
FROM repo r
JOIN gitserver_repos gr ON gr.repo_id = r.id
JOIN lsif_configuration_policies p ON p.repository_id = r.id
WHERE
	r.deleted_at IS NULL AND
	r.blocked IS NULL AND
	p.{enablement} AND
	gr.clone_status = 'cloned'
"#;

const REPOSITORIES_WITH_POLICY_VIA_PATTERN_QUERY: &str = r#"
SELECT
	r.id,
	CASE
		-- Return non-NULL last_changed only for policies that are attached to a HEAD commit.
		-- We don't want to superfluously return the same repos because they had an update, but
		-- we only (for example) index a branch that doesn't have many active commits.
		WHEN p.type = 'GIT_COMMIT' AND p.pattern = 'HEAD' THEN gr.last_changed
		ELSE NULL
	END AS last_changed
FROM repo r
JOIN gitserver_repos gr ON gr.repo_id = r.id
JOIN lsif_configuration_policies_repository_pattern_lookup rpl ON rpl.repo_id = r.id
JOIN lsif_configuration_policies p ON p.id = rpl.policy_id
WHERE
	r.deleted_at IS NULL AND
	r.blocked IS NULL AND
	p.{enablement} AND
	gr.clone_status = 'cloned'
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntactic_layout_matches_the_baseline() {
        assert_eq!(
            SYNTACTIC_LAYOUT.policy_enablement_field,
            "syntactic_indexing_enabled"
        );
        assert_eq!(
            SYNTACTIC_LAYOUT.last_scan_table,
            "syntactic_scip_last_index_scan"
        );
        assert_eq!(PRECISE_LAYOUT.policy_enablement_field, "indexing_enabled");
        assert_eq!(PRECISE_LAYOUT.last_scan_table, "lsif_last_index_scan");
    }

    #[test]
    fn query_uses_the_layout_columns_and_all_placeholders() {
        let query = get_repositories_for_index_scan_query(SYNTACTIC_LAYOUT);
        assert!(query.contains("p.syntactic_indexing_enabled"));
        assert!(query.contains("LEFT JOIN syntactic_scip_last_index_scan lrs"));
        assert!(query.contains("INSERT INTO syntactic_scip_last_index_scan"));
        assert!(query.contains("{arms}"));
        for placeholder in [
            "{now_before}",
            "{delay}",
            "{limit}",
            "{insert_now}",
            "{conflict_now}",
        ] {
            assert!(query.contains(placeholder), "missing {placeholder}");
        }
    }
}
