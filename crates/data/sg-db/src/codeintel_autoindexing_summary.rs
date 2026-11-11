#![forbid(unsafe_code)]

//! Store operations used by the `codeintel-autoindexing-summary-builder`
//! worker job.
//!
//! The baseline routine lives in
//! `internal/codeintel/autoindexing/internal/background/summary/job_summary_builder.go`
//! and reads/writes:
//!
//! * `store.TopRepositoriesToConfigure` (`internal/codeintel/autoindexing/internal/store/coverage.go`):
//!   the repositories with the most `event_logs` entries in the five
//!   `codeintel.search*` names over the last 30 days, excluding deleted and
//!   blocked repositories;
//! * `uploads.Service.GetRecentUploadsSummary` and
//!   `GetRecentAutoIndexJobsSummary` (`internal/codeintel/uploads/internal/store/summary.go`):
//!   the `latest completed + still-in-flight` rows per `(root, indexer)`;
//! * `store.SetConfigurationSummary`: `cached_available_indexers` upsert;
//! * `store.TruncateConfigurationSummary`: delete every row outside the top
//!   `num_events` window.
//!
//! `SetConfigurationSummary` marshals `map[string]AvailableIndexer{}` to `{}`
//! when `codeIntelAutoIndexing.enabled` is false, which is the only path this
//! port implements (inference is not ported; see `06_worker.md` §34).
//!
//! The two recent-summary queries return the full baseline row set; only the
//! `(id, root, indexer)` columns are selected because that is all the routine
//! consumes (the blocklist is keyed by `(indexer, root)`). The `id` is widened
//! to `bigint` so both summaries scan into the same Rust type.

use sqlx::postgres::PgPool;
use sqlx::Row;

/// `eventLogNames`: the search events that mark a repository as active.
pub const EVENT_LOG_NAMES: [&str; 5] = [
    "codeintel.searchDefinitions.xrepo",
    "codeintel.searchDefinitions",
    "codeintel.searchHover",
    "codeintel.searchReferences.xrepo",
    "codeintel.searchReferences",
];

/// `eventLogsWindow / time.Hour`: "about one month", passed to the query as a
/// number of hours (`time.Hour * 24 * 30`).
pub const EVENT_LOGS_WINDOW_HOURS: i64 = 24 * 30;

/// `uploads/shared.RepositoryWithCount`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryWithCount {
    pub repository_id: i32,
    pub count: i64,
}

/// One `(root, indexer)` group of the recent uploads / auto-index jobs
/// summary; `ids` are the record identifiers the baseline's
/// `UploadsWithRepositoryNamespace.Uploads` / `GroupedAutoIndexJobs.Indexes`
/// contain, in query order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceGroup {
    pub root: String,
    pub indexer: String,
    pub ids: Vec<i64>,
}

/// `TOP_REPOSITORIES_TO_CONFIGURE_QUERY`. The baseline binds `pq.Array(eventLogNames)`
/// and `eventLogsWindow/time.Hour`; both are code constants, so they are
/// inlined here unchanged.
const TOP_REPOSITORIES_TO_CONFIGURE_QUERY: &str = r#"
SELECT
	r.id,
	COUNT(*) as num_events
FROM event_logs e
JOIN repo r ON r.id = (e.argument->'repositoryId')::integer
WHERE
	e.name = ANY(ARRAY['codeintel.searchDefinitions.xrepo', 'codeintel.searchDefinitions', 'codeintel.searchHover', 'codeintel.searchReferences.xrepo', 'codeintel.searchReferences']) AND
	e.timestamp >= NOW() - (720 * '1 hour'::interval) AND
	r.deleted_at IS NULL AND
	r.blocked IS NULL
GROUP BY r.id
ORDER BY num_events DESC, id
LIMIT $1
"#;

/// `recentUploadsSummaryQuery` (`sanitizedIndexerExpression` inlined).
const RECENT_UPLOADS_SUMMARY_QUERY: &str = r#"
WITH ranked_completed AS (
	SELECT
		u.id,
		u.root,
		u.indexer,
		u.finished_at,
		RANK() OVER (PARTITION BY root, (
			split_part(
				split_part(
					CASE
						-- Strip sourcegraph/ prefix if it exists
						WHEN strpos(indexer, 'sourcegraph/') = 1 THEN substr(indexer, length('sourcegraph/') + 1)
						ELSE indexer
					END,
				'@', 1), -- strip off @sha256:...
			':', 1) -- strip off tag
		) ORDER BY finished_at DESC) AS rank
	FROM lsif_uploads u
	WHERE
		u.repository_id = $1 AND
		u.state NOT IN ('uploading', 'queued', 'processing', 'deleted')
),
latest_uploads AS (
	SELECT u.id, u.root, u.indexer, u.uploaded_at
	FROM lsif_uploads u
	WHERE
		u.id IN (
			SELECT rc.id
			FROM ranked_completed rc
			WHERE rc.rank = 1
		)
	ORDER BY u.root, u.indexer
),
new_uploads AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE
		u.repository_id = $1 AND
		u.state IN ('uploading', 'queued', 'processing') AND
		u.uploaded_at >= (
			SELECT lu.uploaded_at
			FROM latest_uploads lu
			WHERE
				lu.root = u.root AND
				lu.indexer = u.indexer
			-- condition passes when latest_uploads is empty
			UNION SELECT u.queued_at LIMIT 1
		)
)
SELECT
	u.id::bigint AS id,
	u.root,
	u.indexer
FROM lsif_uploads_with_repository_name u
WHERE u.id IN (
	SELECT lu.id FROM latest_uploads lu
	UNION
	SELECT nu.id FROM new_uploads nu
)
ORDER BY u.root, u.indexer
"#;

/// `recentIndexesSummaryQuery` (`sanitizedIndexerExpression` inlined).
const RECENT_INDEXES_SUMMARY_QUERY: &str = r#"
WITH ranked_completed AS (
	SELECT
		u.id,
		u.root,
		u.indexer,
		u.finished_at,
		RANK() OVER (PARTITION BY root, (
			split_part(
				split_part(
					CASE
						-- Strip sourcegraph/ prefix if it exists
						WHEN strpos(indexer, 'sourcegraph/') = 1 THEN substr(indexer, length('sourcegraph/') + 1)
						ELSE indexer
					END,
				'@', 1), -- strip off @sha256:...
			':', 1) -- strip off tag
		) ORDER BY finished_at DESC) AS rank
	FROM lsif_indexes u
	WHERE
		u.repository_id = $1 AND
		u.state NOT IN ('queued', 'processing', 'deleted')
),
latest_indexes AS (
	SELECT u.id, u.root, u.indexer, u.queued_at
	FROM lsif_indexes u
	WHERE
		u.id IN (
			SELECT rc.id
			FROM ranked_completed rc
			WHERE rc.rank = 1
		)
	ORDER BY u.root, u.indexer
),
new_indexes AS (
	SELECT u.id
	FROM lsif_indexes u
	WHERE
		u.repository_id = $1 AND
		u.state IN ('queued', 'processing') AND
		u.queued_at >= (
			SELECT lu.queued_at
			FROM latest_indexes lu
			WHERE
				lu.root = u.root AND
				lu.indexer = u.indexer
			-- condition passes when latest_indexes is empty
			UNION SELECT u.queued_at LIMIT 1
		)
)
SELECT
	u.id::bigint AS id,
	u.root,
	u.indexer
FROM lsif_indexes_with_repository_name u
WHERE u.id IN (
	SELECT lu.id FROM latest_indexes lu
	UNION
	SELECT nu.id FROM new_indexes nu
)
ORDER BY u.root, u.indexer
"#;

/// `setConfigurationSummaryQuery`.
const SET_CONFIGURATION_SUMMARY_QUERY: &str = r#"
INSERT INTO cached_available_indexers (repository_id, num_events, available_indexers)
VALUES ($1, $2, $3)
ON CONFLICT(repository_id) DO UPDATE
SET
	num_events = EXCLUDED.num_events,
	available_indexers = EXCLUDED.available_indexers
"#;

/// `truncateConfigurationSummaryQuery`.
const TRUNCATE_CONFIGURATION_SUMMARY_QUERY: &str = r#"
WITH safe AS (
	SELECT id
	FROM cached_available_indexers
	ORDER BY num_events DESC
	LIMIT $1
)
DELETE FROM cached_available_indexers
WHERE id NOT IN (SELECT id FROM safe)
"#;

/// `store.TopRepositoriesToConfigure`.
pub async fn top_repositories_to_configure(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<RepositoryWithCount>, sqlx::Error> {
    let rows = sqlx::query(TOP_REPOSITORIES_TO_CONFIGURE_QUERY)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(RepositoryWithCount {
                repository_id: row.try_get("id")?,
                count: row.try_get("num_events")?,
            })
        })
        .collect()
}

/// `store.GetRecentUploadsSummary`'s query, grouped.
///
/// The grouping reproduces the baseline's `groupedUploads` loop exactly,
/// including its sentinel: a first row whose `(root, indexer)` is `("", "")`
/// would be folded into the sentinel and dropped by `groupedUploads[1:]`.
pub async fn recent_uploads_summary(
    pool: &PgPool,
    repository_id: i32,
) -> Result<Vec<NamespaceGroup>, sqlx::Error> {
    group_root_indexer(pool, RECENT_UPLOADS_SUMMARY_QUERY, repository_id).await
}

/// `store.GetRecentAutoIndexJobsSummary`'s query, grouped (same sentinel
/// behaviour as [`recent_uploads_summary`]).
pub async fn recent_auto_index_jobs_summary(
    pool: &PgPool,
    repository_id: i32,
) -> Result<Vec<NamespaceGroup>, sqlx::Error> {
    group_root_indexer(pool, RECENT_INDEXES_SUMMARY_QUERY, repository_id).await
}

async fn group_root_indexer(
    pool: &PgPool,
    query: &str,
    repository_id: i32,
) -> Result<Vec<NamespaceGroup>, sqlx::Error> {
    let rows = sqlx::query(query)
        .bind(repository_id)
        .fetch_all(pool)
        .await?;

    // The baseline folds consecutive rows with equal (root, indexer) into one
    // group, starting from an empty sentinel group that is removed at the end.
    let mut groups = vec![NamespaceGroup {
        root: String::new(),
        indexer: String::new(),
        ids: Vec::new(),
    }];
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let root: String = row.try_get("root")?;
        let indexer: String = row.try_get("indexer")?;

        let last = groups.last().expect("sentinel group");
        if last.root != root || last.indexer != indexer {
            groups.push(NamespaceGroup {
                root,
                indexer,
                ids: Vec::new(),
            });
        }
        groups
            .last_mut()
            .expect("just pushed or sentinel")
            .ids
            .push(id);
    }

    groups.remove(0);
    Ok(groups)
}

/// `store.SetConfigurationSummary`: `json.Marshal(availableIndexers)` bound to
/// the `available_indexers` jsonb column.
pub async fn set_configuration_summary(
    pool: &PgPool,
    repository_id: i32,
    num_events: i64,
    available_indexers: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(SET_CONFIGURATION_SUMMARY_QUERY)
        .bind(repository_id)
        .bind(num_events)
        .bind(sqlx::types::Json(available_indexers))
        .execute(pool)
        .await?;
    Ok(())
}

/// `store.TruncateConfigurationSummary`.
pub async fn truncate_configuration_summary(
    pool: &PgPool,
    num_records_to_retain: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(TRUNCATE_CONFIGURATION_SUMMARY_QUERY)
        .bind(num_records_to_retain)
        .execute(pool)
        .await?;
    Ok(())
}
