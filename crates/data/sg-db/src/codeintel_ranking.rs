#![forbid(unsafe_code)]

//! `internal/codeintel/ranking/internal/store` — the non-mapper/reducer half
//! of the ranking store: graph keys, progress coordination, exported-upload
//! tracking, progress summaries and read-only retrieval.
//!
//! Ported 1:1 from the baseline files:
//!
//! | baseline | this module |
//! |---|---|
//! | `internal/shared/keys.go` | [`encode_graph_key`], [`new_derivative_graph_key`], [`graph_key_from_derivative_graph_key`], [`derivative_graph_key_from_prefix`] |
//! | `store/graph_keys.go` | [`derivative_graph_key`], [`bump_derivative_graph_key`], [`delete_ranking_progress`] |
//! | `store/coordinator.go` | [`coordinate`] |
//! | `store/uploads.go` | [`get_uploads_for_ranking`], [`vacuum_abandoned_exported_uploads`], [`soft_delete_stale_exported_uploads`], [`vacuum_deleted_exported_uploads`] |
//! | `store/summary.go` | [`summaries`] |
//! | `store/retrieval.go` | [`star_rank`], [`document_ranks`], [`reference_count_statistics`], [`coverage_counts`], [`last_updated_at`] |
//! | `store/mapper.go`, `store/reducer.go` | [`insert_path_count_inputs`], [`insert_initial_path_counts`], [`vacuum_stale_processed_references`], [`vacuum_stale_processed_paths`], [`vacuum_stale_graphs`], [`insert_path_ranks`], [`vacuum_stale_ranks`] |
//! | `store/definitions.go`, `store/references.go`, `store/paths.go` | [`insert_definitions_for_ranking`], [`insert_references_for_ranking`], [`insert_initial_path_ranks`] |
//!
//! Representation notes:
//!
//! * `Store.WithTransaction` is not modelled; every function here opens its own
//!   transaction where the baseline does (`BumpDerivativeGraphKey`,
//!   `Coordinate`) and runs standalone statements elsewhere, which have the
//!   same transactional semantics in PostgreSQL.
//! * `Coordinate` receives `now` explicitly (the baseline's `testNow` seam);
//!   production callers pass the process clock.
//! * The unused `objectPrefix` parameter of `GetUploadsForRanking` is omitted
//!   (its SQL never referenced it).
//! * `GetDocumentRanks` returns the decoded payload maps merged the same way
//!   the baseline merges them (per-row object merge, later rows win).
//! * Observation spans and metrics are omitted; migrations stay owned by the
//!   Go `migrator`.

use std::collections::BTreeMap;

use sqlx::postgres::{PgPool, PgRow};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgConnection, QueryBuilder, Row};

/// `vacuumBatchSize` (`store/util.go`).
pub const VACUUM_BATCH_SIZE: i64 = 100;
/// `threshold` (`store/util.go`), expressed in whole hours because the
/// baseline passes `int(threshold/time.Hour)` to the query.
pub const THRESHOLD_HOURS: i64 = 1;
/// `MaxGraphKeyRecords` (`store/graph_keys.go`).
pub const MAX_GRAPH_KEY_RECORDS: i64 = 10;
/// `MaxProgressRecords` (`store/coordinator.go`).
pub const MAX_PROGRESS_RECORDS: i64 = 10;

// ---------------------------------------------------------------------------
// Graph keys (`internal/shared/keys.go`)
// ---------------------------------------------------------------------------

/// `encode`: replaces `.` and `-` with `_`.
pub fn encode_graph_key(value: &str) -> String {
    value.replace(['.', '-'], "_")
}

/// `NewGraphKey`: a new root graph key.
pub fn new_graph_key(graph_key: &str) -> String {
    encode_graph_key(graph_key)
}

/// `NewDerivativeGraphKey`: `encode(root) + "." + encode(prefix)`.
pub fn new_derivative_graph_key(graph_key: &str, derivative_graph_key_prefix: &str) -> String {
    format!(
        "{}.{}",
        encode_graph_key(graph_key),
        encode_graph_key(derivative_graph_key_prefix)
    )
}

/// `DerivativeGraphKeyFromPrefix`: root is supplied by the caller (the
/// baseline reads `conf.CodeIntelRankingDocumentReferenceCountsGraphKey()`,
/// defaulting to `"dev"`).
pub fn derivative_graph_key_from_prefix(
    root_graph_key: &str,
    derivative_graph_key_prefix: &str,
) -> String {
    new_derivative_graph_key(root_graph_key, derivative_graph_key_prefix)
}

/// `GraphKeyFromDerivativeGraphKey`: the root of a derivative key, which must
/// have exactly two dot-separated segments.
pub fn graph_key_from_derivative_graph_key(derivative_graph_key: &str) -> Option<String> {
    // `strings.Split` keeps empty segments, so `"a."` has two parts and is
    // accepted by the baseline; only extra separators or a lone segment are
    // rejected.
    match derivative_graph_key
        .split('.')
        .collect::<Vec<_>>()
        .as_slice()
    {
        [root, _derivative] => Some((*root).to_owned()),
        _ => None,
    }
}

/// The baseline's `conf.CodeIntelRankingDocumentReferenceCountsGraphKey()`
/// default when the site setting is empty.
pub const DEFAULT_DOCUMENT_REFERENCE_COUNTS_GRAPH_KEY: &str = "dev";

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// `shared.ExportedUpload` (`ranking/shared/types.go`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedUpload {
    pub upload_id: i32,
    pub exported_upload_id: i32,
    pub repo: String,
    pub repo_id: i32,
    pub root: String,
}

/// `shared.Progress`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub started_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
    pub processed: i64,
    pub total: i64,
}

/// `shared.Summary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub graph_key: String,
    pub visible_to_zoekt: bool,
    pub path_mapper_progress: Progress,
    pub reference_mapper_progress: Progress,
    pub reducer_progress: Option<Progress>,
}

/// `shared.CoverageCounts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageCounts {
    pub num_target_indexes: i64,
    pub num_exported_indexes: i64,
    pub num_repositories_without_current_ranks: i64,
}

// ---------------------------------------------------------------------------
// Graph keys (`store/graph_keys.go`)
// ---------------------------------------------------------------------------

const DERIVATIVE_GRAPH_KEY_QUERY: &str = r#"
SELECT graph_key, created_at
FROM codeintel_ranking_graph_keys
ORDER BY created_at DESC
LIMIT 1
"#;

/// `(*store).DerivativeGraphKey`: the newest graph key, if any.
pub async fn derivative_graph_key(
    pool: &PgPool,
) -> Result<Option<(String, OffsetDateTime)>, sqlx::Error> {
    let row = sqlx::query(DERIVATIVE_GRAPH_KEY_QUERY)
        .fetch_optional(pool)
        .await?;

    row.map(|row| {
        Ok((
            row.try_get::<String, _>("graph_key")?,
            row.try_get("created_at")?,
        ))
    })
    .transpose()
}

/// `DerivativeGraphKey(ctx, store)` (the package-level helper in
/// `store/graph_keys.go`): the newest graph key, bumping a fresh one first
/// when the table is empty.
pub async fn derivative_graph_key_or_bump(
    pool: &PgPool,
) -> Result<(String, OffsetDateTime), sqlx::Error> {
    if let Some(key) = derivative_graph_key(pool).await? {
        return Ok(key);
    }

    bump_derivative_graph_key(pool).await?;
    derivative_graph_key(pool).await?.ok_or_else(|| {
        sqlx::Error::Protocol("no graph key after bump_derivative_graph_key".to_owned())
    })
}

const BUMP_DERIVATIVE_GRAPH_KEY_QUERY: &str = r#"
INSERT INTO codeintel_ranking_graph_keys (graph_key) VALUES ($1)
"#;

const BUMP_DERIVATIVE_GRAPH_KEY_PRUNE_QUERY: &str = r#"
DELETE FROM codeintel_ranking_graph_keys WHERE id IN (
	SELECT id
	FROM codeintel_ranking_graph_keys
	ORDER BY created_at DESC
	OFFSET $1
)
"#;

/// `BumpDerivativeGraphKey`: inserts a fresh `uuid.NewString()` graph key and
/// prunes all but the newest [`MAX_GRAPH_KEY_RECORDS`] rows, in one
/// transaction.
pub async fn bump_derivative_graph_key(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    sqlx::query(BUMP_DERIVATIVE_GRAPH_KEY_QUERY)
        .bind(uuid::Uuid::new_v4().to_string())
        .execute(&mut *tx)
        .await?;

    sqlx::query(BUMP_DERIVATIVE_GRAPH_KEY_PRUNE_QUERY)
        .bind(MAX_GRAPH_KEY_RECORDS)
        .execute(&mut *tx)
        .await?;

    tx.commit().await
}

const DELETE_RANKING_PROGRESS_QUERY: &str = r#"
DELETE FROM codeintel_ranking_progress WHERE graph_key = $1
"#;

/// `DeleteRankingProgress`.
pub async fn delete_ranking_progress(pool: &PgPool, graph_key: &str) -> Result<(), sqlx::Error> {
    sqlx::query(DELETE_RANKING_PROGRESS_QUERY)
        .bind(graph_key)
        .execute(pool)
        .await
        .map(|_| ())
}

// ---------------------------------------------------------------------------
// Coordinator (`store/coordinator.go`)
// ---------------------------------------------------------------------------

const COORDINATE_START_MAPPER_QUERY: &str = r#"
WITH
progress AS (
	SELECT
		COALESCE((SELECT MAX(id) FROM codeintel_ranking_exports WHERE graph_key = $1), 0) AS max_export_id
),
processable_paths AS (
	SELECT ipr.id
	FROM codeintel_initial_path_ranks ipr
	JOIN codeintel_ranking_exports cre ON cre.id = ipr.exported_upload_id
	JOIN progress p ON TRUE
	WHERE
		ipr.graph_key = $2 AND
		cre.id <= p.max_export_id AND
		cre.deleted_at IS NULL
),
processable_references AS (
	SELECT rr.id
	FROM codeintel_ranking_references rr
	JOIN codeintel_ranking_exports cre ON cre.id = rr.exported_upload_id
	JOIN progress p ON TRUE
	WHERE
		rr.graph_key = $3 AND
		cre.id <= p.max_export_id AND
		cre.deleted_at IS NULL
),
values AS (
	SELECT
		$4,
		p.max_export_id,
		$5::timestamp with time zone,
		(SELECT COUNT(*) FROM processable_paths),
		(SELECT COUNT(*) FROM processable_references)
	FROM progress p
	WHERE NOT EXISTS (
		SELECT 1
		FROM codeintel_ranking_progress
		WHERE graph_key = $6
	)
)
INSERT INTO codeintel_ranking_progress(
	graph_key,
	max_export_id,
	mappers_started_at,
	num_path_records_total,
	num_reference_records_total
)
SELECT * FROM values
ON CONFLICT DO NOTHING
"#;

const COORDINATE_PRUNE_QUERY: &str = r#"
DELETE FROM codeintel_ranking_progress WHERE id IN (
	SELECT id
	FROM codeintel_ranking_progress
	WHERE graph_key != $1
	ORDER BY mappers_started_at DESC
	OFFSET $2
)
"#;

const COORDINATE_START_REDUCER_QUERY: &str = r#"
WITH
processable_counts AS (
	SELECT pci.id
	FROM codeintel_ranking_path_counts_inputs pci
	WHERE
		pci.graph_key = $1 AND
		NOT pci.processed
)
UPDATE codeintel_ranking_progress
SET
	reducer_started_at      = $2,
	num_count_records_total = (SELECT COUNT(*) FROM processable_counts)
WHERE
	graph_key = $3 AND
	mapper_completed_at IS NOT NULL AND
	seed_mapper_completed_at IS NOT NULL AND
	reducer_started_at IS NULL
"#;

/// `Coordinate`: in one transaction, start the mapper phase for the root of
/// `derivative_graph_key` (when it has no progress row yet), prune older
/// progress rows, and start the reducer phase once both mappers completed.
pub async fn coordinate(
    pool: &PgPool,
    derivative_graph_key: &str,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let Some(graph_key) = graph_key_from_derivative_graph_key(derivative_graph_key) else {
        return Err(sqlx::Error::Protocol(format!(
            "unexpected derivative graph key {derivative_graph_key:?}"
        )));
    };

    let mut tx = pool.begin().await?;

    sqlx::query(COORDINATE_START_MAPPER_QUERY)
        .bind(&graph_key)
        .bind(&graph_key)
        .bind(&graph_key)
        .bind(derivative_graph_key)
        .bind(now)
        .bind(derivative_graph_key)
        .execute(&mut *tx)
        .await?;

    sqlx::query(COORDINATE_PRUNE_QUERY)
        .bind(derivative_graph_key)
        .bind(MAX_PROGRESS_RECORDS)
        .execute(&mut *tx)
        .await?;

    sqlx::query(COORDINATE_START_REDUCER_QUERY)
        .bind(derivative_graph_key)
        .bind(now)
        .bind(derivative_graph_key)
        .execute(&mut *tx)
        .await?;

    tx.commit().await
}

// ---------------------------------------------------------------------------
// Exported uploads (`store/uploads.go`)
// ---------------------------------------------------------------------------

const GET_UPLOADS_FOR_RANKING_QUERY: &str = r#"
WITH candidates AS (
	SELECT
		u.id AS upload_id,
		u.repository_id,
		r.name AS repository_name,
		u.root,
		md5(u.repository_id || ':' || u.root || ':' || u.indexer) AS upload_key
	FROM lsif_uploads u
	JOIN lsif_uploads_visible_at_tip uvt ON uvt.upload_id = u.id
	JOIN repo r ON r.id = u.repository_id
	WHERE
		uvt.is_default_branch AND
		r.deleted_at IS NULL AND
		r.blocked IS NULL AND
		NOT EXISTS (
			SELECT 1
			FROM codeintel_ranking_exports re
			WHERE
				re.graph_key = $1 AND
				re.upload_id = u.id
		)
	ORDER BY u.id DESC
	LIMIT $2
	FOR UPDATE SKIP LOCKED
),
inserted AS (
	INSERT INTO codeintel_ranking_exports (graph_key, upload_id, upload_key)
	SELECT $3, upload_id, upload_key FROM candidates
	ON CONFLICT (graph_key, upload_id) DO NOTHING
	RETURNING id, upload_id
)
SELECT
	i.upload_id,
	i.id,
	c.repository_name,
	c.repository_id,
	c.root
FROM inserted i
JOIN candidates c ON c.upload_id = i.upload_id
ORDER BY c.upload_id
"#;

/// `GetUploadsForRanking`: registers up to `batch_size` not-yet-exported
/// uploads for `graph_key` and returns them in `upload_id` order.
pub async fn get_uploads_for_ranking(
    pool: &PgPool,
    graph_key: &str,
    batch_size: i64,
) -> Result<Vec<ExportedUpload>, sqlx::Error> {
    let rows = sqlx::query(GET_UPLOADS_FOR_RANKING_QUERY)
        .bind(graph_key)
        .bind(batch_size)
        .bind(graph_key)
        .fetch_all(pool)
        .await?;

    rows.into_iter().map(exported_upload_from_row).collect()
}

/// [`get_uploads_for_ranking`] against an open connection, so the ranking
/// exporter registers exports and reads them back inside its transaction.
pub async fn get_uploads_for_ranking_on(
    conn: &mut PgConnection,
    graph_key: &str,
    batch_size: i64,
) -> Result<Vec<ExportedUpload>, sqlx::Error> {
    let rows = sqlx::query(GET_UPLOADS_FOR_RANKING_QUERY)
        .bind(graph_key)
        .bind(batch_size)
        .bind(graph_key)
        .fetch_all(&mut *conn)
        .await?;

    rows.into_iter().map(exported_upload_from_row).collect()
}

fn exported_upload_from_row(row: PgRow) -> Result<ExportedUpload, sqlx::Error> {
    Ok(ExportedUpload {
        upload_id: row.try_get("upload_id")?,
        exported_upload_id: row.try_get("id")?,
        repo: row.try_get("repository_name")?,
        repo_id: row.try_get("repository_id")?,
        root: row.try_get("root")?,
    })
}

const VACUUM_ABANDONED_EXPORTED_UPLOADS_QUERY: &str = r#"
WITH
locked_exported_uploads AS (
	SELECT id
	FROM codeintel_ranking_exports
	WHERE (graph_key < $1 OR graph_key > $2)
	ORDER BY graph_key, id
	FOR UPDATE SKIP LOCKED
	LIMIT $3
),
deleted_uploads AS (
	DELETE FROM codeintel_ranking_exports
	WHERE id IN (SELECT id FROM locked_exported_uploads)
	RETURNING 1
)
SELECT COUNT(*) FROM deleted_uploads
"#;

/// `VacuumAbandonedExportedUploads`: deletes export rows outside the graph
/// key range `[graph_key, graph_key]` (baseline passes the same key twice).
pub async fn vacuum_abandoned_exported_uploads(
    pool: &PgPool,
    graph_key: &str,
    batch_size: i64,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(VACUUM_ABANDONED_EXPORTED_UPLOADS_QUERY)
        .bind(graph_key)
        .bind(graph_key)
        .bind(batch_size)
        .fetch_one(pool)
        .await?;

    row.try_get::<i64, _>("count")
}

const SOFT_DELETE_STALE_EXPORTED_UPLOADS_QUERY: &str = r#"
WITH
locked_exported_uploads AS (
	SELECT
		cre.id,
		cre.upload_id
	FROM codeintel_ranking_exports cre
	WHERE
		cre.graph_key = $1 AND
		cre.deleted_at IS NULL AND
		(cre.last_scanned_at IS NULL OR NOW() - cre.last_scanned_at >= $2 * '1 hour'::interval)
	ORDER BY cre.last_scanned_at ASC NULLS FIRST, cre.id
	FOR UPDATE SKIP LOCKED
	LIMIT $3
),
candidates AS (
	SELECT
		leu.id,
		uvt.is_default_branch IS TRUE AS safe
	FROM locked_exported_uploads leu
	LEFT JOIN lsif_uploads u ON u.id = leu.upload_id
	LEFT JOIN lsif_uploads_visible_at_tip uvt ON uvt.repository_id = u.repository_id AND uvt.upload_id = leu.upload_id AND uvt.is_default_branch
),
updated_exported_uploads AS (
	UPDATE codeintel_ranking_exports cre
	SET last_scanned_at = NOW()
	WHERE id IN (SELECT c.id FROM candidates c WHERE c.safe)
),
deleted_exported_uploads AS (
	UPDATE codeintel_ranking_exports cre
	SET deleted_at = NOW()
	WHERE id IN (SELECT c.id FROM candidates c WHERE NOT c.safe)
	RETURNING 1
)
SELECT
	(SELECT COUNT(*) FROM candidates) AS num_scanned,
	(SELECT COUNT(*) FROM deleted_exported_uploads) AS num_deleted
"#;

/// `SoftDeleteStaleExportedUploads`: refreshes or soft-deletes export rows
/// whose last scan is at least [`THRESHOLD_HOURS`] old. Returns
/// `(scanned, deleted)`.
pub async fn soft_delete_stale_exported_uploads(
    pool: &PgPool,
    graph_key: &str,
) -> Result<(i64, i64), sqlx::Error> {
    let row = sqlx::query(SOFT_DELETE_STALE_EXPORTED_UPLOADS_QUERY)
        .bind(graph_key)
        .bind(THRESHOLD_HOURS)
        .bind(VACUUM_BATCH_SIZE)
        .fetch_one(pool)
        .await?;

    Ok((row.try_get("num_scanned")?, row.try_get("num_deleted")?))
}

const VACUUM_DELETED_EXPORTED_UPLOADS_QUERY: &str = r#"
WITH
locked_exported_uploads AS (
	SELECT cre.id
	FROM codeintel_ranking_exports cre
	WHERE
		cre.graph_key = $1 AND
		cre.deleted_at IS NOT NULL AND
		NOT EXISTS (
			SELECT 1
			FROM codeintel_ranking_progress crp
			WHERE
				crp.graph_key = $2 AND
				crp.reducer_completed_at IS NULL AND
				crp.mappers_started_at <= cre.deleted_at
		)
	ORDER BY cre.id
	FOR UPDATE SKIP LOCKED
	LIMIT $3
),
deleted_exported_uploads AS (
	DELETE FROM codeintel_ranking_exports
	WHERE id IN (SELECT id FROM locked_exported_uploads)
	RETURNING 1
)
SELECT COUNT(*) FROM deleted_exported_uploads
"#;

/// `VacuumDeletedExportedUploads`: hard-deletes soft-deleted export rows that
/// no in-progress progress row still references.
pub async fn vacuum_deleted_exported_uploads(
    pool: &PgPool,
    derivative_graph_key: &str,
) -> Result<i64, sqlx::Error> {
    let Some(graph_key) = graph_key_from_derivative_graph_key(derivative_graph_key) else {
        return Err(sqlx::Error::Protocol(format!(
            "unexpected derivative graph key {derivative_graph_key:?}"
        )));
    };

    let row = sqlx::query(VACUUM_DELETED_EXPORTED_UPLOADS_QUERY)
        .bind(&graph_key)
        .bind(derivative_graph_key)
        .bind(VACUUM_BATCH_SIZE)
        .fetch_one(pool)
        .await?;

    row.try_get::<i64, _>("count")
}

// ---------------------------------------------------------------------------
// Summaries (`store/summary.go`)
// ---------------------------------------------------------------------------

const SUMMARIES_QUERY: &str = r#"
SELECT
	p.graph_key,
	p.mappers_started_at,
	p.mapper_completed_at,
	p.seed_mapper_completed_at,
	p.reducer_started_at,
	p.reducer_completed_at,
	p.num_path_records_total,
	p.num_reference_records_total,
	p.num_count_records_total,
	p.num_path_records_processed,
	p.num_reference_records_processed,
	p.num_count_records_processed,
	COALESCE(p.id = (
		SELECT pl.id
		FROM codeintel_ranking_progress pl
		WHERE pl.reducer_completed_at IS NOT NULL
		ORDER BY pl.reducer_completed_at DESC
		LIMIT 1
	), false) AS visible_to_zoekt
FROM codeintel_ranking_progress p
ORDER BY p.mappers_started_at DESC
"#;

/// `Summaries`: one summary per progress row, newest mapper start first.
pub async fn summaries(pool: &PgPool) -> Result<Vec<Summary>, sqlx::Error> {
    let rows = sqlx::query(SUMMARIES_QUERY).fetch_all(pool).await?;

    rows.into_iter()
        .map(|row| {
            let mappers_started_at: OffsetDateTime = row.try_get("mappers_started_at")?;
            let mapper_completed_at: Option<OffsetDateTime> = row.try_get("mapper_completed_at")?;
            let seed_mapper_completed_at: Option<OffsetDateTime> =
                row.try_get("seed_mapper_completed_at")?;
            let reducer_started_at: Option<OffsetDateTime> = row.try_get("reducer_started_at")?;
            let reducer_completed_at: Option<OffsetDateTime> =
                row.try_get("reducer_completed_at")?;
            let num_path_records_total: Option<i32> = row.try_get("num_path_records_total")?;
            let num_reference_records_total: Option<i32> =
                row.try_get("num_reference_records_total")?;
            let num_count_records_total: Option<i32> = row.try_get("num_count_records_total")?;
            let num_path_records_processed: Option<i32> =
                row.try_get("num_path_records_processed")?;
            let num_reference_records_processed: Option<i32> =
                row.try_get("num_reference_records_processed")?;
            let num_count_records_processed: Option<i32> =
                row.try_get("num_count_records_processed")?;
            let visible_to_zoekt: bool = row.try_get("visible_to_zoekt")?;

            let path_mapper_progress = Progress {
                started_at: mappers_started_at,
                completed_at: seed_mapper_completed_at,
                processed: i64::from(num_path_records_processed.unwrap_or_default()),
                total: i64::from(num_path_records_total.unwrap_or_default()),
            };
            let reference_mapper_progress = Progress {
                started_at: mappers_started_at,
                completed_at: mapper_completed_at,
                processed: i64::from(num_reference_records_processed.unwrap_or_default()),
                total: i64::from(num_reference_records_total.unwrap_or_default()),
            };
            let reducer_progress = reducer_started_at.map(|started_at| Progress {
                started_at,
                completed_at: reducer_completed_at,
                processed: i64::from(num_count_records_processed.unwrap_or_default()),
                total: i64::from(num_count_records_total.unwrap_or_default()),
            });

            Ok(Summary {
                graph_key: row.try_get("graph_key")?,
                visible_to_zoekt,
                path_mapper_progress,
                reference_mapper_progress,
                reducer_progress,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Retrieval (`store/retrieval.go`)
// ---------------------------------------------------------------------------

const GET_STAR_RANK_QUERY: &str = r#"
SELECT
	s.rank
FROM (
	SELECT
		name,
		percent_rank() OVER (ORDER BY stars) AS rank
	FROM repo
) s
WHERE s.name = $1
"#;

/// `GetStarRank`: the repo's star percentile rank. A missing repo is
/// `RowNotFound` (the baseline propagates `sql.ErrNoRows`).
pub async fn star_rank(pool: &PgPool, repo_name: &str) -> Result<f64, sqlx::Error> {
    let row = sqlx::query(GET_STAR_RANK_QUERY)
        .bind(repo_name)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => row.try_get("rank"),
        None => Err(sqlx::Error::RowNotFound),
    }
}

const GET_DOCUMENT_RANKS_QUERY: &str = r#"
WITH
last_completed_progress AS (
	SELECT crp.graph_key
	FROM codeintel_ranking_progress crp
	WHERE crp.reducer_completed_at IS NOT NULL
	ORDER BY crp.reducer_completed_at DESC
	LIMIT 1
)
SELECT payload
FROM codeintel_path_ranks pr
JOIN repo r ON r.id = pr.repository_id
WHERE
	pr.graph_key IN (SELECT graph_key FROM last_completed_progress) AND
	r.name = $1 AND
	r.deleted_at IS NULL AND
	r.blocked IS NULL
"#;

/// `GetDocumentRanks`: merged `path -> rank` payload of the repo's ranks for
/// the last completed progress. The baseline always reports `true` for the
/// second return value; that constant is kept as the `bool` tuple member.
pub async fn document_ranks(
    pool: &PgPool,
    repo_name: &str,
) -> Result<(BTreeMap<String, f64>, bool), sqlx::Error> {
    let rows = sqlx::query(GET_DOCUMENT_RANKS_QUERY)
        .bind(repo_name)
        .fetch_all(pool)
        .await?;

    let mut path_ranks_with_precision = BTreeMap::new();
    for row in rows {
        let serialized: sqlx::types::Json<serde_json::Value> = row.try_get("payload")?;
        let path_ranks: BTreeMap<String, f64> = serde_json::from_value(serialized.0)
            .map_err(|err| sqlx::Error::Protocol(format!("decode path ranks: {err}")))?;
        path_ranks_with_precision.extend(path_ranks);
    }

    Ok((path_ranks_with_precision, true))
}

const GET_REFERENCE_COUNT_STATISTICS_QUERY: &str = r#"
WITH
last_completed_progress AS (
	SELECT crp.graph_key
	FROM codeintel_ranking_progress crp
	WHERE crp.reducer_completed_at IS NOT NULL
	ORDER BY crp.reducer_completed_at DESC
	LIMIT 1
)
SELECT
	CASE WHEN COALESCE(SUM(pr.num_paths), 0) = 0
		THEN 0.0
		ELSE SUM(pr.refcount_logsum) / SUM(pr.num_paths)::float
	END AS logmean
FROM codeintel_path_ranks pr
WHERE pr.graph_key IN (SELECT graph_key FROM last_completed_progress)
"#;

/// `GetReferenceCountStatistics`: the log-mean reference count over the last
/// completed progress (0 when there are no paths).
pub async fn reference_count_statistics(pool: &PgPool) -> Result<f64, sqlx::Error> {
    let row = sqlx::query(GET_REFERENCE_COUNT_STATISTICS_QUERY)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => row.try_get("logmean"),
        None => Ok(0.0),
    }
}

const COVERAGE_COUNTS_QUERY: &str = r#"
WITH
targets AS (
	SELECT uvt.upload_id
	FROM lsif_uploads_visible_at_tip uvt
	JOIN repo r ON r.id = uvt.repository_id
	WHERE
		uvt.is_default_branch AND
		r.deleted_at IS NULL AND
		r.blocked IS NULL
),
exported AS (
	SELECT re.id
	FROM codeintel_ranking_exports re
	JOIN targets t ON t.upload_id = re.upload_id
	WHERE
		re.graph_key = $1 AND
		re.deleted_at IS NULL
),
progress AS (
	SELECT pl.id
	FROM codeintel_ranking_progress pl
	WHERE pl.reducer_completed_at IS NOT NULL
	ORDER BY pl.reducer_completed_at DESC
	LIMIT 1
),
unindexed AS (
	SELECT r.id
	FROM repo r
	JOIN codeintel_path_ranks pr ON pr.repository_id = r.id
	JOIN codeintel_ranking_progress crp ON crp.graph_key = pr.graph_key
	WHERE
		r.deleted_at IS NULL AND
		r.blocked IS NULL AND
		crp.id = (SELECT id FROM progress) AND
		NOT EXISTS (
			SELECT 1
			FROM zoekt_repos zr
			WHERE
				zr.repo_id = r.id AND
				zr.index_status = 'indexed' AND
				crp.reducer_completed_at < zr.last_indexed_at
		)
)
SELECT
	(SELECT COUNT(*) FROM targets) AS num_targets,
	(SELECT COUNT(*) FROM exported) AS num_exported,
	(SELECT COUNT(*) FROM unindexed) AS num_unindexed
"#;

/// `CoverageCounts`: target/exported uploads and repos without current ranks.
pub async fn coverage_counts(
    pool: &PgPool,
    graph_key: &str,
) -> Result<CoverageCounts, sqlx::Error> {
    let row = sqlx::query(COVERAGE_COUNTS_QUERY)
        .bind(graph_key)
        .fetch_one(pool)
        .await?;

    Ok(CoverageCounts {
        num_target_indexes: row.try_get("num_targets")?,
        num_exported_indexes: row.try_get("num_exported")?,
        num_repositories_without_current_ranks: row.try_get("num_unindexed")?,
    })
}

const LAST_UPDATED_AT_QUERY: &str = r#"
WITH
progress AS (
	SELECT pl.id
	FROM codeintel_ranking_progress pl
	WHERE pl.reducer_completed_at IS NOT NULL
	ORDER BY pl.reducer_completed_at DESC
	LIMIT 1
)
SELECT
	pr.repository_id,
	crp.reducer_completed_at
FROM codeintel_path_ranks pr
JOIN codeintel_ranking_progress crp ON crp.graph_key = pr.graph_key
WHERE
	pr.repository_id = ANY($1) AND
	crp.id = (SELECT id FROM progress)
"#;

/// `LastUpdatedAt`: `reducer_completed_at` per repo under the last completed
/// progress.
pub async fn last_updated_at(
    pool: &PgPool,
    repo_ids: &[i32],
) -> Result<BTreeMap<i32, OffsetDateTime>, sqlx::Error> {
    let rows = sqlx::query(LAST_UPDATED_AT_QUERY)
        .bind(repo_ids)
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get::<i32, _>("repository_id")?,
                row.try_get("reducer_completed_at")?,
            ))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Mapper / reducer writes (`store/mapper.go`, `store/reducer.go`)
// ---------------------------------------------------------------------------

fn invalid_derivative_graph_key(key: &str) -> sqlx::Error {
    sqlx::Error::Protocol(format!("unexpected derivative graph key {key:?}"))
}

const INSERT_PATH_COUNT_INPUTS_QUERY: &str = r#"
WITH
progress AS (
	SELECT
		crp.id,
		crp.max_export_id,
		crp.reference_cursor_export_deleted_at,
		crp.reference_cursor_export_id,
		crp.mappers_started_at as started_at
	FROM codeintel_ranking_progress crp
	WHERE
		crp.graph_key = $1 AND
		crp.mapper_completed_at IS NULL
),
exported_uploads AS (
	SELECT
		cre.id,
		cre.upload_id,
		cre.upload_key,
		cre.deleted_at
	FROM codeintel_ranking_exports cre
	JOIN progress p ON TRUE
	WHERE
		cre.graph_key = $2 AND
		cre.id <= p.max_export_id AND
		(cre.deleted_at IS NULL OR cre.deleted_at > p.started_at) AND
		NOT (p.reference_cursor_export_deleted_at IS NOT NULL AND cre.deleted_at IS NULL) AND
		NOT (p.reference_cursor_export_deleted_at IS NOT NULL AND cre.deleted_at IS NOT NULL AND p.reference_cursor_export_deleted_at < cre.deleted_at) AND
		NOT (
			p.reference_cursor_export_id IS NOT NULL AND
			p.reference_cursor_export_deleted_at IS NOT DISTINCT FROM cre.deleted_at AND
			cre.id < p.reference_cursor_export_id
		)
	ORDER BY cre.graph_key, cre.deleted_at DESC NULLS FIRST, cre.id
),
refs AS (
	SELECT
		rr.id,
		eu.upload_id,
		eu.deleted_at AS exported_upload_deleted_at,
		eu.id AS exported_upload_id,
		eu.upload_key,
		rr.symbol_checksums
	FROM codeintel_ranking_references rr
	JOIN exported_uploads eu ON eu.id = rr.exported_upload_id
	WHERE
		NOT EXISTS (
			SELECT 1
			FROM codeintel_ranking_references_processed rrp
			WHERE
				rrp.graph_key = $3 AND
				rrp.codeintel_ranking_reference_id = rr.id
		)
	ORDER BY eu.deleted_at DESC NULLS FIRST, eu.id, rr.exported_upload_id
	LIMIT $4
	FOR UPDATE SKIP LOCKED
),
ordered_refs AS (
	SELECT
		r.*,
		RANK() OVER (ORDER BY r.exported_upload_deleted_at ASC NULLS LAST, r.exported_upload_id DESC) AS rank
	FROM refs r
),
locked_refs AS (
	INSERT INTO codeintel_ranking_references_processed (graph_key, codeintel_ranking_reference_id)
	SELECT $5, r.id FROM refs r
	ON CONFLICT DO NOTHING
	RETURNING codeintel_ranking_reference_id
),
referenced_upload_keys AS (
	SELECT DISTINCT r.upload_key
	FROM locked_refs lr
	JOIN refs r ON r.id = lr.codeintel_ranking_reference_id
),
processed_upload_keys AS (
	SELECT cre2.upload_key, cre2.upload_id
	FROM codeintel_ranking_exports cre2
	JOIN codeintel_ranking_references rr2 ON rr2.exported_upload_id = cre2.id
	JOIN codeintel_ranking_references_processed rrp2 ON rrp2.codeintel_ranking_reference_id = rr2.id
	WHERE
		cre2.graph_key = $6 AND
		rr2.graph_key = $7 AND
		rrp2.graph_key = $8 AND
		cre2.upload_key IN (SELECT upload_key FROM referenced_upload_keys)
),
processable_symbols AS (
	SELECT r.symbol_checksums
	FROM locked_refs lr
	JOIN refs r ON r.id = lr.codeintel_ranking_reference_id
	WHERE
		NOT EXISTS (
			SELECT 1
			FROM processed_upload_keys puk
			WHERE
				puk.upload_key = r.upload_key AND
				puk.upload_id != r.upload_id
		) AND
		NOT EXISTS (
			SELECT 1
			FROM locked_refs lr2
			JOIN refs r2 ON r2.id = lr2.codeintel_ranking_reference_id
			WHERE
				r2.upload_key = r.upload_key AND
				r2.upload_id > r.upload_id
		)
),
referenced_symbols AS (
	SELECT DISTINCT unnest(r.symbol_checksums) AS symbol_checksum
	FROM processable_symbols r
),
ranked_referenced_definitions AS (
	SELECT
		rd.id AS definition_id,
		RANK() OVER (PARTITION BY cre.upload_key ORDER BY cre.upload_id DESC) AS rank
	FROM codeintel_ranking_definitions rd
	JOIN referenced_symbols rs ON rs.symbol_checksum = rd.symbol_checksum
	JOIN codeintel_ranking_exports cre ON cre.id = rd.exported_upload_id
	JOIN progress p ON TRUE
	WHERE
		rd.graph_key = $9 AND
		cre.graph_key = $10 AND
		cre.id <= p.max_export_id AND
		(cre.deleted_at IS NULL OR cre.deleted_at > p.started_at)
	ORDER BY cre.graph_key, cre.deleted_at DESC NULLS FIRST, cre.id
),
referenced_definitions AS (
	SELECT
		s.definition_id,
		COUNT(*) AS count
	FROM ranked_referenced_definitions s
	WHERE s.rank = 1
	GROUP BY s.definition_id
),
ins AS (
	INSERT INTO codeintel_ranking_path_counts_inputs AS target (graph_key, definition_id, count, processed)
	SELECT
		$11,
		rx.definition_id,
		rx.count,
		false
	FROM referenced_definitions rx
	ON CONFLICT (graph_key, definition_id) WHERE NOT processed DO UPDATE SET count = target.count + EXCLUDED.count
	RETURNING 1
),
set_progress AS (
	UPDATE codeintel_ranking_progress
	SET
		reference_cursor_export_deleted_at = COALESCE((SELECT tor.exported_upload_deleted_at FROM ordered_refs tor WHERE tor.rank = 1 LIMIT 1), NULL),
		reference_cursor_export_id         = COALESCE((SELECT tor.exported_upload_id FROM ordered_refs tor WHERE tor.rank = 1 LIMIT 1), NULL),
		num_reference_records_processed    = COALESCE(num_reference_records_processed, 0) + (SELECT COUNT(*) FROM locked_refs),
		mapper_completed_at                = CASE WHEN (SELECT COUNT(*) FROM refs) = 0 THEN NOW() ELSE NULL END
	WHERE id IN (SELECT id FROM progress)
)
SELECT
	(SELECT COUNT(*) FROM locked_refs),
	(SELECT COUNT(*) FROM ins)
"#;

/// `InsertPathCountInputs`: process one batch of ranking references into
/// `codeintel_ranking_path_counts_inputs`. Returns
/// `(num_reference_records_processed, num_inputs_inserted)`.
pub async fn insert_path_count_inputs(
    pool: &PgPool,
    derivative_graph_key: &str,
    batch_size: i64,
) -> Result<(i64, i64), sqlx::Error> {
    let graph_key = graph_key_from_derivative_graph_key(derivative_graph_key)
        .ok_or_else(|| invalid_derivative_graph_key(derivative_graph_key))?;

    // Argument order mirrors the baseline `sqlf.Sprintf` call:
    // derivative, root, derivative, batch, derivative, root, root, derivative,
    // root, root, derivative.
    let row = sqlx::query(INSERT_PATH_COUNT_INPUTS_QUERY)
        .bind(derivative_graph_key)
        .bind(&graph_key)
        .bind(derivative_graph_key)
        .bind(batch_size)
        .bind(derivative_graph_key)
        .bind(&graph_key)
        .bind(&graph_key)
        .bind(derivative_graph_key)
        .bind(&graph_key)
        .bind(&graph_key)
        .bind(derivative_graph_key)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => Ok((row.try_get(0)?, row.try_get(1)?)),
        None => Ok((0, 0)),
    }
}

const INSERT_INITIAL_PATH_COUNTS_QUERY: &str = r#"
WITH
progress AS (
	SELECT
		crp.id,
		crp.max_export_id,
		crp.path_cursor_deleted_export_at,
		crp.path_cursor_export_id,
		crp.mappers_started_at as started_at
	FROM codeintel_ranking_progress crp
	WHERE
		crp.graph_key = $1 AND
		crp.seed_mapper_completed_at IS NULL
),
exported_uploads AS (
	SELECT
		cre.id,
		cre.upload_id,
		cre.deleted_at
	FROM codeintel_ranking_exports cre
	JOIN progress p ON TRUE
	WHERE
		cre.graph_key = $2 AND
		cre.id <= p.max_export_id AND
		(cre.deleted_at IS NULL OR cre.deleted_at > p.started_at) AND
		NOT (p.path_cursor_deleted_export_at IS NOT NULL AND cre.deleted_at IS NULL) AND
		NOT (p.path_cursor_deleted_export_at IS NOT NULL AND cre.deleted_at IS NOT NULL AND p.path_cursor_deleted_export_at < cre.deleted_at) AND
		NOT (
			p.path_cursor_export_id IS NOT NULL AND
			p.path_cursor_deleted_export_at IS NOT DISTINCT FROM cre.deleted_at AND
			cre.id < p.path_cursor_export_id
		)
	ORDER BY cre.graph_key, cre.deleted_at DESC NULLS FIRST, cre.id
),
unprocessed_path_counts AS (
	SELECT
		ipr.id,
		eu.upload_id,
		eu.deleted_at AS exported_upload_deleted_at,
		eu.id AS exported_upload_id,
		ipr.graph_key,
		CASE
			WHEN ipr.document_path != '' THEN array_append('{}'::text[], ipr.document_path)
			ELSE ipr.document_paths
		END AS document_paths
	FROM codeintel_initial_path_ranks ipr
	JOIN exported_uploads eu ON eu.id = ipr.exported_upload_id
	WHERE
		NOT EXISTS (
			SELECT 1
			FROM codeintel_initial_path_ranks_processed prp
			WHERE
				prp.graph_key = $3 AND
				prp.codeintel_initial_path_ranks_id = ipr.id
		)
	ORDER BY eu.deleted_at DESC NULLS FIRST, eu.id, ipr.exported_upload_id
	LIMIT $4
	FOR UPDATE SKIP LOCKED
),
ordered_paths AS (
	SELECT
		p.*,
		RANK() OVER (ORDER BY p.exported_upload_deleted_at ASC NULLS LAST, p.exported_upload_id DESC) AS rank
	FROM unprocessed_path_counts p
),
locked_path_counts AS (
	INSERT INTO codeintel_initial_path_ranks_processed (graph_key, codeintel_initial_path_ranks_id)
	SELECT
		$5,
		eupc.id
	FROM unprocessed_path_counts eupc
	ON CONFLICT DO NOTHING
	RETURNING codeintel_initial_path_ranks_id
),
expanded_unprocessed_path_counts AS (
	SELECT
		upc.id,
		upc.upload_id,
		upc.exported_upload_id,
		upc.graph_key,
		unnest(upc.document_paths) AS document_path
	FROM unprocessed_path_counts upc
),
ins AS (
	INSERT INTO codeintel_ranking_path_counts_inputs (graph_key, definition_id, count, processed)
	SELECT
		$6,
		rd.id,
		0,
		false
	FROM locked_path_counts lpc
	JOIN expanded_unprocessed_path_counts eupc ON eupc.id = lpc.codeintel_initial_path_ranks_id
	JOIN codeintel_ranking_definitions rd ON
		rd.exported_upload_id = eupc.exported_upload_id AND
		rd.document_path = eupc.document_path
	WHERE
		rd.graph_key = $7 AND
		rd.symbol_checksum = '\xc3e97dd6e97fb5125688c97f36720cbe'::bytea
	ON CONFLICT DO NOTHING
	RETURNING 1
),
set_progress AS (
	UPDATE codeintel_ranking_progress
	SET
		path_cursor_deleted_export_at = COALESCE((SELECT op.exported_upload_deleted_at FROM ordered_paths op WHERE op.rank = 1 LIMIT 1), NULL),
		path_cursor_export_id         = COALESCE((SELECT op.exported_upload_id FROM ordered_paths op WHERE op.rank = 1 LIMIT 1), NULL),
		num_path_records_processed    = COALESCE(num_path_records_processed, 0) + (SELECT COUNT(*) FROM locked_path_counts),
		seed_mapper_completed_at      = CASE WHEN (SELECT COUNT(*) FROM unprocessed_path_counts) = 0 THEN NOW() ELSE NULL END
	WHERE id IN (SELECT id FROM progress)
)
SELECT
	(SELECT COUNT(*) FROM locked_path_counts),
	(SELECT COUNT(*) FROM ins)
"#;

/// `InsertInitialPathCounts`: seed zero-count inputs for one batch of initial
/// path ranks. Returns `(num_initial_paths_processed, num_inputs_inserted)`.
pub async fn insert_initial_path_counts(
    pool: &PgPool,
    derivative_graph_key: &str,
    batch_size: i64,
) -> Result<(i64, i64), sqlx::Error> {
    let graph_key = graph_key_from_derivative_graph_key(derivative_graph_key)
        .ok_or_else(|| invalid_derivative_graph_key(derivative_graph_key))?;

    // Argument order: derivative, root, derivative, batch, derivative,
    // derivative, root.
    let row = sqlx::query(INSERT_INITIAL_PATH_COUNTS_QUERY)
        .bind(derivative_graph_key)
        .bind(&graph_key)
        .bind(derivative_graph_key)
        .bind(batch_size)
        .bind(derivative_graph_key)
        .bind(derivative_graph_key)
        .bind(&graph_key)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => Ok((row.try_get(0)?, row.try_get(1)?)),
        None => Ok((0, 0)),
    }
}

const VACUUM_STALE_PROCESSED_REFERENCES_QUERY: &str = r#"
WITH
locked_references_processed AS (
	SELECT id
	FROM codeintel_ranking_references_processed
	WHERE (graph_key < $1 OR graph_key > $2)
	ORDER BY graph_key, id
	FOR UPDATE SKIP LOCKED
	LIMIT $3
),
deleted_locked_references_processed AS (
	DELETE FROM codeintel_ranking_references_processed
	WHERE id IN (SELECT id FROM locked_references_processed)
	RETURNING 1
)
SELECT COUNT(*) FROM deleted_locked_references_processed
"#;

/// `VacuumStaleProcessedReferences`.
pub async fn vacuum_stale_processed_references(
    pool: &PgPool,
    derivative_graph_key: &str,
    batch_size: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(VACUUM_STALE_PROCESSED_REFERENCES_QUERY)
        .bind(derivative_graph_key)
        .bind(derivative_graph_key)
        .bind(batch_size)
        .fetch_one(pool)
        .await
}

const VACUUM_STALE_PROCESSED_PATHS_QUERY: &str = r#"
WITH
locked_paths_processed AS (
	SELECT id
	FROM codeintel_initial_path_ranks_processed
	WHERE (graph_key < $1 OR graph_key > $2)
	ORDER BY graph_key, id
	FOR UPDATE SKIP LOCKED
	LIMIT $3
),
deleted_locked_paths_processed AS (
	DELETE FROM codeintel_initial_path_ranks_processed
	WHERE id IN (SELECT id FROM locked_paths_processed)
	RETURNING 1
)
SELECT COUNT(*) FROM deleted_locked_paths_processed
"#;

/// `VacuumStaleProcessedPaths`.
pub async fn vacuum_stale_processed_paths(
    pool: &PgPool,
    derivative_graph_key: &str,
    batch_size: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(VACUUM_STALE_PROCESSED_PATHS_QUERY)
        .bind(derivative_graph_key)
        .bind(derivative_graph_key)
        .bind(batch_size)
        .fetch_one(pool)
        .await
}

const VACUUM_STALE_GRAPHS_QUERY: &str = r#"
WITH
locked_path_counts_inputs AS (
	SELECT id
	FROM codeintel_ranking_path_counts_inputs
	WHERE (graph_key < $1 OR graph_key > $2)
	ORDER BY graph_key, id
	FOR UPDATE SKIP LOCKED
	LIMIT $3
),
deleted_path_counts_inputs AS (
	DELETE FROM codeintel_ranking_path_counts_inputs
	WHERE id IN (SELECT id FROM locked_path_counts_inputs)
	RETURNING 1
)
SELECT COUNT(*) FROM deleted_path_counts_inputs
"#;

/// `VacuumStaleGraphs`.
pub async fn vacuum_stale_graphs(
    pool: &PgPool,
    derivative_graph_key: &str,
    batch_size: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(VACUUM_STALE_GRAPHS_QUERY)
        .bind(derivative_graph_key)
        .bind(derivative_graph_key)
        .bind(batch_size)
        .fetch_one(pool)
        .await
}

const INSERT_PATH_RANKS_QUERY: &str = r#"
WITH
progress AS (
	SELECT crp.id
	FROM codeintel_ranking_progress crp
	WHERE
		crp.graph_key = $1 and
		crp.reducer_started_at IS NOT NULL AND
		crp.reducer_completed_at IS NULL
),
rank_ids AS (
	SELECT pci.id
	FROM codeintel_ranking_path_counts_inputs pci
	JOIN progress p ON TRUE
	WHERE
		pci.graph_key = $2 AND
		NOT pci.processed
	ORDER BY pci.graph_key, pci.definition_id
	LIMIT $3
	FOR UPDATE SKIP LOCKED
),
input_ranks AS (
	SELECT
		pci.id,
		u.repository_id,
		rd.document_path AS path,
		pci.count
	FROM codeintel_ranking_path_counts_inputs pci
	JOIN codeintel_ranking_definitions rd ON rd.id = pci.definition_id
	JOIN codeintel_ranking_exports eu ON eu.id = rd.exported_upload_id
	JOIN lsif_uploads u ON u.id = eu.upload_id
	JOIN repo r ON r.id = u.repository_id
	JOIN progress p ON TRUE
	WHERE
		pci.id IN (SELECT id FROM rank_ids) AND
		r.deleted_at IS NULL AND
		r.blocked IS NULL
),
processed AS (
	UPDATE codeintel_ranking_path_counts_inputs
	SET processed = true
	WHERE id IN (SELECT ir.id FROM rank_ids ir)
	RETURNING 1
),
inserted AS (
	INSERT INTO codeintel_path_ranks AS pr (graph_key, repository_id, payload)
	SELECT
		$4,
		temp.repository_id,
		jsonb_object_agg(temp.path, temp.count)
	FROM (
		SELECT
			cr.repository_id,
			cr.path,
			SUM(count) AS count
		FROM input_ranks cr
		GROUP BY cr.repository_id, cr.path
	) temp
	GROUP BY temp.repository_id
	ON CONFLICT (graph_key, repository_id) DO UPDATE SET
		payload = (
			SELECT jsonb_object_agg(key, sum) FROM (
				SELECT key, SUM(value::int) AS sum
				FROM
					(
						SELECT * FROM jsonb_each(pr.payload)
						UNION
						SELECT * FROM jsonb_each(EXCLUDED.payload)
					) AS both_payloads
				GROUP BY key
			) AS combined_json
		)
	RETURNING 1
),
set_progress AS (
	UPDATE codeintel_ranking_progress
	SET
		num_count_records_processed = COALESCE(num_count_records_processed, 0) + (SELECT COUNT(*) FROM processed),
		reducer_completed_at        = CASE WHEN (SELECT COUNT(*) FROM rank_ids) = 0 THEN NOW() ELSE NULL END
	WHERE id IN (SELECT id FROM progress)
)
SELECT
	(SELECT COUNT(*) FROM processed) AS num_processed,
	(SELECT COUNT(*) FROM inserted) AS num_inserted
"#;

/// `InsertPathRanks`: reduce one batch of path count inputs into
/// `codeintel_path_ranks`. Returns `(num_inputs_processed,
/// num_path_ranks_inserted)`.
pub async fn insert_path_ranks(
    pool: &PgPool,
    derivative_graph_key: &str,
    batch_size: i64,
) -> Result<(i64, i64), sqlx::Error> {
    if graph_key_from_derivative_graph_key(derivative_graph_key).is_none() {
        return Err(invalid_derivative_graph_key(derivative_graph_key));
    }

    // Argument order: derivative, derivative, batch, derivative.
    let row = sqlx::query(INSERT_PATH_RANKS_QUERY)
        .bind(derivative_graph_key)
        .bind(derivative_graph_key)
        .bind(batch_size)
        .bind(derivative_graph_key)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => Ok((row.try_get(0)?, row.try_get(1)?)),
        None => Err(sqlx::Error::Protocol("no rows from count".to_owned())),
    }
}

const VACUUM_STALE_RANKS_QUERY: &str = r#"
WITH
valid_graph_keys AS (
	SELECT $1 AS graph_key
	UNION (
		SELECT crp.graph_key
		FROM codeintel_ranking_progress crp
		WHERE crp.reducer_completed_at IS NOT NULL
		ORDER BY crp.reducer_completed_at DESC
		LIMIT 1
	)
),
locked_records AS (
	SELECT id
	FROM codeintel_path_ranks
	WHERE graph_key NOT IN (SELECT graph_key FROM valid_graph_keys)
	ORDER BY id
	FOR UPDATE
),
deleted_records AS (
	DELETE FROM codeintel_path_ranks
	WHERE id IN (SELECT id FROM locked_records)
	RETURNING 1
)
SELECT
	(SELECT COUNT(*) FROM locked_records),
	(SELECT COUNT(*) FROM deleted_records)
"#;

/// `VacuumStaleRanks`: delete path ranks whose graph key is neither the
/// current derivative key nor the most recently completed graph key.
/// Returns `(rank_records_scanned, rank_records_deleted)`.
pub async fn vacuum_stale_ranks(
    pool: &PgPool,
    derivative_graph_key: &str,
) -> Result<(i64, i64), sqlx::Error> {
    if graph_key_from_derivative_graph_key(derivative_graph_key).is_none() {
        return Err(invalid_derivative_graph_key(derivative_graph_key));
    }

    let row = sqlx::query(VACUUM_STALE_RANKS_QUERY)
        .bind(derivative_graph_key)
        .fetch_one(pool)
        .await?;

    Ok((row.try_get(0)?, row.try_get(1)?))
}

// ---------------------------------------------------------------------------
// Raw export inserts (`store/definitions.go`, `store/references.go`,
// `store/paths.go`)
// ---------------------------------------------------------------------------

/// `MaxNumPostgresParameters` (`internal/database/batch/batch.go`): the
/// maximum number of bind parameters in a single statement.
pub const MAX_NUM_POSTGRES_PARAMETERS: usize = 65535;

/// `sentinelPathDefinitionName` (`store/paths.go`): `md5.Sum([]byte("$"))`,
/// written as the Postgres literal `'\xc3e97dd6e97fb5125688c97f36720cbe'`.
pub const SENTINEL_PATH_DEFINITION_NAME: [u8; 16] = [
    0xc3, 0xe9, 0x7d, 0xd6, 0xe9, 0x7f, 0xb5, 0x12, 0x56, 0x88, 0xc9, 0x7f, 0x36, 0x72, 0x0c, 0xbe,
];

/// `shared.RankingDefinitions`: one row of `codeintel_ranking_definitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankingDefinition {
    pub exported_upload_id: i32,
    pub symbol_checksum: [u8; 16],
    pub document_path: String,
}

/// `batchSlice`/`batchChannel` (`store/util.go`): everything up to a batch of
/// `batch_size` elements; a non-empty tail forms its own batch. A batch size
/// `<= 0` behaves like a single batch over the whole input (the Go helpers
/// never reach their emit threshold), and an empty input yields no batches.
fn chunks_for_batch_size<T>(values: &[T], batch_size: i64) -> Vec<&[T]> {
    if values.is_empty() {
        return Vec::new();
    }

    let size = if batch_size <= 0 {
        values.len()
    } else {
        batch_size as usize
    };

    values.chunks(size).collect()
}

/// `InsertDefinitionsForRanking` (`store/definitions.go`): batch-insert rows
/// into `codeintel_ranking_definitions` with an empty `symbol_name`.
pub async fn insert_definitions_for_ranking(
    pool: &PgPool,
    graph_key: &str,
    definitions: &[RankingDefinition],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    insert_definitions_for_ranking_on(&mut *tx, graph_key, definitions).await?;
    tx.commit().await
}

/// [`insert_definitions_for_ranking`] against an open connection, so
/// [`insert_initial_path_ranks`] and the ranking exporter write their
/// definitions through the same transaction as their other writes.
pub async fn insert_definitions_for_ranking_on(
    conn: &mut PgConnection,
    graph_key: &str,
    definitions: &[RankingDefinition],
) -> Result<(), sqlx::Error> {
    // Five columns per row; the batch inserter flushes on the same limit.
    let rows_per_insert = (MAX_NUM_POSTGRES_PARAMETERS / 5).max(1);

    for chunk in definitions.chunks(rows_per_insert) {
        let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO codeintel_ranking_definitions \
             (exported_upload_id, symbol_name, symbol_checksum, document_path, graph_key) ",
        );
        builder.push_values(chunk, |mut builder, definition| {
            builder
                .push_bind(definition.exported_upload_id)
                .push_bind("")
                .push_bind(definition.symbol_checksum.to_vec())
                .push_bind(definition.document_path.as_str())
                .push_bind(graph_key);
        });
        builder.build().execute(&mut *conn).await?;
    }

    Ok(())
}

/// `InsertReferencesForRanking` (`store/references.go`): group the checksums
/// into batches of `batch_size` and insert one `codeintel_ranking_references`
/// row per batch, with empty `symbol_names`.
pub async fn insert_references_for_ranking(
    pool: &PgPool,
    graph_key: &str,
    batch_size: i64,
    exported_upload_id: i32,
    references: &[[u8; 16]],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    insert_references_for_ranking_on(
        &mut *tx,
        graph_key,
        batch_size,
        exported_upload_id,
        references,
    )
    .await?;
    tx.commit().await
}

/// [`insert_references_for_ranking`] against an open connection, so the
/// ranking exporter writes references through its outer transaction.
pub async fn insert_references_for_ranking_on(
    conn: &mut PgConnection,
    graph_key: &str,
    batch_size: i64,
    exported_upload_id: i32,
    references: &[[u8; 16]],
) -> Result<(), sqlx::Error> {
    let batches = chunks_for_batch_size(references, batch_size);
    if batches.is_empty() {
        return Ok(());
    }

    for checksums in batches {
        // One row per batch; the checksums are spelled out as `ARRAY[...]` so
        // only scalar `bytea` parameters are bound (the empty `symbol_names`
        // array is a literal, mirroring `pq.Array([]string{})`).
        let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO codeintel_ranking_references \
             (exported_upload_id, symbol_names, symbol_checksums, graph_key) \
             VALUES (",
        );
        builder.push_bind(exported_upload_id);
        builder.push(", '{}'::text[], ARRAY[");
        for (index, checksum) in checksums.iter().enumerate() {
            if index > 0 {
                builder.push(", ");
            }
            builder.push_bind(checksum.to_vec());
        }
        builder.push("]::bytea[], ");
        builder.push_bind(graph_key);
        builder.push(")");
        builder.build().execute(&mut *conn).await?;
    }
    Ok(())
}

/// `createInitialPathTemporaryTableQuery` (`store/paths.go`).
const CREATE_INITIAL_PATH_TEMPORARY_TABLE_QUERY: &str = r#"
CREATE TEMPORARY TABLE IF NOT EXISTS t_codeintel_initial_path_ranks (
	document_paths text[] NOT NULL
)
ON COMMIT DROP
"#;

/// `insertInitialPathRankCountsQuery` (`store/paths.go`).
const INSERT_INITIAL_PATH_RANK_COUNTS_QUERY: &str = r#"
INSERT INTO codeintel_initial_path_ranks (exported_upload_id, document_paths, graph_key)
SELECT $1, document_paths, $2 FROM t_codeintel_initial_path_ranks
"#;

/// `InsertInitialPathRanks` (`store/paths.go`): stage the document paths in a
/// temporary table, copy them into `codeintel_initial_path_ranks` as batches,
/// and insert one sentinel definition (`md5("$")`) per path — all in one
/// transaction. The sentinel rows are what [`insert_initial_path_counts`]
/// later matches on.
pub async fn insert_initial_path_ranks(
    pool: &PgPool,
    exported_upload_id: i32,
    document_paths: &[String],
    batch_size: i64,
    graph_key: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    insert_initial_path_ranks_on(
        &mut *tx,
        exported_upload_id,
        document_paths,
        batch_size,
        graph_key,
    )
    .await?;
    tx.commit().await
}

/// [`insert_initial_path_ranks`] against an open connection, so the ranking
/// exporter shares its outer transaction. The temporary table persists for the
/// lifetime of that transaction, exactly as the baseline's nested transaction
/// does (later uploads see rows staged by earlier ones).
pub async fn insert_initial_path_ranks_on(
    conn: &mut PgConnection,
    exported_upload_id: i32,
    document_paths: &[String],
    batch_size: i64,
    graph_key: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(CREATE_INITIAL_PATH_TEMPORARY_TABLE_QUERY)
        .execute(&mut *conn)
        .await?;

    for paths in chunks_for_batch_size(document_paths, batch_size) {
        // `text[]` row spelled out as `ARRAY[...]` of scalar text parameters.
        let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO t_codeintel_initial_path_ranks (document_paths) VALUES (ARRAY[",
        );
        for (index, path) in paths.iter().enumerate() {
            if index > 0 {
                builder.push(", ");
            }
            builder.push_bind(path.as_str());
        }
        builder.push("]::text[])");
        builder.build().execute(&mut *conn).await?;
    }

    sqlx::query(INSERT_INITIAL_PATH_RANK_COUNTS_QUERY)
        .bind(exported_upload_id)
        .bind(graph_key)
        .execute(&mut *conn)
        .await?;

    let definitions: Vec<RankingDefinition> = document_paths
        .iter()
        .map(|document_path| RankingDefinition {
            exported_upload_id,
            symbol_checksum: SENTINEL_PATH_DEFINITION_NAME,
            document_path: document_path.clone(),
        })
        .collect();
    insert_definitions_for_ranking_on(&mut *conn, graph_key, &definitions).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_graph_key_matches_the_baseline() {
        assert_eq!(encode_graph_key("a.b-c"), "a_b_c");
        assert_eq!(new_graph_key("dev"), "dev");
        assert_eq!(new_derivative_graph_key("dev", "2024.01"), "dev.2024_01");
        assert_eq!(
            derivative_graph_key_from_prefix("dev", "exports-x"),
            "dev.exports_x"
        );
    }

    #[test]
    fn graph_key_from_derivative_graph_key_requires_two_segments() {
        assert_eq!(
            graph_key_from_derivative_graph_key("dev.123").as_deref(),
            Some("dev")
        );
        assert_eq!(
            graph_key_from_derivative_graph_key("dev.").as_deref(),
            Some("dev")
        );
        assert_eq!(graph_key_from_derivative_graph_key("dev"), None);
        assert_eq!(graph_key_from_derivative_graph_key("dev.1.2"), None);
    }

    #[test]
    fn constants_match_the_baseline() {
        assert_eq!(VACUUM_BATCH_SIZE, 100);
        assert_eq!(THRESHOLD_HOURS, 1);
        assert_eq!(MAX_GRAPH_KEY_RECORDS, 10);
        assert_eq!(MAX_PROGRESS_RECORDS, 10);
        assert_eq!(DEFAULT_DOCUMENT_REFERENCE_COUNTS_GRAPH_KEY, "dev");
        assert_eq!(MAX_NUM_POSTGRES_PARAMETERS, 65535);
    }

    #[test]
    fn sentinel_path_definition_name_is_md5_of_dollar() {
        // `md5.Sum([]byte("$"))`; the same bytes appear as the Postgres bytea
        // literal in `insertInitialPathCountsInputsQuery`.
        let expected = [
            0xc3, 0xe9, 0x7d, 0xd6, 0xe9, 0x7f, 0xb5, 0x12, 0x56, 0x88, 0xc9, 0x7f, 0x36, 0x72,
            0x0c, 0xbe,
        ];
        assert_eq!(SENTINEL_PATH_DEFINITION_NAME.len(), 16);
        assert_eq!(SENTINEL_PATH_DEFINITION_NAME, expected);
    }

    #[test]
    fn chunks_for_batch_size_matches_the_baseline_helpers() {
        let values = [1, 2, 3, 4, 5];

        let sizes: Vec<usize> = chunks_for_batch_size(&values, 2)
            .iter()
            .map(|chunk| chunk.len())
            .collect();
        assert_eq!(sizes, vec![2, 2, 1]);

        assert!(chunks_for_batch_size::<i32>(&[], 2).is_empty());

        // A non-positive batch size degenerates to a single batch.
        let whole = chunks_for_batch_size(&values, 0);
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0], &values[..]);

        // Larger batches than the input still yield exactly one batch.
        assert_eq!(chunks_for_batch_size(&values, 10).len(), 1);
    }
}
