#![forbid(unsafe_code)]

//! Code-intel (codegraph) schema operations used by the
//! `codeintel-upload-janitor` worker job: `IDsWithMeta`,
//! `ReconcileCandidates`, `DeleteLsifDataByUploadIds`,
//! `DeleteAbandonedSchemaVersionsRecords` and `DeleteUnreferencedDocuments`
//! from `internal/codeintel/codegraph/cleanup.go`.
//!
//! These statements run against the code-intel database; the pool is created
//! by the job from `ServiceConnections.CodeIntelPostgresDSN`. Migrations stay
//! owned by the Go `migrator`.

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `store.IDsWithMeta`.
pub async fn ids_with_meta(pool: &PgPool, ids: &[i64]) -> Result<Vec<i64>, sqlx::Error> {
    let rows = sqlx::query(IDS_WITH_META_QUERY)
        .bind(ids)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| i64::from(row.get::<i32, _>(0)))
        .collect())
}

/// `store.ReconcileCandidatesWithTime` (`ReconcileCandidates` uses `now`).
pub async fn reconcile_candidates(
    pool: &PgPool,
    batch_size: i64,
    now: OffsetDateTime,
) -> Result<Vec<i64>, sqlx::Error> {
    let rows = sqlx::query(RECONCILE_CANDIDATES_QUERY)
        .bind(batch_size)
        .bind(batch_size)
        .bind(now)
        .bind(now)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| i64::from(row.get::<i32, _>(0)))
        .collect())
}

/// `store.DeleteLsifDataByUploadIds`: five deletes in one transaction, then
/// `codeintel_last_reconcile` outside it (the baseline commits the
/// transaction before running the last statement).
pub async fn delete_lsif_data_by_upload_ids(
    pool: &PgPool,
    upload_ids: &[i64],
) -> Result<(), sqlx::Error> {
    if upload_ids.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await?;
    for (statement, binds_twice) in [
        (DELETE_SCIP_DOCUMENT_LOOKUP_QUERY, false),
        (DELETE_SCIP_METADATA_QUERY, false),
        // The symbol-names statement takes the array twice.
        (DELETE_SCIP_SYMBOL_NAMES_QUERY, true),
        (DELETE_SCIP_DOCUMENT_LOOKUP_SCHEMA_VERSIONS_QUERY, false),
        (DELETE_SCIP_SYMBOLS_SCHEMA_VERSIONS_QUERY, false),
    ] {
        let mut query = sqlx::query(statement).bind(upload_ids);
        if binds_twice {
            query = query.bind(upload_ids);
        }
        query.execute(&mut *tx).await?;
    }
    tx.commit().await?;

    sqlx::query(DELETE_LAST_RECONCILE_QUERY)
        .bind(upload_ids)
        .execute(pool)
        .await?;
    Ok(())
}

/// `store.DeleteAbandonedSchemaVersionsRecords`; returns the summed count.
pub async fn delete_abandoned_schema_versions_records(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let first: i64 = sqlx::query(DELETE_ABANDONED_SYMBOLS_SCHEMA_VERSIONS_QUERY)
        .fetch_one(&mut *tx)
        .await?
        .try_get(0)?;
    let second: i64 = sqlx::query(DELETE_ABANDONED_DOCUMENT_LOOKUP_SCHEMA_VERSIONS_QUERY)
        .fetch_one(&mut *tx)
        .await?
        .try_get(0)?;
    tx.commit().await?;
    Ok(first + second)
}

/// `store.DeleteUnreferencedDocuments`; returns `(numScanned, numDeleted)`.
pub async fn delete_unreferenced_documents(
    pool: &PgPool,
    batch_size: i64,
    max_age: Duration,
    now: OffsetDateTime,
) -> Result<(i64, i64), sqlx::Error> {
    let row = sqlx::query(DELETE_UNREFERENCED_DOCUMENTS_QUERY)
        .bind(now)
        .bind(max_age.as_secs() as i64)
        .bind(batch_size)
        .fetch_one(pool)
        .await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}

//
// The SQL below is transcribed from
// `internal/codeintel/codegraph/cleanup.go`.
//

const IDS_WITH_META_QUERY: &str = r#"
SELECT m.upload_id
FROM codeintel_scip_metadata m
WHERE m.upload_id = ANY($1)
"#;

const RECONCILE_CANDIDATES_QUERY: &str = r#"
WITH
unscanned_candidates AS (
	SELECT m.upload_id
	FROM codeintel_scip_metadata m
	WHERE NOT EXISTS (SELECT 1 FROM codeintel_last_reconcile lr WHERE lr.dump_id = m.upload_id)
	ORDER BY m.upload_id
),
scanned_candidates AS (
	SELECT lr.dump_id AS upload_id
	FROM codeintel_last_reconcile lr
	ORDER BY lr.last_reconcile_at, lr.dump_id
),
ordered_candidates AS (
	(
		SELECT upload_id FROM unscanned_candidates
		LIMIT $1
	) UNION ALL (
		SELECT upload_id FROM scanned_candidates
	)
	LIMIT $2
)
INSERT INTO codeintel_last_reconcile
SELECT upload_id, $3 FROM ordered_candidates
ON CONFLICT (dump_id) DO UPDATE
SET last_reconcile_at = $4
RETURNING dump_id
"#;

const DELETE_SCIP_DOCUMENT_LOOKUP_QUERY: &str = r#"
WITH
locked_document_lookup AS (
	SELECT id
	FROM codeintel_scip_document_lookup
	WHERE upload_id = ANY($1)
	ORDER BY id
	FOR UPDATE
)
DELETE FROM codeintel_scip_document_lookup
WHERE id IN (SELECT id FROM locked_document_lookup)
"#;

const DELETE_SCIP_METADATA_QUERY: &str = r#"
WITH
 locked_metadata AS (
 	SELECT id
 	FROM codeintel_scip_metadata
 	WHERE upload_id = ANY($1)
 	ORDER BY id
 	FOR UPDATE
 )
DELETE FROM codeintel_scip_metadata
WHERE id IN (SELECT id FROM locked_metadata)
"#;

const DELETE_SCIP_SYMBOL_NAMES_QUERY: &str = r#"
WITH
locked_symbol_names AS (
	SELECT id
	FROM codeintel_scip_symbol_names
	WHERE upload_id = ANY($1)
	ORDER BY id
	FOR UPDATE
)
DELETE FROM codeintel_scip_symbol_names
WHERE upload_id = ANY($2) AND id IN (SELECT id FROM locked_symbol_names)
"#;

const DELETE_SCIP_DOCUMENT_LOOKUP_SCHEMA_VERSIONS_QUERY: &str = r#"
DELETE FROM codeintel_scip_document_lookup_schema_versions WHERE upload_id = ANY($1)
"#;

const DELETE_SCIP_SYMBOLS_SCHEMA_VERSIONS_QUERY: &str = r#"
DELETE FROM codeintel_scip_symbols_schema_versions WHERE upload_id = ANY($1)
"#;

const DELETE_LAST_RECONCILE_QUERY: &str = r#"
WITH locked_rows AS (
	SELECT dump_id
	FROM codeintel_last_reconcile
	WHERE dump_id = ANY($1)
	ORDER BY dump_id
	FOR UPDATE
)
DELETE FROM codeintel_last_reconcile
WHERE dump_id IN (SELECT dump_id FROM locked_rows)
"#;

const DELETE_ABANDONED_SYMBOLS_SCHEMA_VERSIONS_QUERY: &str = r#"
WITH del AS (
	DELETE FROM codeintel_scip_symbols_schema_versions sv
	WHERE NOT EXISTS (
		SELECT 1
		FROM codeintel_scip_metadata m
		WHERE m.upload_id = sv.upload_id
	)
	RETURNING 1
)
SELECT COUNT(*) FROM del
"#;

const DELETE_ABANDONED_DOCUMENT_LOOKUP_SCHEMA_VERSIONS_QUERY: &str = r#"
WITH del AS (
	DELETE FROM codeintel_scip_document_lookup_schema_versions sv
	WHERE NOT EXISTS (
		SELECT 1
		FROM codeintel_scip_metadata m
		WHERE m.upload_id = sv.upload_id
	)
	RETURNING 1
)
SELECT COUNT(*) FROM del
"#;

const DELETE_UNREFERENCED_DOCUMENTS_QUERY: &str = r#"
WITH
candidates AS (
	SELECT id, document_id
	FROM codeintel_scip_documents_dereference_logs log
	WHERE $1 - log.last_removal_time > ($2 * interval '1 second')
	ORDER BY last_removal_time DESC, document_id
	LIMIT $3
	FOR UPDATE SKIP LOCKED
),
locked_documents AS (
	SELECT sd.id
	FROM candidates d
	JOIN codeintel_scip_documents sd ON sd.id = d.document_id
	WHERE NOT EXISTS (SELECT 1 FROM codeintel_scip_document_lookup sdl WHERE sdl.document_id = sd.id)
	ORDER BY sd.id
	FOR UPDATE OF sd
),
deleted_documents AS (
	DELETE FROM codeintel_scip_documents
	WHERE id IN (SELECT id FROM locked_documents)
	RETURNING id
),
deleted_candidates AS (
	DELETE FROM codeintel_scip_documents_dereference_logs
	WHERE id IN (SELECT id FROM candidates)
	RETURNING id
)
SELECT
	(SELECT COUNT(*) FROM candidates),
	(SELECT COUNT(*) FROM deleted_documents)
"#;
