#![forbid(unsafe_code)]

//! Port of `internal/oobmigration/migrations/codeintel/lsif/scip_migrator.go`
//! (ID 20): convert stored LSIF documents into canonical SCIP documents.
//!
//! The baseline uses `github.com/sourcegraph/scip/bindings/go/scip` for the
//! message types and `lib/codeintel/lsif/scip` for `ConvertLSIFDocument`. Here
//! the message types and `CanonicalizeDocument` come from the workspace
//! `sg-scip` crate; the LSIF -> SCIP conversion, the result-chunk cache, the
//! symbol trie and the writer are ported in this module.
//!
//! Note: the Rust runtime supplies a single pool per call, whereas the Go
//! migrator uses a frontend store for `lsif_uploads` and a code-intel store for
//! everything else. Both are addressed through the supplied pool here (in the
//! standalone deployment they are the same database).

mod support;
mod trie;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use sg_scip::canonicalize_document;
use sg_scip::scip::{
    Diagnostic as ScipDiagnostic, Document, Occurrence, Relationship, SymbolInformation, SymbolRole,
};
use sg_scip::Message;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::error::MigratorError;

use self::support::{
    encode_ranges, extract_language_from_indexer_name, extract_symbol_indexes, hash_key, sha256,
};
use self::trie::Trie;
use super::super::model::{Migrator, MigratorPools};
use super::gzip;
use super::serializer;
use super::types::{DocumentData, Id, MarshalledDocumentData, RangeData, ResultChunkData};

const PROGRESS_QUERY: &str = r#"
SELECT CASE c1.count + c2.count WHEN 0 THEN 1 ELSE
	cast(c1.count as float) / cast((c1.count + c2.count) as float)
END FROM
	(SELECT COUNT(*) as count FROM codeintel_scip_metadata) c1,
	(SELECT COUNT(*) as count FROM lsif_data_metadata) c2
"#;

const SELECT_FOR_MIGRATION_QUERY: &str = r#"
SELECT dump_id
FROM lsif_data_metadata
ORDER BY dump_id
FOR UPDATE SKIP LOCKED
LIMIT 1
"#;

const MARK_REINDEXABLE_QUERY: &str = r#"
UPDATE lsif_uploads
SET should_reindex = true
WHERE id = $1
"#;

const INDEXER_QUERY: &str = r#"
SELECT indexer
FROM lsif_uploads
WHERE id = $1
"#;

const READ_METADATA_QUERY: &str = r#"
SELECT num_result_chunks
FROM lsif_data_metadata
WHERE dump_id = $1
"#;

const SCAN_DOCUMENTS_QUERY: &str = r#"
SELECT
	path,
	ranges,
	hovers,
	monikers,
	packages,
	diagnostics
FROM lsif_data_documents
WHERE dump_id = $1
ORDER BY path
LIMIT $2
OFFSET $3
"#;

const WRITE_METADATA_QUERY: &str = r#"
INSERT INTO codeintel_scip_metadata (upload_id, text_document_encoding, tool_name, tool_version, tool_arguments, protocol_version)
VALUES ($1, '', '', '', '{}', 1)
"#;

const SCAN_RESULT_CHUNKS_QUERY: &str = r#"
SELECT
	idx,
	data
FROM lsif_data_result_chunks
WHERE
	dump_id = $1 AND
	idx = ANY($2)
"#;

const CREATE_TEMPORARY_SYMBOL_NAMES_TABLE_QUERY: &str = r#"
CREATE TEMPORARY TABLE t_codeintel_scip_symbol_names (
	id integer NOT NULL,
	name_segment text NOT NULL,
	prefix_id integer
) ON COMMIT DROP
"#;

const CREATE_TEMPORARY_SYMBOLS_TABLE_QUERY: &str = r#"
CREATE TEMPORARY TABLE t_codeintel_scip_symbols (
	symbol_id integer NOT NULL,
	document_lookup_id integer NOT NULL,
	definition_ranges bytea,
	reference_ranges bytea,
	implementation_ranges bytea
) ON COMMIT DROP
"#;

const INSERT_DOCUMENT_QUERY: &str = r#"
INSERT INTO codeintel_scip_documents (schema_version, payload_hash, raw_scip_payload)
VALUES ($1, $2, $3)
RETURNING id
"#;

const INSERT_DOCUMENT_LOOKUP_QUERY: &str = r#"
INSERT INTO codeintel_scip_document_lookup (upload_id, document_path, document_id)
VALUES ($1, $2, $3)
RETURNING id
"#;

const INSERT_SYMBOL_NAME_QUERY: &str = r#"
INSERT INTO t_codeintel_scip_symbol_names (id, name_segment, prefix_id)
VALUES ($1, $2, $3)
"#;

const INSERT_SYMBOL_QUERY: &str = r#"
INSERT INTO t_codeintel_scip_symbols (
	symbol_id,
	document_lookup_id,
	definition_ranges,
	reference_ranges,
	implementation_ranges
) VALUES ($1, $2, $3, $4, $5)
"#;

const FLUSH_SYMBOL_NAMES_QUERY: &str = r#"
INSERT INTO codeintel_scip_symbol_names (
	upload_id,
	id,
	name_segment,
	prefix_id
)
SELECT
	$1,
	source.id,
	source.name_segment,
	source.prefix_id
FROM t_codeintel_scip_symbol_names source
ON CONFLICT DO NOTHING
"#;

const FLUSH_SYMBOLS_QUERY: &str = r#"
INSERT INTO codeintel_scip_symbols (
	upload_id,
	symbol_id,
	document_lookup_id,
	schema_version,
	definition_ranges,
	reference_ranges,
	implementation_ranges
)
SELECT
	$1,
	source.symbol_id,
	source.document_lookup_id,
	1,
	source.definition_ranges,
	source.reference_ranges,
	source.implementation_ranges
FROM t_codeintel_scip_symbols source
ON CONFLICT DO NOTHING
"#;

const DELETE_LSIF_DATA_QUERY: &str = "DELETE FROM {table} WHERE dump_id = $1";

const LSIF_TABLE_NAMES: [&str; 6] = [
    "lsif_data_metadata",
    "lsif_data_documents",
    "lsif_data_result_chunks",
    "lsif_data_definitions",
    "lsif_data_references",
    "lsif_data_implementations",
];

// Configuration values from `scip_migrator.go` (`SCIP_MIGRATOR_*`
// environment variables default to these).
const UPLOAD_READER_BATCH_SIZE: usize = 32;
const RESULT_CHUNK_READER_CACHE_SIZE: i32 = 8192;
const DOCUMENT_READER_BATCH_SIZE: i64 = 64;
const DOCUMENT_WRITER_BATCH_SIZE: usize = 256;
const DOCUMENT_WRITER_MAX_PAYLOAD_SUM: usize = 1024 * 1024 * 32;
const MAX_DEFINITIONS_PER_DEFINITION_RESULT: usize = 16;

/// `scipMigrator`, ID 20.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScipMigrator;

#[async_trait]
impl Migrator for ScipMigrator {
    fn id(&self) -> i32 {
        20
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn progress(&self, db: &PgPool, apply_reverse: bool) -> Result<f64, MigratorError> {
        if apply_reverse {
            // A downgrade loses access to SCIP records but leaves them on disk.
            return Ok(0.0);
        }

        sqlx::query_scalar::<_, f64>(PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("scip migration progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        self.run_up(db, db).await
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Reverse migration is a no-op; progress reports 0 immediately.
        Ok(())
    }

    async fn progress_with_pools(
        &self,
        pools: &MigratorPools,
        apply_reverse: bool,
    ) -> Result<f64, MigratorError> {
        self.progress(pools.codeintel_or_frontend(), apply_reverse)
            .await
    }

    async fn up_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.run_up(&pools.frontend, pools.codeintel_or_frontend())
            .await
    }

    async fn down_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.down(pools.codeintel_or_frontend()).await
    }
}

impl ScipMigrator {
    async fn run_up(&self, frontend: &PgPool, codeintel: &PgPool) -> Result<(), MigratorError> {
        // Each invocation processes at most one upload-reader batch, mirroring
        // the baseline's closed token channel.
        for _ in 0..UPLOAD_READER_BATCH_SIZE {
            if !self.up_single(frontend, codeintel).await? {
                break;
            }
        }
        Ok(())
    }

    async fn up_single(
        &self,
        frontend: &PgPool,
        codeintel: &PgPool,
    ) -> Result<bool, MigratorError> {
        let mut tx = codeintel
            .begin()
            .await
            .map_err(|error| MigratorError::new(format!("scip migration begin: {error}")))?;

        let upload_id: Option<i32> = sqlx::query_scalar(SELECT_FOR_MIGRATION_QUERY)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| MigratorError::new(format!("scip migration select: {error}")))?;

        let Some(upload_id) = upload_id else {
            tx.commit()
                .await
                .map_err(|error| MigratorError::new(format!("scip migration commit: {error}")))?;
            return Ok(false);
        };

        if let Err(error) = self.migrate_upload(&mut tx, frontend, upload_id).await {
            return Err(MigratorError::new(format!(
                "failed to migrate upload {upload_id}: {error}"
            )));
        }

        tx.commit()
            .await
            .map_err(|error| MigratorError::new(format!("scip migration commit: {error}")))?;
        Ok(true)
    }

    async fn migrate_upload(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        frontend: &PgPool,
        upload_id: i32,
    ) -> Result<(), MigratorError> {
        // `lsif_uploads` lives in the frontend database; everything else in
        // this migration lives in the code-intelligence database.
        let indexer_name: Option<Option<String>> = sqlx::query_scalar(INDEXER_QUERY)
            .bind(upload_id)
            .fetch_optional(frontend)
            .await
            .map_err(|error| MigratorError::new(format!("scip indexer: {error}")))?;
        let indexer_name = indexer_name.flatten().unwrap_or_default();

        let num_result_chunks: Option<i32> = sqlx::query_scalar(READ_METADATA_QUERY)
            .bind(upload_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| MigratorError::new(format!("scip metadata: {error}")))?;
        let Some(num_result_chunks) = num_result_chunks else {
            return Ok(());
        };

        let mut cache_size = RESULT_CHUNK_READER_CACHE_SIZE;
        if num_result_chunks < cache_size {
            cache_size = num_result_chunks;
        }
        let mut cache: HashMap<i32, ResultChunkData> = HashMap::new();

        // Warm the cache when every chunk fits.
        if num_result_chunks <= cache_size {
            let ids: Vec<i32> = (0..num_result_chunks).collect();
            let rows = sqlx::query(SCAN_RESULT_CHUNKS_QUERY)
                .bind(upload_id)
                .bind(ids)
                .fetch_all(&mut **tx)
                .await
                .map_err(|error| MigratorError::new(format!("scip warm chunks: {error}")))?;
            for row in rows {
                let index: i32 = row
                    .try_get("idx")
                    .map_err(|error| MigratorError::new(format!("scip chunk idx: {error}")))?;
                let data: Option<Vec<u8>> = row
                    .try_get("data")
                    .map_err(|error| MigratorError::new(format!("scip chunk data: {error}")))?;
                let chunk = serializer::unmarshal_result_chunk_data(data.as_deref().unwrap_or(&[]))
                    .map_err(|error| MigratorError::new(format!("scip chunk decode: {error}")))?;
                cache.insert(index, chunk);
            }
        }

        sqlx::query(CREATE_TEMPORARY_SYMBOL_NAMES_TABLE_QUERY)
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!("scip temporary symbol names table: {error}"))
            })?;
        sqlx::query(CREATE_TEMPORARY_SYMBOLS_TABLE_QUERY)
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!("scip temporary symbols table: {error}"))
            })?;

        let mut writer = Writer::new(upload_id);

        let mut page: i64 = 0;
        loop {
            let rows = sqlx::query(SCAN_DOCUMENTS_QUERY)
                .bind(upload_id)
                .bind(DOCUMENT_READER_BATCH_SIZE)
                .bind(page * DOCUMENT_READER_BATCH_SIZE)
                .fetch_all(&mut **tx)
                .await
                .map_err(|error| MigratorError::new(format!("scip documents: {error}")))?;
            page += 1;

            if rows.is_empty() {
                break;
            }

            let mut documents: BTreeMap<String, DocumentData> = BTreeMap::new();
            for row in rows {
                let path: String = row
                    .try_get("path")
                    .map_err(|error| MigratorError::new(format!("scip document path: {error}")))?;
                let read = |column: &str| -> Result<Vec<u8>, MigratorError> {
                    let value: Option<Vec<u8>> = row.try_get(column).map_err(|error| {
                        MigratorError::new(format!("scip document {column}: {error}"))
                    })?;
                    Ok(value.unwrap_or_default())
                };
                let columns = MarshalledDocumentData {
                    ranges: read("ranges")?,
                    hover_results: read("hovers")?,
                    monikers: read("monikers")?,
                    package_information: read("packages")?,
                    diagnostics: read("diagnostics")?,
                };
                let document = serializer::unmarshal_document_data(&columns).map_err(|error| {
                    MigratorError::new(format!("scip document decode: {error}"))
                })?;
                documents.insert(path, document);
            }

            let paths: Vec<String> = documents.keys().cloned().collect();
            let result_ids: Vec<Vec<Id>> = paths
                .iter()
                .map(|path| extract_result_ids(&documents[path]))
                .collect();

            for (index, path) in paths.iter().enumerate() {
                let scip_document = process_document(
                    tx,
                    &mut cache,
                    cache_size,
                    upload_id,
                    num_result_chunks,
                    &indexer_name,
                    path,
                    &documents[path],
                    &result_ids[index],
                    &result_ids[index + 1..],
                )
                .await?;

                writer
                    .insert_document(tx, path.clone(), scip_document)
                    .await?;
            }
        }

        sqlx::query(WRITE_METADATA_QUERY)
            .bind(upload_id)
            .execute(&mut **tx)
            .await
            .map_err(|error| MigratorError::new(format!("scip write metadata: {error}")))?;

        writer.flush_all(tx).await?;

        for table in LSIF_TABLE_NAMES {
            let query = DELETE_LSIF_DATA_QUERY.replace("{table}", table);
            sqlx::query(&query)
                .bind(upload_id)
                .execute(&mut **tx)
                .await
                .map_err(|error| MigratorError::new(format!("scip delete {table}: {error}")))?;
        }

        sqlx::query(MARK_REINDEXABLE_QUERY)
            .bind(upload_id)
            .execute(frontend)
            .await
            .map_err(|error| MigratorError::new(format!("scip mark reindexable: {error}")))?;

        Ok(())
    }
}

/// `extractResultIDs`: unique definition/implementation result identifiers
/// attached to the document's ranges.
fn extract_result_ids(document: &DocumentData) -> Vec<Id> {
    let mut ids = BTreeSet::new();
    for range in document.ranges.values() {
        if !range.definition_result_id.is_empty() {
            ids.insert(range.definition_result_id.clone());
        }
        if !range.implementation_result_id.is_empty() {
            ids.insert(range.implementation_result_id.clone());
        }
    }
    ids.into_iter().collect()
}

#[allow(clippy::too_many_arguments)]
async fn process_document(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut HashMap<i32, ResultChunkData>,
    cache_size: i32,
    upload_id: i32,
    num_result_chunks: i32,
    indexer_name: &str,
    path: &str,
    document: &DocumentData,
    result_ids: &[Id],
    preload_result_ids: &[Vec<Id>],
) -> Result<Document, MigratorError> {
    let result_chunks = fetch_result_chunks(
        tx,
        cache,
        cache_size,
        upload_id,
        num_result_chunks,
        result_ids,
        preload_result_ids,
    )
    .await?;

    let target_range_fetcher = |result_id: &str| -> Vec<Id> {
        if result_id.is_empty() {
            return Vec::new();
        }
        let index = hash_key(result_id, num_result_chunks);
        let Some(chunk) = result_chunks.get(&index) else {
            return Vec::new();
        };
        chunk
            .document_id_range_ids
            .get(result_id)
            .map(|pairs| pairs.iter().map(|pair| pair.range_id.clone()).collect())
            .unwrap_or_default()
    };

    let mut scip_document = convert_lsif_document(
        upload_id,
        &target_range_fetcher,
        indexer_name,
        path,
        document,
    );
    canonicalize_document(&mut scip_document);
    Ok(scip_document)
}

/// `fetchResultChunks`.
async fn fetch_result_chunks(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut HashMap<i32, ResultChunkData>,
    cache_size: i32,
    upload_id: i32,
    num_result_chunks: i32,
    ids: &[Id],
    preload_ids: &[Vec<Id>],
) -> Result<HashMap<i32, ResultChunkData>, MigratorError> {
    let mut index_map: HashMap<i32, bool> = HashMap::new();
    let mut result_chunks: HashMap<i32, ResultChunkData> = HashMap::new();

    let batches = std::iter::once(ids).chain(preload_ids.iter().map(Vec::as_slice));
    'outer: for (batch_index, batch_ids) in batches.enumerate() {
        for id in batch_ids {
            if index_map.len() as i32 >= cache_size && batch_index != 0 {
                break 'outer;
            }

            let index = hash_key(id, num_result_chunks);
            if result_chunks.contains_key(&index) {
                continue;
            }

            if let Some(cached) = cache.get(&index) {
                if batch_index == 0 {
                    result_chunks.insert(index, cached.clone());
                }
            } else {
                let entry = index_map.entry(index).or_insert(false);
                *entry = batch_index == 0 || *entry;
            }
        }
    }

    if !index_map.is_empty() {
        let indexes: Vec<i32> = index_map.keys().copied().collect();
        let rows = sqlx::query(SCAN_RESULT_CHUNKS_QUERY)
            .bind(upload_id)
            .bind(indexes)
            .fetch_all(&mut **tx)
            .await
            .map_err(|error| MigratorError::new(format!("scip result chunks: {error}")))?;

        for row in rows {
            let index: i32 = row
                .try_get("idx")
                .map_err(|error| MigratorError::new(format!("scip chunk idx: {error}")))?;
            let data: Option<Vec<u8>> = row
                .try_get("data")
                .map_err(|error| MigratorError::new(format!("scip chunk data: {error}")))?;
            let chunk = serializer::unmarshal_result_chunk_data(data.as_deref().unwrap_or(&[]))
                .map_err(|error| MigratorError::new(format!("scip chunk decode: {error}")))?;

            if index_map.get(&index).copied().unwrap_or(false) {
                result_chunks.insert(index, chunk.clone());
            }
            cache.insert(index, chunk);
        }
    }

    Ok(result_chunks)
}

/// `ConvertLSIFDocument`.
fn convert_lsif_document(
    upload_id: i32,
    target_range_fetcher: &dyn Fn(&str) -> Vec<Id>,
    indexer_name: &str,
    path: &str,
    document: &DocumentData,
) -> Document {
    let n = document.ranges.len();
    let mut occurrences: Vec<Occurrence> = Vec::with_capacity(n);
    let mut documentation_by_symbol: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut interfaces_by_symbol: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for (range_id, range) in &document.ranges {
        let (range_occurrences, symbols) =
            convert_range(upload_id, target_range_fetcher, document, range_id, range);
        occurrences.extend(range_occurrences);

        for symbol in symbols {
            documentation_by_symbol
                .entry(symbol.name.clone())
                .or_default()
                .insert(symbol.documentation);
            for interface_name in symbol.implementation_relationships {
                interfaces_by_symbol
                    .entry(symbol.name.clone())
                    .or_default()
                    .insert(interface_name);
            }
        }
    }

    for diagnostic in &document.diagnostics {
        occurrences.push(convert_diagnostic(diagnostic));
    }

    let mut symbol_map: BTreeMap<String, SymbolInformation> = documentation_by_symbol
        .into_iter()
        .map(|(name, documentation)| {
            let documentation = documentation
                .into_iter()
                .filter(|entry| !entry.is_empty())
                .collect();
            (
                name.clone(),
                SymbolInformation {
                    symbol: name,
                    documentation,
                    ..SymbolInformation::default()
                },
            )
        })
        .collect();

    for (symbol_name, interface_names) in interfaces_by_symbol {
        let interface_names: Vec<String> = interface_names.into_iter().collect();
        {
            let symbol =
                symbol_map
                    .entry(symbol_name.clone())
                    .or_insert_with(|| SymbolInformation {
                        symbol: symbol_name.clone(),
                        ..SymbolInformation::default()
                    });
            for interface_name in &interface_names {
                symbol.relationships.push(Relationship {
                    symbol: interface_name.clone(),
                    is_implementation: true,
                    ..Relationship::default()
                });
            }
        }
        for interface_name in interface_names {
            symbol_map
                .entry(interface_name.clone())
                .or_insert_with(|| SymbolInformation {
                    symbol: interface_name,
                    ..SymbolInformation::default()
                });
        }
    }

    Document {
        language: extract_language_from_indexer_name(indexer_name),
        relative_path: path.to_string(),
        occurrences,
        symbols: symbol_map.into_values().collect(),
        ..Document::default()
    }
}

struct SymbolMetadata {
    name: String,
    documentation: String,
    implementation_relationships: Vec<String>,
}

/// `convertRange`.
fn convert_range(
    upload_id: i32,
    target_range_fetcher: &dyn Fn(&str) -> Vec<Id>,
    document: &DocumentData,
    range_id: &str,
    range: &RangeData,
) -> (Vec<Occurrence>, Vec<SymbolMetadata>) {
    let mut monikers: Vec<String> = Vec::new();
    let mut implements_monikers: Vec<String> = Vec::new();

    for moniker_id in &range.moniker_ids {
        let Some(moniker) = document.monikers.get(moniker_id) else {
            continue;
        };
        let Some(package_information) = document
            .package_information
            .get(&moniker.package_information_id)
        else {
            continue;
        };

        let manager = if package_information.manager.is_empty() {
            "."
        } else {
            &package_information.manager
        };
        let symbol_name = format!(
            "{} {} {} {} `{}`.",
            moniker.scheme,
            manager,
            package_information.name,
            package_information.version,
            moniker.identifier.replace('`', "``"),
        );

        match moniker.kind.as_str() {
            "import" | "export" => monikers.push(symbol_name),
            "implementation" => implements_monikers.push(symbol_name),
            _ => {}
        }
    }

    for target_range_id in target_range_fetcher(&range.implementation_result_id) {
        implements_monikers.push(construct_symbol_name(upload_id, &target_range_id));
    }

    let hover_documentation = document
        .hover_results
        .get(&range.hover_result_id)
        .cloned()
        .unwrap_or_default();

    let mut occurrences = Vec::new();
    let mut symbols = Vec::new();
    let definition_role = SymbolRole::Definition as i32;
    let reference_role = SymbolRole::UnspecifiedSymbolRole as i32;

    let is_definition = target_range_fetcher(&range.definition_result_id)
        .iter()
        .any(|target| target == range_id);

    if is_definition {
        push_occurrence(
            &mut occurrences,
            &mut symbols,
            range,
            construct_symbol_name(upload_id, range_id),
            definition_role,
            &hover_documentation,
            &implements_monikers,
        );
        for moniker in &monikers {
            push_occurrence(
                &mut occurrences,
                &mut symbols,
                range,
                moniker.clone(),
                definition_role,
                &hover_documentation,
                &implements_monikers,
            );
        }
    } else {
        let mut target_ranges = target_range_fetcher(&range.definition_result_id);
        target_ranges.sort();
        target_ranges.truncate(MAX_DEFINITIONS_PER_DEFINITION_RESULT);
        for target_range_id in target_ranges {
            push_occurrence(
                &mut occurrences,
                &mut symbols,
                range,
                construct_symbol_name(upload_id, &target_range_id),
                reference_role,
                &hover_documentation,
                &implements_monikers,
            );
        }
        for moniker in &monikers {
            push_occurrence(
                &mut occurrences,
                &mut symbols,
                range,
                moniker.clone(),
                reference_role,
                &hover_documentation,
                &implements_monikers,
            );
        }
    }

    (occurrences, symbols)
}

#[allow(clippy::too_many_arguments)]
fn push_occurrence(
    occurrences: &mut Vec<Occurrence>,
    symbols: &mut Vec<SymbolMetadata>,
    range: &RangeData,
    symbol_name: String,
    role: i32,
    hover_documentation: &str,
    implements_monikers: &[String],
) {
    occurrences.push(Occurrence {
        range: vec![
            range.start_line,
            range.start_character,
            range.end_line,
            range.end_character,
        ],
        symbol: symbol_name.clone(),
        symbol_roles: role,
        ..Occurrence::default()
    });
    symbols.push(SymbolMetadata {
        name: symbol_name,
        documentation: hover_documentation.to_string(),
        implementation_relationships: implements_monikers.to_vec(),
    });
}

/// `convertDiagnostic`.
fn convert_diagnostic(diagnostic: &super::types::DiagnosticData) -> Occurrence {
    Occurrence {
        range: vec![
            diagnostic.start_line,
            diagnostic.start_character,
            diagnostic.end_line,
            diagnostic.end_character,
        ],
        diagnostics: vec![ScipDiagnostic {
            severity: diagnostic.severity,
            code: diagnostic.code.clone(),
            message: diagnostic.message.clone(),
            source: diagnostic.source.clone(),
            tags: Vec::new(),
        }],
        ..Occurrence::default()
    }
}

/// `constructSymbolName`.
fn construct_symbol_name(upload_id: i32, result_id: &str) -> String {
    if result_id.is_empty() {
        return String::new();
    }
    format!("lsif . {upload_id} . `{result_id}`.")
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

struct BufferedDocument {
    path: String,
    scip_document: Document,
    payload: Vec<u8>,
    payload_hash: Vec<u8>,
}

struct Writer {
    upload_id: i32,
    next_id: i32,
    batch_payload_sum: usize,
    batch: Vec<BufferedDocument>,
}

impl Writer {
    fn new(upload_id: i32) -> Self {
        Self {
            upload_id,
            next_id: 0,
            batch_payload_sum: 0,
            batch: Vec::new(),
        }
    }

    /// `InsertDocument`: buffer a canonical document.
    async fn insert_document(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        path: String,
        scip_document: Document,
    ) -> Result<(), MigratorError> {
        if self.batch_payload_sum >= DOCUMENT_WRITER_MAX_PAYLOAD_SUM {
            self.flush_batch(tx).await?;
        }

        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);
        let unique_prefix = format!("lsif-{}:{}:", self.upload_id, millis);

        let payload = scip_document.encode_to_vec();
        let compressed = gzip::compress(&payload);
        let mut payload_hash = unique_prefix.into_bytes();
        payload_hash.extend_from_slice(&sha256(&payload));

        self.batch_payload_sum += compressed.len();
        self.batch.push(BufferedDocument {
            path,
            scip_document,
            payload: compressed,
            payload_hash,
        });

        if self.batch.len() >= DOCUMENT_WRITER_BATCH_SIZE {
            self.flush_batch(tx).await?;
        }

        Ok(())
    }

    /// Write the buffered documents, lookups and symbol rows.
    async fn flush_batch(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), MigratorError> {
        let documents = std::mem::take(&mut self.batch);
        self.batch_payload_sum = 0;
        if documents.is_empty() {
            return Ok(());
        }

        let mut document_ids = Vec::with_capacity(documents.len());
        for document in &documents {
            let id: i32 = sqlx::query(INSERT_DOCUMENT_QUERY)
                .bind(1_i32)
                .bind(&document.payload_hash)
                .bind(&document.payload)
                .fetch_one(&mut **tx)
                .await
                .map_err(|error| MigratorError::new(format!("scip insert document: {error}")))?
                .try_get("id")
                .map_err(|error| MigratorError::new(format!("scip document id: {error}")))?;
            document_ids.push(id);
        }

        let mut document_lookup_ids = Vec::with_capacity(documents.len());
        for (index, document) in documents.iter().enumerate() {
            let id: i32 = sqlx::query(INSERT_DOCUMENT_LOOKUP_QUERY)
                .bind(self.upload_id)
                .bind(&document.path)
                .bind(document_ids[index])
                .fetch_one(&mut **tx)
                .await
                .map_err(|error| {
                    MigratorError::new(format!("scip insert document lookup: {error}"))
                })?
                .try_get("id")
                .map_err(|error| MigratorError::new(format!("scip lookup id: {error}")))?;
            document_lookup_ids.push(id);
        }

        let mut symbol_names = BTreeSet::new();
        let mut inverted_range_indexes = Vec::with_capacity(documents.len());
        for document in &documents {
            let indexes = extract_symbol_indexes(&document.scip_document);
            for index in &indexes {
                symbol_names.insert(index.symbol_name.clone());
            }
            inverted_range_indexes.push(indexes);
        }

        let names: Vec<String> = symbol_names.into_iter().collect();
        let (trie, next_id) = Trie::new(&names, self.next_id);
        self.next_id = next_id;

        let mut symbol_name_by_id: HashMap<i32, String> = HashMap::new();
        let mut ids_by_symbol_name: HashMap<String, i32> = HashMap::new();
        let mut visited: Vec<(i32, Option<i32>, String)> = Vec::new();
        trie.traverse(|id, parent_id, prefix| {
            visited.push((id, parent_id, prefix.to_string()));
            Ok(())
        })
        .map_err(|error| MigratorError::new(format!("scip trie: {error}")))?;

        for (id, parent_id, prefix) in visited {
            let name = match parent_id {
                None => prefix.clone(),
                Some(parent_id) => {
                    let parent = symbol_name_by_id.get(&parent_id).ok_or_else(|| {
                        MigratorError::new(format!(
                            "malformed trie - expected prefix with id={parent_id} to exist"
                        ))
                    })?;
                    format!("{parent}{prefix}")
                }
            };
            symbol_name_by_id.insert(id, name.clone());
            ids_by_symbol_name.insert(name, id);

            sqlx::query(INSERT_SYMBOL_NAME_QUERY)
                .bind(id)
                .bind(&prefix)
                .bind(parent_id)
                .execute(&mut **tx)
                .await
                .map_err(|error| MigratorError::new(format!("scip insert symbol name: {error}")))?;
        }

        for (index, indexes) in inverted_range_indexes.iter().enumerate() {
            for inverted in indexes {
                let definition_ranges =
                    encode_ranges(&inverted.definition_ranges).map_err(|error| {
                        MigratorError::new(format!("scip encode definition ranges: {error}"))
                    })?;
                let reference_ranges =
                    encode_ranges(&inverted.reference_ranges).map_err(|error| {
                        MigratorError::new(format!("scip encode reference ranges: {error}"))
                    })?;
                let implementation_ranges = encode_ranges(&inverted.implementation_ranges)
                    .map_err(|error| {
                        MigratorError::new(format!("scip encode implementation ranges: {error}"))
                    })?;

                let symbol_id = ids_by_symbol_name
                    .get(&inverted.symbol_name)
                    .copied()
                    .ok_or_else(|| {
                        MigratorError::new(format!(
                            "malformed trie - expected {:?} to be a member",
                            inverted.symbol_name
                        ))
                    })?;

                // Go's `ranges.EncodeRanges` returns nil for an empty range
                // set, which is stored as SQL NULL; preserve that.
                let null_if_empty = |bytes: Vec<u8>| {
                    if bytes.is_empty() {
                        None
                    } else {
                        Some(bytes)
                    }
                };

                sqlx::query(INSERT_SYMBOL_QUERY)
                    .bind(symbol_id)
                    .bind(document_lookup_ids[index])
                    .bind(null_if_empty(definition_ranges))
                    .bind(null_if_empty(reference_ranges))
                    .bind(null_if_empty(implementation_ranges))
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| MigratorError::new(format!("scip insert symbol: {error}")))?;
            }
        }

        Ok(())
    }

    /// `Flush`: flush buffered documents and move temporary symbol rows into
    /// the permanent tables.
    async fn flush_all(&mut self, tx: &mut Transaction<'_, Postgres>) -> Result<(), MigratorError> {
        self.flush_batch(tx).await?;

        sqlx::query(FLUSH_SYMBOL_NAMES_QUERY)
            .bind(self.upload_id)
            .execute(&mut **tx)
            .await
            .map_err(|error| MigratorError::new(format!("scip flush symbol names: {error}")))?;
        sqlx::query(FLUSH_SYMBOLS_QUERY)
            .bind(self.upload_id)
            .execute(&mut **tx)
            .await
            .map_err(|error| MigratorError::new(format!("scip flush symbols: {error}")))?;

        Ok(())
    }
}

/// The registered implementation for ID 20.
pub fn migrators() -> Vec<Arc<dyn Migrator>> {
    vec![Arc::new(ScipMigrator)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructs_symbol_names() {
        assert_eq!(construct_symbol_name(5, "42"), "lsif . 5 . `42`.");
        assert_eq!(construct_symbol_name(5, ""), "");
    }

    #[test]
    fn extracts_result_ids_from_ranges() {
        let mut document = DocumentData::default();
        document.ranges.insert(
            "1".to_string(),
            RangeData {
                definition_result_id: "10".to_string(),
                implementation_result_id: "20".to_string(),
                ..RangeData::default()
            },
        );
        document.ranges.insert(
            "2".to_string(),
            RangeData {
                definition_result_id: "10".to_string(),
                ..RangeData::default()
            },
        );

        assert_eq!(extract_result_ids(&document), vec!["10", "20"]);
    }

    #[test]
    fn converts_range_definition_and_references() {
        let mut document = DocumentData::default();
        document
            .hover_results
            .insert("h".to_string(), "doc".to_string());
        document.monikers.insert(
            "m".to_string(),
            super::super::types::MonikerData {
                kind: "export".to_string(),
                scheme: "gomod".to_string(),
                identifier: "pkg:Foo".to_string(),
                package_information_id: "p".to_string(),
            },
        );
        document.package_information.insert(
            "p".to_string(),
            super::super::types::PackageInformationData {
                manager: String::new(),
                name: "pkg".to_string(),
                version: "v1".to_string(),
            },
        );

        let range = RangeData {
            start_line: 1,
            start_character: 2,
            end_line: 1,
            end_character: 5,
            definition_result_id: "def".to_string(),
            hover_result_id: "h".to_string(),
            moniker_ids: vec!["m".to_string()],
            ..RangeData::default()
        };

        let fetcher = |id: &str| -> Vec<Id> {
            if id == "def" {
                vec!["r1".to_string()]
            } else {
                Vec::new()
            }
        };

        let (occurrences, symbols) = convert_range(7, &fetcher, &document, "r1", &range);

        // Definition occurrence plus the moniker occurrence.
        assert_eq!(occurrences.len(), 2);
        assert_eq!(occurrences[0].range, vec![1, 2, 1, 5]);
        assert_eq!(occurrences[0].symbol, "lsif . 7 . `r1`.");
        assert_eq!(occurrences[0].symbol_roles, 1);
        assert_eq!(occurrences[1].symbol, "gomod . pkg v1 `pkg:Foo`.");
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].documentation, "doc");
    }

    #[test]
    fn converts_reference_to_definition_targets() {
        let document = DocumentData::default();
        let range = RangeData {
            start_line: 3,
            start_character: 0,
            end_line: 3,
            end_character: 4,
            definition_result_id: "def".to_string(),
            ..RangeData::default()
        };
        let fetcher = |id: &str| -> Vec<Id> {
            if id == "def" {
                vec!["r1".to_string(), "r2".to_string()]
            } else {
                Vec::new()
            }
        };

        let (occurrences, _) = convert_range(1, &fetcher, &document, "r9", &range);
        assert_eq!(occurrences.len(), 2);
        assert_eq!(occurrences[0].symbol, "lsif . 1 . `r1`.");
        assert_eq!(occurrences[0].symbol_roles, 0);
    }

    #[test]
    fn scip_migrator_metadata() {
        let migrator = ScipMigrator;
        assert_eq!(migrator.id(), 20);
        assert_eq!(migrator.interval(), Duration::from_secs(1));
    }

    /// Progress math against a real database. Requires `DATABASE_URL`.
    #[tokio::test]
    #[ignore = "requires a PostgreSQL database and the code-intel tables"]
    async fn scip_progress_against_database() {
        use super::super::super::model::Migrator as _;

        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let pool = sqlx::PgPool::connect(&url).await.expect("connect");

        sqlx::query("DROP TABLE IF EXISTS codeintel_scip_metadata")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS lsif_data_metadata")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE codeintel_scip_metadata (upload_id integer PRIMARY KEY NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE lsif_data_metadata (dump_id integer NOT NULL, num_result_chunks integer NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO lsif_data_metadata (dump_id, num_result_chunks) VALUES (1, 0)")
            .execute(&pool)
            .await
            .unwrap();

        let migrator = ScipMigrator;
        assert_eq!(migrator.progress(&pool, false).await.unwrap(), 0.0);
        assert_eq!(migrator.progress(&pool, true).await.unwrap(), 0.0);

        sqlx::query("INSERT INTO codeintel_scip_metadata (upload_id) VALUES (1)")
            .execute(&pool)
            .await
            .unwrap();
        // One SCIP upload and one LSIF upload: 1 / (1 + 1).
        assert_eq!(migrator.progress(&pool, false).await.unwrap(), 0.5);

        sqlx::query("DROP TABLE codeintel_scip_metadata")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE lsif_data_metadata")
            .execute(&pool)
            .await
            .unwrap();
    }
}
