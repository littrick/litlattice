#![forbid(unsafe_code)]

//! Port of `internal/codeintel/codegraph`'s write path (`insert.go`) and
//! `writeSCIPDocuments` from the upload processor.
//!
//! Every write targets the **code-intel** database, and the whole index is
//! written inside one transaction:
//!
//! - `codeintel_scip_metadata` — one row per upload
//! - `codeintel_scip_documents` — canonical document payload, deduplicated by
//!   the sha256 of the uncompressed `Document` proto
//! - `codeintel_scip_document_lookup` — `(upload_id, path) → document_id`
//! - `codeintel_scip_symbol_names` — compressed prefix trie of symbol names
//! - `codeintel_scip_symbols` — per-document, per-symbol encoded ranges
//!
//! The baseline stages symbol rows in `ON COMMIT DROP` temporary tables and
//! copies them at `Flush`; this port inserts them straight into the target
//! tables inside the same transaction. The committed database state is
//! identical (the temporary tables are not observable outside the
//! transaction).

use std::collections::{BTreeSet, HashMap};
use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, QueryBuilder};

use sg_scip::scip::{Document, SymbolInformation};
use sg_scip::{is_local_symbol, Range};

use crate::ranges::{encode_ranges, RangeError};
use crate::scip_stream::{ProcessedMetadata, ProcessedPackageData, ScipDataStream};
use crate::trie::{new_trie, TrieNode};

/// `shared.SyntacticIndexer`: uploads produced by the syntactic indexer are
/// written without symbol rows (the trie cannot support fuzzy matching for
/// them).
pub const SYNTACTIC_INDEXER: &str = "scip-syntax";

/// `codeintel_scip_documents.schema_version` / `codeintel_scip_symbols.schema_version`.
pub const SCHEMA_VERSION: i32 = 1;

/// `DocumentsBatchSize`.
pub const DOCUMENTS_BATCH_SIZE: usize = 256;

/// `MaxBatchPayloadSum` (32 MiB of uncompressed document payloads).
pub const MAX_BATCH_PAYLOAD_SUM: i64 = 1024 * 1024 * 32;

/// `scip.SymbolRole_Definition`.
const SYMBOL_ROLE_DEFINITION: i32 = 0x1;

/// Rows per multi-row `INSERT` (keeps the bind-parameter count well below the
/// PostgreSQL limit of 65535).
const INSERT_CHUNK_ROWS: usize = 1000;

/// A failure while writing the code graph.
#[derive(Debug, thiserror::Error)]
pub enum CodeGraphError {
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("opening the SCIP payload: {0}")]
    Open(#[from] std::io::Error),
    #[error("compressing document payload: {0}")]
    Compress(std::io::Error),
    #[error(transparent)]
    Range(#[from] RangeError),
    #[error("unexpected number of document records inserted/retrieved")]
    DocumentCount,
    #[error("unexpected number of document lookup records inserted")]
    DocumentLookupCount,
    #[error("malformed trie - expected a symbol name to be a member: {0}")]
    MalformedTrie(String),
}

/// `writeSCIPDocuments`: insert metadata, documents, lookups and symbols for
/// one upload inside the caller's transaction, and return the accumulated
/// package data (normalized).
///
/// The caller owns the transaction (`codeGraphDataStore.WithTransaction` in
/// the baseline); on error nothing is committed.
pub async fn write_scip_documents(
    conn: &mut PgConnection,
    upload_id: i32,
    indexer: &str,
    scip_data_stream: &ScipDataStream,
) -> Result<ProcessedPackageData, CodeGraphError> {
    insert_metadata(conn, upload_id, &scip_data_stream.metadata).await?;

    let mut writer = if indexer == SYNTACTIC_INDEXER {
        ScipWriter::new_syntactic(upload_id)
    } else {
        ScipWriter::new_precise(upload_id)
    };

    let mut package_data = ProcessedPackageData::default();
    let mut iterator = scip_data_stream
        .document_iterator
        .second_pass()
        .map_err(CodeGraphError::Open)?;

    while let Some(document) = iterator.next_document() {
        writer
            .insert_document(conn, &document.path, &document.document)
            .await?;
        iterator.record_packages(&document.document);
    }

    writer.flush(conn).await?;
    iterator.finish(&mut package_data);
    package_data.normalize();

    Ok(package_data)
}

/// `InsertMetadata`.
pub async fn insert_metadata(
    conn: &mut PgConnection,
    upload_id: i32,
    metadata: &ProcessedMetadata,
) -> Result<(), CodeGraphError> {
    sqlx::query(
        "INSERT INTO codeintel_scip_metadata (upload_id, text_document_encoding, tool_name, \
         tool_version, tool_arguments, protocol_version) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(upload_id)
    .bind(&metadata.text_document_encoding)
    .bind(&metadata.tool_name)
    .bind(&metadata.tool_version)
    .bind(&metadata.tool_arguments)
    .bind(metadata.protocol_version)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// `scipWriter`.
pub struct ScipWriter {
    upload_id: i32,
    is_syntactic: bool,
    next_id: i32,
    inserted_symbols_count: u32,
    batch_payload_sum: i64,
    batch: Vec<BufferedDocument>,
}

struct BufferedDocument {
    path: String,
    document: Document,
    payload: Vec<u8>,
    payload_hash: Vec<u8>,
}

impl ScipWriter {
    /// `NewSyntacticSCIPWriter`.
    pub fn new_syntactic(upload_id: i32) -> Self {
        Self {
            upload_id,
            is_syntactic: true,
            next_id: 0,
            inserted_symbols_count: 0,
            batch_payload_sum: 0,
            batch: Vec::new(),
        }
    }

    /// `NewPreciseSCIPWriter`. The baseline also creates the two temporary
    /// tables here; this port writes to the target tables directly (see the
    /// module comment).
    pub fn new_precise(upload_id: i32) -> Self {
        Self {
            upload_id,
            is_syntactic: false,
            next_id: 0,
            inserted_symbols_count: 0,
            batch_payload_sum: 0,
            batch: Vec::new(),
        }
    }

    /// `InsertDocument`: buffer (and compress) the canonical payload, flushing
    /// at the batch-size or payload-sum limits.
    pub async fn insert_document(
        &mut self,
        conn: &mut PgConnection,
        path: &str,
        document: &Document,
    ) -> Result<(), CodeGraphError> {
        if self.batch_payload_sum >= MAX_BATCH_PAYLOAD_SUM {
            self.flush_documents(conn).await?;
        }

        let payload = sg_scip::canonicalize::encode_document(document);
        let compressed_payload = compress(&payload)?;

        let payload_hash = Sha256::digest(&payload).to_vec();

        self.batch.push(BufferedDocument {
            path: path.to_owned(),
            document: document.clone(),
            payload: compressed_payload,
            payload_hash,
        });
        self.batch_payload_sum += payload.len() as i64;

        if self.batch.len() >= DOCUMENTS_BATCH_SIZE {
            self.flush_documents(conn).await?;
        }

        Ok(())
    }

    async fn flush_documents(&mut self, conn: &mut PgConnection) -> Result<(), CodeGraphError> {
        let documents = std::mem::take(&mut self.batch);
        if documents.is_empty() {
            return Ok(());
        }
        self.batch_payload_sum = 0;

        let document_ids = self.insert_documents(conn, &documents).await?;
        let document_lookup_ids = self
            .insert_document_lookups(conn, &documents, &document_ids)
            .await?;

        // Syntactic indexes have no symbol rows: the trie cannot be used for
        // fuzzy matching.
        if self.is_syntactic {
            return Ok(());
        }

        let inverted_range_indexes = extract_all_symbol_indexes(&documents);
        let (trie_nodes, next_id) = construct_trie(&inverted_range_indexes, self.next_id);
        self.next_id = next_id;

        let ids_by_symbol_name = insert_symbol_names(conn, self.upload_id, &trie_nodes).await?;
        self.insert_symbols(
            conn,
            &inverted_range_indexes,
            &ids_by_symbol_name,
            &document_lookup_ids,
        )
        .await?;

        Ok(())
    }

    /// `insertDocuments`: insert payloads, deduplicating by hash. Rows already
    /// present (from a previous attempt) are looked up again.
    async fn insert_documents(
        &self,
        conn: &mut PgConnection,
        documents: &[BufferedDocument],
    ) -> Result<Vec<i64>, CodeGraphError> {
        let mut ids: Vec<Option<i64>> = Vec::with_capacity(documents.len());
        for document in documents {
            let id: Option<i64> = sqlx::query_scalar(
                "INSERT INTO codeintel_scip_documents (schema_version, payload_hash, raw_scip_payload) \
                 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING id",
            )
            .bind(SCHEMA_VERSION)
            .bind(&document.payload_hash)
            .bind(&document.payload)
            .fetch_optional(&mut *conn)
            .await?;
            ids.push(id);
        }

        if ids.iter().any(Option::is_none) {
            let mut ids_by_hash: HashMap<Vec<u8>, i64> = HashMap::new();
            let mut distinct_hashes: BTreeSet<Vec<u8>> = BTreeSet::new();
            for document in documents {
                distinct_hashes.insert(document.payload_hash.clone());
            }
            for hash in &distinct_hashes {
                let id: i64 = sqlx::query_scalar(
                    "SELECT id FROM codeintel_scip_documents WHERE payload_hash = $1",
                )
                .bind(hash)
                .fetch_one(&mut *conn)
                .await?;
                ids_by_hash.insert(hash.clone(), id);
            }

            if ids_by_hash.len() != distinct_hashes.len() {
                return Err(CodeGraphError::DocumentCount);
            }

            ids = documents
                .iter()
                .map(|document| ids_by_hash.get(&document.payload_hash).copied())
                .collect();
        }

        ids.into_iter()
            .map(|id| id.ok_or(CodeGraphError::DocumentCount))
            .collect()
    }

    /// `insertDocumentLookups`: one lookup row per document (in order).
    async fn insert_document_lookups(
        &self,
        conn: &mut PgConnection,
        documents: &[BufferedDocument],
        document_ids: &[i64],
    ) -> Result<Vec<i64>, CodeGraphError> {
        let mut ids = Vec::with_capacity(documents.len());
        for (document, document_id) in documents.iter().zip(document_ids) {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO codeintel_scip_document_lookup (upload_id, document_path, document_id) \
                 VALUES ($1, $2, $3) RETURNING id",
            )
            .bind(self.upload_id)
            .bind(&document.path)
            .bind(document_id)
            .fetch_one(&mut *conn)
            .await?;
            ids.push(id);
        }

        if ids.len() != documents.len() {
            return Err(CodeGraphError::DocumentLookupCount);
        }

        Ok(ids)
    }

    /// `insertSymbols`: encode the inverted range indexes and insert one row
    /// per (document, symbol).
    async fn insert_symbols(
        &mut self,
        conn: &mut PgConnection,
        inverted_range_indexes: &[Vec<InvertedRangeIndex>],
        ids_by_symbol_name: &HashMap<String, i32>,
        document_lookup_ids: &[i64],
    ) -> Result<(), CodeGraphError> {
        let mut rows: Vec<SymbolRow> = Vec::new();

        for (index, indexes) in inverted_range_indexes.iter().enumerate() {
            for inverted_range in indexes {
                let symbol_id = ids_by_symbol_name
                    .get(&inverted_range.symbol_name)
                    .copied()
                    .ok_or_else(|| {
                        CodeGraphError::MalformedTrie(inverted_range.symbol_name.clone())
                    })?;

                let definition_ranges = encode(&inverted_range.definition_ranges)?;
                let reference_ranges = encode(&inverted_range.reference_ranges)?;
                let implementation_ranges = encode(&inverted_range.implementation_ranges)?;
                let type_definition_ranges = encode(&inverted_range.type_definition_ranges)?;

                rows.push(SymbolRow {
                    symbol_id,
                    document_lookup_id: document_lookup_ids[index],
                    definition_ranges,
                    reference_ranges,
                    implementation_ranges,
                    type_definition_ranges,
                });
                self.inserted_symbols_count += 1;
            }
        }

        for chunk in rows.chunks(INSERT_CHUNK_ROWS) {
            let mut query = QueryBuilder::new(
                "INSERT INTO codeintel_scip_symbols (upload_id, symbol_id, document_lookup_id, \
                 schema_version, definition_ranges, reference_ranges, implementation_ranges, \
                 type_definition_ranges) ",
            );
            query.push_values(chunk, |mut row, symbol| {
                row.push_bind(self.upload_id)
                    .push_bind(symbol.symbol_id)
                    .push_bind(symbol.document_lookup_id)
                    .push_bind(SCHEMA_VERSION)
                    .push_bind(&symbol.definition_ranges)
                    .push_bind(&symbol.reference_ranges)
                    .push_bind(&symbol.implementation_ranges)
                    .push_bind(&symbol.type_definition_ranges);
            });
            query.build().execute(&mut *conn).await?;
        }

        Ok(())
    }

    /// `Flush`: write any buffered documents and return the number of symbol
    /// rows inserted.
    pub async fn flush(&mut self, conn: &mut PgConnection) -> Result<u32, CodeGraphError> {
        self.flush_documents(conn).await?;
        Ok(self.inserted_symbols_count)
    }
}

struct SymbolRow {
    symbol_id: i32,
    document_lookup_id: i64,
    definition_ranges: Vec<u8>,
    reference_ranges: Vec<u8>,
    implementation_ranges: Vec<u8>,
    type_definition_ranges: Vec<u8>,
}

/// `EncodeRanges`: absent range lists encode to an empty byte string.
///
/// The baseline passes the `nil` returned for an empty input through
/// `batch.Inserter`, whose driver stores it as `''::bytea` — not SQL NULL
/// (verified against the Go harness: 0 NULL rows, 161 empty rows for
/// `reference_ranges`). Binding an empty `Vec<u8>` reproduces that state.
fn encode(values: &[i32]) -> Result<Vec<u8>, RangeError> {
    encode_ranges(values)
}

/// `scipWriter.constructTrie`: rebuild the symbol-name trie for one batch,
/// carrying the identifier clock across batches.
fn construct_trie(
    inverted_range_indexes: &[Vec<InvertedRangeIndex>],
    next_id: i32,
) -> (Vec<TrieNode>, i32) {
    let mut symbol_name_set: BTreeSet<String> = BTreeSet::new();
    for indexes in inverted_range_indexes {
        for inverted_range in indexes {
            symbol_name_set.insert(inverted_range.symbol_name.clone());
        }
    }

    let symbol_names: Vec<String> = symbol_name_set.into_iter().collect();
    new_trie(&symbol_names, next_id)
}

/// `insertSymbolNames`: insert the trie nodes and return `name → id`.
async fn insert_symbol_names(
    conn: &mut PgConnection,
    upload_id: i32,
    trie_nodes: &[TrieNode],
) -> Result<HashMap<String, i32>, CodeGraphError> {
    for chunk in trie_nodes.chunks(INSERT_CHUNK_ROWS) {
        let mut query = QueryBuilder::new(
            "INSERT INTO codeintel_scip_symbol_names (upload_id, id, name_segment, prefix_id) ",
        );
        query.push_values(chunk, |mut row, node| {
            row.push_bind(upload_id)
                .push_bind(node.id)
                .push_bind(&node.prefix)
                .push_bind(node.parent_id);
        });
        query.build().execute(&mut *conn).await?;
    }

    Ok(trie_nodes
        .iter()
        .map(|node| (node.full_name.clone(), node.id))
        .collect())
}

/// `shared.InvertedRangeIndex`.
pub struct InvertedRangeIndex {
    pub symbol_name: String,
    pub definition_ranges: Vec<i32>,
    pub reference_ranges: Vec<i32>,
    pub implementation_ranges: Vec<i32>,
    pub type_definition_ranges: Vec<i32>,
}

#[derive(Default)]
struct RangeSets {
    definition: Vec<Range>,
    reference: Vec<Range>,
    implementation: Vec<Range>,
    type_definition: Vec<Range>,
}

fn extract_all_symbol_indexes(documents: &[BufferedDocument]) -> Vec<Vec<InvertedRangeIndex>> {
    documents
        .iter()
        .map(|document| extract_symbol_indexes(&document.document))
        .collect()
}

/// `ExtractSymbolIndexes`: invert a document's occurrences into per-symbol
/// range sets. Only occurrences contribute definitions/references; symbol
/// relationships contribute implementation/type-definition ranges.
pub fn extract_symbol_indexes(document: &Document) -> Vec<InvertedRangeIndex> {
    let mut ranges_by_symbol: HashMap<String, RangeSets> = HashMap::new();

    for occurrence in &document.occurrences {
        if occurrence.symbol.is_empty() || is_local_symbol(&occurrence.symbol) {
            continue;
        }

        let range = Range::new_unchecked(&occurrence.range);
        let range_set = ranges_by_symbol
            .entry(occurrence.symbol.clone())
            .or_default();
        if occurrence.symbol_roles & SYMBOL_ROLE_DEFINITION != 0 {
            range_set.definition.push(range);
        } else {
            range_set.reference.push(range);
        }
    }

    // `document.Symbols` is only used to propagate a symbol's definition
    // ranges to the symbols it is an implementation/type definition of.
    // Copied out so the map can be mutated in the loop below.
    let definitions_by_symbol: HashMap<String, Vec<Range>> = document
        .symbols
        .iter()
        .filter_map(|symbol: &SymbolInformation| {
            let ranges = ranges_by_symbol.get(&symbol.symbol)?;
            if ranges.definition.is_empty() {
                return None;
            }
            Some((symbol.symbol.clone(), ranges.definition.clone()))
        })
        .collect();

    for symbol in &document.symbols {
        let Some(definition_ranges) = definitions_by_symbol.get(&symbol.symbol) else {
            continue;
        };

        for relationship in &symbol.relationships {
            if !(relationship.is_implementation || relationship.is_type_definition) {
                continue;
            }

            let range_set = ranges_by_symbol
                .entry(relationship.symbol.clone())
                .or_default();
            if relationship.is_implementation {
                range_set
                    .implementation
                    .extend_from_slice(definition_ranges);
            }
            if relationship.is_type_definition {
                range_set
                    .type_definition
                    .extend_from_slice(definition_ranges);
            }
        }
    }

    let mut indexes: Vec<InvertedRangeIndex> = ranges_by_symbol
        .into_iter()
        .map(|(symbol_name, range_set)| InvertedRangeIndex {
            symbol_name,
            definition_ranges: collapse_ranges(range_set.definition),
            reference_ranges: collapse_ranges(range_set.reference),
            implementation_ranges: collapse_ranges(range_set.implementation),
            type_definition_ranges: collapse_ranges(range_set.type_definition),
        })
        .collect();

    indexes.sort_by(|a, b| a.symbol_name.cmp(&b.symbol_name));
    indexes
}

/// `collapseRanges`: sort by `LessStrict` and flatten to quads.
fn collapse_ranges(ranges: Vec<Range>) -> Vec<i32> {
    if ranges.is_empty() {
        return Vec::new();
    }

    let mut ranges = ranges;
    ranges.sort_by(|a, b| {
        if a.less_strict(b) {
            std::cmp::Ordering::Less
        } else if b.less_strict(a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });

    let mut components = Vec::with_capacity(ranges.len() * 4);
    for range in ranges {
        components.push(range.start.line);
        components.push(range.start.character);
        components.push(range.end.line);
        components.push(range.end.character);
    }
    components
}

/// `shared.Compressor.Compress`: gzip the payload.
fn compress(payload: &[u8]) -> Result<Vec<u8>, CodeGraphError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(payload)
        .map_err(CodeGraphError::Compress)?;
    encoder.finish().map_err(CodeGraphError::Compress)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_scip::scip::{Occurrence, Relationship};

    fn occurrence(range: Vec<i32>, symbol: &str, roles: i32) -> Occurrence {
        Occurrence {
            range,
            symbol: symbol.to_owned(),
            symbol_roles: roles,
            ..Occurrence::default()
        }
    }

    #[test]
    fn definitions_and_references_are_inverted() {
        let document = Document {
            occurrences: vec![
                occurrence(vec![0, 0, 1], "s m n 1 A#", 1),
                occurrence(vec![1, 0, 1], "s m n 1 A#", 0),
                occurrence(vec![2, 0, 1], "local 1", 1),
                occurrence(vec![3, 0, 1], "", 0),
            ],
            ..Document::default()
        };

        let indexes = extract_symbol_indexes(&document);
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].symbol_name, "s m n 1 A#");
        assert_eq!(indexes[0].definition_ranges, vec![0, 0, 0, 1]);
        assert_eq!(indexes[0].reference_ranges, vec![1, 0, 1, 1]);
        assert!(indexes[0].implementation_ranges.is_empty());
    }

    #[test]
    fn relationships_copy_definition_ranges() {
        let document = Document {
            occurrences: vec![occurrence(vec![5, 2, 8], "s m n 1 A#", 1)],
            symbols: vec![SymbolInformation {
                symbol: "s m n 1 A#".to_owned(),
                relationships: vec![
                    Relationship {
                        symbol: "s m n 1 I#".to_owned(),
                        is_implementation: true,
                        ..Relationship::default()
                    },
                    Relationship {
                        symbol: "s m n 1 T#".to_owned(),
                        is_type_definition: true,
                        ..Relationship::default()
                    },
                ],
                ..SymbolInformation::default()
            }],
            ..Document::default()
        };

        let indexes = extract_symbol_indexes(&document);
        let by_name: HashMap<&str, &InvertedRangeIndex> = indexes
            .iter()
            .map(|index| (index.symbol_name.as_str(), index))
            .collect();

        assert_eq!(by_name["s m n 1 A#"].definition_ranges, vec![5, 2, 5, 8]);
        assert_eq!(
            by_name["s m n 1 I#"].implementation_ranges,
            vec![5, 2, 5, 8]
        );
        assert_eq!(
            by_name["s m n 1 T#"].type_definition_ranges,
            vec![5, 2, 5, 8]
        );
        // The relationship targets only exist as inverted entries via the
        // propagated ranges (they have no occurrence of their own).
        assert!(by_name["s m n 1 I#"].definition_ranges.is_empty());
    }

    #[test]
    fn symbols_without_definitions_have_no_entry() {
        let document = Document {
            occurrences: vec![occurrence(vec![0, 0, 1], "s m n 1 A#", 0)],
            symbols: vec![SymbolInformation {
                symbol: "s m n 1 A#".to_owned(),
                relationships: vec![Relationship {
                    symbol: "s m n 1 B#".to_owned(),
                    is_implementation: true,
                    ..Relationship::default()
                }],
                ..SymbolInformation::default()
            }],
            ..Document::default()
        };

        let indexes = extract_symbol_indexes(&document);
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].symbol_name, "s m n 1 A#");
        assert!(indexes[0].implementation_ranges.is_empty());
    }

    #[test]
    fn ranges_are_sorted_and_flattened() {
        let document = Document {
            occurrences: vec![
                occurrence(vec![3, 0, 2], "s m n 1 A#", 1),
                occurrence(vec![1, 4, 6], "s m n 1 A#", 1),
                occurrence(vec![1, 1, 2], "s m n 1 A#", 1),
            ],
            ..Document::default()
        };

        let indexes = extract_symbol_indexes(&document);
        assert_eq!(
            indexes[0].definition_ranges,
            vec![1, 1, 1, 2, 1, 4, 1, 6, 3, 0, 3, 2]
        );
    }
}
