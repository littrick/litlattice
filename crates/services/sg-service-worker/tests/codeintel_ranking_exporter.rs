#![forbid(unsafe_code)]

//! M9.8dt differential harness (Rust side): [`export_ranking_graph`] over the
//! SCIP document stream of two uploads (one with an empty root, one with
//! `sub/`), mirrored from the Go harness
//! `internal/codeintel/ranking/internal/background/exporter/exporter_harness_test.go`.
//!
//! `test_ws/p9-go/run_codeintel_ranking_exporter_diff.sh` applies
//! `codeintel_ranking_exporter_fixtures.sql` to the frontend scratch database
//! immediately before each side and diffs the canonical `rexp ` JSON lines.
//! The SCIP documents live in the separate code-intel database
//! (`SG_CODEINTEL_RANKING_EXPORTER_CODEINTEL_DSN`); both harnesses truncate its
//! SCIP tables and insert the same gzip-compressed protobuf documents, so the
//! payloads match.

use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use prost::Message;
use serde_json::{json, Value};
use sg_config::SiteConfiguration;
use sg_scip::scip::{Document, Occurrence, SymbolRole};
use sg_service_worker::jobs::codeintel_ranking_file_reference_counter::export_ranking_graph;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPool;
use sqlx::Row;

fn print(value: Value) {
    println!("rexp {}", serde_json::to_string(&value).expect("serialize"));
}

fn site_with(enabled: bool) -> SiteConfiguration {
    let enabled = if enabled { "true" } else { "false" };
    SiteConfiguration::parse(&format!(
        r#"{{"codeIntelRanking.documentReferenceCountsEnabled": {enabled}, "codeIntelRanking.documentReferenceCountsGraphKey": "rexp"}}"#
    ))
    .expect("valid site config")
}

/// The shared symbol corpus of both harnesses.
const SYMBOL_BAR: &str =
    "scip-go gomod github.com/sourcegraph/sourcegraph v1.2.3 internal/foo/Bar#";
const SYMBOL_BAR_V9: &str =
    "scip-go gomod github.com/sourcegraph/sourcegraph v9.9.9 internal/foo/Bar#";
const SYMBOL_BAZ: &str =
    "scip-go gomod github.com/sourcegraph/sourcegraph v1.2.3 internal/foo/Bar#baz().";
const SYMBOL_LOCAL: &str = "local 1";
const SYMBOL_LSIF_PREFIX: &str = "lsif . something";
const SYMBOL_SPACE_PKG: &str = "s m n v `c d`/";
const SYMBOL_INVALID: &str = "not a symbol";

fn occurrence(symbol: &str, definition: bool) -> Occurrence {
    Occurrence {
        symbol: symbol.to_owned(),
        symbol_roles: if definition {
            SymbolRole::Definition as i32
        } else {
            0
        },
        ..Occurrence::default()
    }
}

fn document(occurrences: Vec<Occurrence>) -> Document {
    Document {
        occurrences,
        ..Document::default()
    }
}

struct HarnessDocument {
    upload_id: i32,
    path: &'static str,
    document: Document,
}

/// Mirrors the Go harness exactly.
fn harness_documents() -> Vec<HarnessDocument> {
    vec![
        HarnessDocument {
            upload_id: 201,
            path: "src/a.go",
            document: document(vec![
                occurrence(SYMBOL_BAR, true),
                occurrence(SYMBOL_BAR, true), // duplicate definition: emitted once
                occurrence(SYMBOL_BAR, false), // already defined in this document
                occurrence(SYMBOL_BAR_V9, false), // same checksum, different raw symbol
                occurrence(SYMBOL_BAZ, false), // reference before the definition occurrence
                occurrence(SYMBOL_BAZ, true),
                occurrence(SYMBOL_LOCAL, false), // local symbols are not canonicalized
                occurrence(SYMBOL_LOCAL, true),
                occurrence(SYMBOL_LSIF_PREFIX, false), // `lsif .` symbols are skipped
                occurrence("", false),                 // empty symbol
                occurrence("", true),
                occurrence(SYMBOL_SPACE_PKG, false), // escaped package name
                occurrence(SYMBOL_INVALID, false),   // unparseable symbol
            ]),
        },
        HarnessDocument {
            upload_id: 201,
            path: "src/b.go",
            document: document(vec![
                occurrence(SYMBOL_SPACE_PKG, false), // already defined later in this document
                occurrence(SYMBOL_SPACE_PKG, true),
                occurrence(SYMBOL_BAZ, true),
                occurrence(SYMBOL_BAR_V9, false),
            ]),
        },
        HarnessDocument {
            upload_id: 202,
            path: "x/y.go",
            document: document(vec![
                occurrence(SYMBOL_BAR, true),
                occurrence(SYMBOL_BAR, false),
                occurrence(SYMBOL_BAZ, false),
                occurrence(SYMBOL_LOCAL, true),
            ]),
        },
        HarnessDocument {
            upload_id: 202,
            path: "z.go",
            document: document(vec![occurrence(SYMBOL_BAZ, true)]),
        },
    ]
}

/// The SCIP tables of the code-intel database, reset before inserting the
/// harness documents (their `payload_hash` is unique).
const TRUNCATE_SCIP_TABLES: &str = "
TRUNCATE
    codeintel_scip_metadata,
    codeintel_scip_documents,
    codeintel_scip_document_lookup,
    codeintel_scip_symbol_names,
    codeintel_scip_symbols,
    codeintel_scip_document_lookup_schema_versions,
    codeintel_scip_symbols_schema_versions,
    codeintel_scip_documents_dereference_logs
RESTART IDENTITY
";

async fn insert_documents(pool: &PgPool, documents: &[HarnessDocument]) {
    for entry in documents {
        let payload = entry.document.encode_to_vec();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&payload).expect("compress document");
        let compressed = encoder.finish().expect("finish compressor");
        let checksum = Sha256::digest(&compressed);

        let document_id: i64 = sqlx::query_scalar(
            "INSERT INTO codeintel_scip_documents (payload_hash, schema_version, raw_scip_payload) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(checksum.to_vec())
        .bind(1i32)
        .bind(compressed)
        .fetch_one(pool)
        .await
        .expect("insert document");

        sqlx::query(
            "INSERT INTO codeintel_scip_document_lookup (upload_id, document_path, document_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(entry.upload_id)
        .bind(entry.path)
        .bind(document_id)
        .execute(pool)
        .await
        .expect("insert document lookup");
    }
}

async fn exports(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT id::bigint AS id, upload_id, graph_key, upload_key, \
         (deleted_at IS NOT NULL) AS deleted \
         FROM codeintel_ranking_exports ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("exports");

    rows.iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i64, _>("id").expect("id"),
                "upload_id": row.try_get::<i32, _>("upload_id").expect("upload_id"),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
                "upload_key": row.try_get::<String, _>("upload_key").expect("upload_key"),
                "deleted": row.try_get::<bool, _>("deleted").expect("deleted"),
            })
        })
        .collect()
}

async fn definitions(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT id, exported_upload_id, symbol_name, document_path, \
         encode(symbol_checksum, 'hex') AS checksum, graph_key \
         FROM codeintel_ranking_definitions ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("definitions");

    rows.iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i64, _>("id").expect("id"),
                "exported_upload_id": row.try_get::<i32, _>("exported_upload_id").expect("exported_upload_id"),
                "symbol_name": row.try_get::<String, _>("symbol_name").expect("symbol_name"),
                "document_path": row.try_get::<String, _>("document_path").expect("document_path"),
                "checksum": row.try_get::<String, _>("checksum").expect("checksum"),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
            })
        })
        .collect()
}

async fn references(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT id, exported_upload_id, symbol_names::text AS symbol_names, \
         (SELECT string_agg(encode(checksum, 'hex'), ',' ORDER BY ordinal) \
            FROM unnest(symbol_checksums) WITH ORDINALITY AS t(checksum, ordinal)) AS checksums, \
         graph_key \
         FROM codeintel_ranking_references ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("references");

    rows.iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i64, _>("id").expect("id"),
                "exported_upload_id": row.try_get::<i32, _>("exported_upload_id").expect("exported_upload_id"),
                "symbol_names": row.try_get::<String, _>("symbol_names").expect("symbol_names"),
                "checksums": row.try_get::<Option<String>, _>("checksums").expect("checksums").unwrap_or_default(),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
            })
        })
        .collect()
}

async fn initial_path_ranks(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT id, exported_upload_id, document_path, document_paths::text AS document_paths, graph_key \
         FROM codeintel_initial_path_ranks ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("initial path ranks");

    rows.iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i64, _>("id").expect("id"),
                "exported_upload_id": row.try_get::<i32, _>("exported_upload_id").expect("exported_upload_id"),
                "document_path": row.try_get::<String, _>("document_path").expect("document_path"),
                "document_paths": row.try_get::<String, _>("document_paths").expect("document_paths"),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
            })
        })
        .collect()
}

#[tokio::test]
async fn ranking_exporter_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_EXPORTER_DIFF").as_deref() != Ok("1") {
        return;
    }
    let (Ok(dsn), Ok(codeintel_dsn)) = (
        std::env::var("SG_CODEINTEL_RANKING_EXPORTER_TEST_DSN"),
        std::env::var("SG_CODEINTEL_RANKING_EXPORTER_CODEINTEL_DSN"),
    ) else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");
    let codeintel_pool = PgPool::connect(&codeintel_dsn)
        .await
        .expect("connect codeintel database");

    sqlx::query(TRUNCATE_SCIP_TABLES)
        .execute(&codeintel_pool)
        .await
        .expect("truncate SCIP tables");
    insert_documents(&codeintel_pool, &harness_documents()).await;

    // Gate off: no database writes at all.
    let (uploads, definition_count, reference_count) =
        export_ranking_graph(&pool, &codeintel_pool, &site_with(false), 16, 2)
            .await
            .expect("export ranking graph (disabled)");
    print(json!({
        "kind": "gate_off",
        "counts": [uploads, definition_count, reference_count],
        "exports": exports(&pool).await,
    }));

    // Enabled: both uploads are streamed, `src/a.go`/`src/b.go` under upload
    // 201 (empty root) and `x/y.go`/`z.go` under `sub/`.
    let (uploads, definition_count, reference_count) =
        export_ranking_graph(&pool, &codeintel_pool, &site_with(true), 16, 2)
            .await
            .expect("export ranking graph");
    print(json!({
        "kind": "run",
        "counts": [uploads, definition_count, reference_count],
        "exports": exports(&pool).await,
        "definitions": definitions(&pool).await,
        "references": references(&pool).await,
        "initial_path_ranks": initial_path_ranks(&pool).await,
    }));
}
