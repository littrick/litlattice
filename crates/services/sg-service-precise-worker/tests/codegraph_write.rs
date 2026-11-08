#![forbid(unsafe_code)]

//! M9.7e differential harness (Rust side): the code-graph write path.
//!
//! Runs [`prepare_scip_data_stream`] followed by [`write_scip_documents`]
//! against a scratch code-intel database, then dumps the SCIP tables in the
//! same canonical form as the Go harness
//! (`internal/codeintel/uploads/internal/background/processor/write_harness_test.go`).
//! `test_ws/p9-go/run_codegraph_write_diff.sh` compares the outputs.
//!
//! Requires `SG_CODEINTEL_DSN` (a disposable database with the `codeintel`
//! schema) and `SG_SCIP_FIXTURE`; without them the test is skipped.

use std::collections::HashMap;
use std::io::Read;

use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPool;
use sqlx::Row;

use sg_service_precise_worker::codegraph::write_scip_documents;
use sg_service_precise_worker::pathexistence::DirectoryChildren;
use sg_service_precise_worker::prelude::UploadData;
use sg_service_precise_worker::scip_stream::{prepare_scip_data_stream, ProcessedPackageData};

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

#[tokio::test]
async fn codegraph_write_matches_go_harness() {
    let (Ok(dsn), Ok(fixture)) = (
        std::env::var("SG_CODEINTEL_DSN"),
        std::env::var("SG_SCIP_FIXTURE"),
    ) else {
        eprintln!(
            "SG_CODEINTEL_DSN / SG_SCIP_FIXTURE are not set; skipping the code-graph harness"
        );
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect");
    sqlx::query(TRUNCATE_SCIP_TABLES)
        .execute(&pool)
        .await
        .expect("truncate SCIP tables");

    let bytes = std::fs::read(&fixture).expect("read fixture");
    let children = directory_children();

    let stream = prepare_scip_data_stream(
        UploadData::from_bytes(bytes),
        "",
        move |dirnames: Vec<String>| {
            let children = children.clone();
            async move {
                let listed: DirectoryChildren = dirnames
                    .into_iter()
                    .map(|dirname| {
                        let entries = children.get(&dirname).cloned().unwrap_or_default();
                        (dirname, entries)
                    })
                    .collect();
                Ok::<DirectoryChildren, ()>(listed)
            }
        },
    )
    .await
    .expect("prepare SCIP data stream");

    let mut tx = pool.begin().await.expect("begin transaction");
    let packages = write_scip_documents(&mut tx, 1, "scip-typescript", &stream)
        .await
        .expect("write SCIP documents");
    tx.commit().await.expect("commit");

    print_packages(&packages);
    dump_scip_tables(&pool).await;
}

fn print_packages(packages: &ProcessedPackageData) {
    println!(
        "write packages definitions={} references={}",
        packages.packages.len(),
        packages.package_references.len()
    );
    for package in &packages.packages {
        print_package(package, true);
    }
    for reference in &packages.package_references {
        print_package(&reference.package, false);
    }
}

fn print_package(package: &sg_scip::Package, definition: bool) {
    println!(
        "write package definition={} scheme={} manager={} name={} version={}",
        definition, package.scheme, package.manager, package.name, package.version
    );
}

async fn dump_scip_tables(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT upload_id, text_document_encoding, tool_name, tool_version, \
         array_to_string(tool_arguments, ','), protocol_version \
         FROM codeintel_scip_metadata ORDER BY upload_id",
    )
    .fetch_all(pool)
    .await
    .expect("metadata query");
    for row in rows {
        println!(
            "write metadata upload_id={} encoding={} tool={} version={} args={} protocol={}",
            row.get::<i32, _>("upload_id"),
            row.get::<String, _>("text_document_encoding"),
            row.get::<String, _>("tool_name"),
            row.get::<String, _>("tool_version"),
            row.get::<String, _>("array_to_string"),
            row.get::<i32, _>("protocol_version"),
        );
    }

    let rows = sqlx::query(
        "SELECT id, encode(payload_hash, 'hex') AS hash, schema_version, raw_scip_payload \
         FROM codeintel_scip_documents ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("documents query");
    for row in rows {
        let id: i64 = row.get("id");
        let payload: Vec<u8> = row.get("raw_scip_payload");
        let decompressed = gunzip(&payload);
        let digest = Sha256::digest(&decompressed);
        println!(
            "write document id={} hash={} schema_version={} payload_len={} payload_sha256={}",
            id,
            row.get::<String, _>("hash"),
            row.get::<i32, _>("schema_version"),
            decompressed.len(),
            hex(&digest),
        );
    }

    let rows = sqlx::query(
        "SELECT id, upload_id, document_path, document_id \
         FROM codeintel_scip_document_lookup ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("lookup query");
    for row in rows {
        println!(
            "write lookup id={} upload_id={} path={} document_id={}",
            row.get::<i64, _>("id"),
            row.get::<i32, _>("upload_id"),
            row.get::<String, _>("document_path"),
            row.get::<i64, _>("document_id"),
        );
    }

    let rows = sqlx::query(
        "SELECT upload_id, id, name_segment, prefix_id FROM codeintel_scip_symbol_names ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("symbol names query");
    for row in rows {
        let segment: String = row.get("name_segment");
        println!(
            "write name upload_id={} id={} segment_hex={} prefix_id={}",
            row.get::<i32, _>("upload_id"),
            row.get::<i32, _>("id"),
            hex(segment.as_bytes()),
            nullable_int(row.get::<Option<i32>, _>("prefix_id")),
        );
    }

    let rows = sqlx::query(
        "SELECT upload_id, symbol_id, document_lookup_id, schema_version, \
         encode(definition_ranges, 'hex') AS definition, \
         encode(reference_ranges, 'hex') AS reference, \
         encode(implementation_ranges, 'hex') AS implementation, \
         encode(type_definition_ranges, 'hex') AS type_definition \
         FROM codeintel_scip_symbols ORDER BY document_lookup_id, symbol_id",
    )
    .fetch_all(pool)
    .await
    .expect("symbols query");
    for row in rows {
        println!(
            "write symbol upload_id={} symbol_id={} lookup_id={} schema_version={} \
             definition={} reference={} implementation={} type_definition={}",
            row.get::<i32, _>("upload_id"),
            row.get::<i32, _>("symbol_id"),
            row.get::<i64, _>("document_lookup_id"),
            row.get::<i32, _>("schema_version"),
            nullable_string(row.get::<Option<String>, _>("definition")),
            nullable_string(row.get::<Option<String>, _>("reference")),
            nullable_string(row.get::<Option<String>, _>("implementation")),
            nullable_string(row.get::<Option<String>, _>("type_definition")),
        );
    }

    let rows = sqlx::query(
        "SELECT upload_id, min_schema_version, max_schema_version \
         FROM codeintel_scip_document_lookup_schema_versions ORDER BY upload_id",
    )
    .fetch_all(pool)
    .await
    .expect("lookup schema versions query");
    for row in rows {
        println!(
            "write lookup_schema_version upload_id={} min={} max={}",
            row.get::<i32, _>("upload_id"),
            nullable_int(row.get::<Option<i32>, _>("min_schema_version")),
            nullable_int(row.get::<Option<i32>, _>("max_schema_version")),
        );
    }

    let rows = sqlx::query(
        "SELECT upload_id, min_schema_version, max_schema_version \
         FROM codeintel_scip_symbols_schema_versions ORDER BY upload_id",
    )
    .fetch_all(pool)
    .await
    .expect("symbols schema versions query");
    for row in rows {
        println!(
            "write symbol_schema_version upload_id={} min={} max={}",
            row.get::<i32, _>("upload_id"),
            nullable_int(row.get::<Option<i32>, _>("min_schema_version")),
            nullable_int(row.get::<Option<i32>, _>("max_schema_version")),
        );
    }
}

fn gunzip(payload: &[u8]) -> Vec<u8> {
    let mut decoder = GzDecoder::new(payload);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).expect("gunzip");
    decompressed
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn nullable_int(value: Option<i32>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

fn nullable_string(value: Option<String>) -> String {
    value.unwrap_or_else(|| "NULL".to_owned())
}

fn directory_children() -> HashMap<String, Vec<String>> {
    HashMap::from([
        (String::new(), vec!["template".to_owned()]),
        ("template".to_owned(), vec!["template/src".to_owned()]),
        (
            "template/src".to_owned(),
            vec![
                "template/src/extension.ts".to_owned(),
                "template/src/indicators.ts".to_owned(),
                "template/src/language.ts".to_owned(),
                "template/src/logging.ts".to_owned(),
                "template/src/util".to_owned(),
            ],
        ),
        (
            "template/src/util".to_owned(),
            vec![
                "template/src/util/api.ts".to_owned(),
                "template/src/util/graphql.ts".to_owned(),
                "template/src/util/ix.test.ts".to_owned(),
                "template/src/util/ix.ts".to_owned(),
                "template/src/util/promise.ts".to_owned(),
                "template/src/util/uri.test.ts".to_owned(),
                "template/src/util/uri.ts".to_owned(),
            ],
        ),
    ])
}
