#![forbid(unsafe_code)]
#![allow(dead_code)]

//! Canonical dumps shared by the database differential harnesses.
//!
//! The Go harnesses in `internal/codeintel/uploads/{storeharness,preludeharness}`
//! and `processor/{write_harness_test.go}` print the same `write ...` lines;
//! keeping the Rust side in one place makes the output formats harder to drift.

use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPool;
use sqlx::Row;

use sg_service_precise_worker::scip_stream::ProcessedPackageData;

pub fn print_packages(packages: &ProcessedPackageData) {
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

pub async fn dump_scip_tables(pool: &PgPool) {
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
    use std::io::Read as _;

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
