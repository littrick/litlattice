//! M9.8ez differential harness for schema assembly.
//!
//! Gated by `SG_GRAPHQL_SCHEMA_HARNESS=1`; reads the corpus named by
//! `GRAPHQL_SCHEMA_CORPUS` and prints one canonical `gs` line per case,
//! matching the Go oracle (`cmd/m9ezschemaharness`, graph-gophers
//! `ParseSchema(..., nil)`).

use sg_graphql::schema::assemble;
use sg_graphql::schema::sdl::{MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA};
use sha2::{Digest, Sha256};

#[test]
fn graphql_schema_harness() {
    if std::env::var("SG_GRAPHQL_SCHEMA_HARNESS").as_deref() != Ok("1") {
        eprintln!("set SG_GRAPHQL_SCHEMA_HARNESS=1 to run");
        return;
    }

    let path = std::env::var("GRAPHQL_SCHEMA_CORPUS").expect("GRAPHQL_SCHEMA_CORPUS is not set");
    let raw = std::fs::read_to_string(&path).expect("read corpus");
    let corpus: serde_json::Value = serde_json::from_str(&raw).expect("parse corpus");

    for case in corpus["cases"].as_array().expect("cases") {
        let name = case["name"].as_str().expect("case name");
        let mut parts = vec![MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA];
        for append in case["append"].as_array().map(Vec::as_slice).unwrap_or(&[]) {
            parts.push(append.as_str().expect("append string"));
        }

        match assemble(&parts) {
            Err(_) => println!("gs {name} ok=0"),
            Ok(schema) => {
                let mut lines = schema.canonical_inventory();
                lines.sort();
                if std::env::var("M9EZ_DUMP").as_deref() == Ok(name) {
                    for line in &lines {
                        println!("L {line}");
                    }
                }
                let mut hasher = Sha256::new();
                hasher.update(lines.join("\n").as_bytes());
                let hash = hasher.finalize();
                let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
                println!("gs {name} ok=1 types={} sha256={hex}", schema.type_count());
            }
        }
    }
}
