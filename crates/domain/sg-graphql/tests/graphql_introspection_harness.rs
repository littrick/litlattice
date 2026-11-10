#![forbid(unsafe_code)]

//! Gated harness for the M9.8fa differential: dump the Relay introspection
//! JSON produced by `Schema::to_json()` so `run_graphql_introspection_diff.sh`
//! can byte-compare it with `(*graphql.Schema).ToJSON()` from graph-gophers.
//!
//! Gating: `SG_GRAPHQL_INTROSPECTION_HARNESS=1`.
//! Output file: `GRAPHQL_INTROSPECTION_OUT` (optional).

use std::env;

use sg_graphql::schema::assemble_oss;
use sha2::{Digest, Sha256};

#[test]
fn dump_relay_introspection_json() {
    if env::var("SG_GRAPHQL_INTROSPECTION_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let schema = assemble_oss().expect("the OSS schema must assemble");
    let json = schema.to_json();
    let digest = Sha256::digest(json.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    println!("introspection len={} sha256={hex}", json.len());
    if let Ok(path) = env::var("GRAPHQL_INTROSPECTION_OUT") {
        std::fs::write(&path, json.as_bytes()).expect("write introspection JSON");
    }
}
