#![forbid(unsafe_code)]

//! Gated harness for the M9.13a schema differential: dump the Relay
//! introspection JSON of the OSS schema extended with the vendored
//! `code_monitors.graphql` and `saved_searches.graphql` documents, in the
//! order `graphqlbackend.NewSchema` appends optional resolvers.
//!
//! Gating: `SG_M9_13A_SCHEMA=1`.
//! Output file: `M9_13A_SCHEMA_OUT` (optional).

use std::env;

use sha2::{Digest, Sha256};
use sg_graphql::schema::{
    assemble, CODE_MONITORS_SCHEMA, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, SAVED_SEARCHES_SCHEMA,
    VIEWER_SCHEMA,
};

#[test]
fn dump_m9_13a_introspection_json() {
    if env::var("SG_M9_13A_SCHEMA").as_deref() != Ok("1") {
        return;
    }
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        CODE_MONITORS_SCHEMA,
        SAVED_SEARCHES_SCHEMA,
    ])
    .expect("the extended schema must assemble");
    let json = schema.to_json();
    let digest = Sha256::digest(json.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    println!("RUST_SCHEMA len={} sha256={hex}", json.len());
    if let Ok(path) = env::var("M9_13A_SCHEMA_OUT") {
        std::fs::write(&path, json.as_bytes()).expect("write introspection JSON");
    }
}
