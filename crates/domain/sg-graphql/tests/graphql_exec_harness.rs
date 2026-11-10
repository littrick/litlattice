#![forbid(unsafe_code)]

//! Gated harness for the M9.8fb differential: execute the shared executor
//! corpus against the OSS schema and dump each response, so
//! `run_graphql_exec_diff.sh` can compare it with the graph-gophers oracle.
//!
//! Gating: `SG_GRAPHQL_EXEC_HARNESS=1`.
//! Corpus: `GRAPHQL_EXEC_CORPUS` (JSON `[{name, query, operationName, variables}]`).
//! Output: `GRAPHQL_EXEC_OUT` (one `<name>.json` per entry).

use std::env;

use sg_graphql::exec::{execute, ExecRequest};
use sg_graphql::schema::assemble_oss;
use sha2::{Digest, Sha256};

#[test]
fn dump_executor_corpus() {
    if env::var("SG_GRAPHQL_EXEC_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = env::var("GRAPHQL_EXEC_CORPUS").expect("GRAPHQL_EXEC_CORPUS");
    let out_dir = env::var("GRAPHQL_EXEC_OUT").expect("GRAPHQL_EXEC_OUT");
    std::fs::create_dir_all(&out_dir).expect("create output directory");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: serde_json::Value = serde_json::from_str(&raw).expect("parse corpus");
    let schema = assemble_oss().expect("the OSS schema must assemble");

    for entry in corpus.as_array().expect("corpus array") {
        let name = entry["name"].as_str().expect("name");
        let query = entry["query"].as_str().expect("query");
        let operation_name = entry["operationName"].as_str().unwrap_or("");
        let variables = entry["variables"].as_object().cloned().unwrap_or_default();
        let response = execute(
            &schema,
            ExecRequest {
                query,
                operation_name,
                variables: &variables,
            },
        );
        let body = response.to_json();
        std::fs::write(format!("{out_dir}/{name}.json"), body.as_bytes()).expect("write response");
        let digest = Sha256::digest(body.as_bytes());
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        println!("RUST {name} len={} sha256={hex}", body.len());
    }
}
