#![forbid(unsafe_code)]

//! M9.16c differential harness (Rust side): gorilla/mux route classification
//! for the routes this slice adds (`POST /.api/repos/{Repo}/-/refresh`,
//! `/.api/llm/chat/completions`, `/.api/llm/models[/{modelId}]`).
//!
//! Mirrors `cmd/frontend/internal/httpapi/zz_m9_16c_http_routes_oracle_test.go`
//! (UNTRACKED, gated by `SG_M916C_HTTP=1`). Both sides consume
//! `data/http_routes_inventory/corpus.json` and print one
//! `hri name=<name> class=<classification>` line per case, where the
//! classification is `matched:<route>`, `status:<code>`, `redirect:<url>` or
//! `not_found`.
//!
//! Runs only when `SG_M916C_HTTP_DIFF=1`; `M916C_HTTP_CORPUS` and
//! `M916C_HTTP_OUT` name the corpus and output file.

use std::fs;

use http::Request;
use serde_json::Value;
use sg_service_frontend::external_api::route_classification;

#[test]
fn http_routes_inventory_matches_go_gorilla() {
    if std::env::var("SG_M916C_HTTP_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M916C_HTTP_CORPUS").expect("M916C_HTTP_CORPUS");
    let output_path = std::env::var("M916C_HTTP_OUT").expect("M916C_HTTP_OUT");
    let corpus: Value =
        serde_json::from_slice(&fs::read(&corpus_path).expect("read corpus")).expect("parse corpus");
    let cases = corpus["cases"].as_array().expect("cases");
    let mut lines = Vec::with_capacity(cases.len());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let method = case["method"].as_str().expect("method");
        let path = case["path"].as_str().expect("path");
        let request = Request::builder()
            .method(method)
            .uri(path)
            .body(())
            .expect("request");
        let class = route_classification(&request);
        lines.push(format!("hri name={name} class={class}"));
    }
    let mut out = lines.join("\n");
    out.push('\n');
    fs::write(&output_path, out).expect("write output");
}
