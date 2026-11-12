//! Differential harness for `pagure_source.rs` / `pagure.rs`.
//!
//! Replays `test_ws/p9-go/data/pagure_source/corpus.json` through the Rust
//! port with an injectable [`HttpDoer`] and writes one JSON envelope per case
//! under `<M914B_OUT>/rust/`; the Go oracle
//! (`internal/repos/zz_m9_14b_pagure_oracle_test.go`) writes
//! `<M914B_OUT>/go/`. `run_pagure_source_diff.sh` compares them.

#[path = "support/m914b_diff.rs"]
mod diff;

use std::sync::Arc;

use sg_extsvc::pagure_source::PagureSource;
use sg_extsvc::{Source, SourceService};

async fn run_case(case: &diff::Case) -> serde_json::Value {
    let config = serde_json::to_string(&case.config).expect("config json");
    let service = SourceService {
        id: 1,
        kind: sg_extsvc::KIND_PAGURE.to_owned(),
        display_name: "pagure".to_owned(),
        config,
        encryption_key_id: String::new(),
        unrestricted: false,
    };
    let doer = Arc::new(diff::FakeDoer::new(
        case.responses.clone(),
        "authorization",
        "authorization",
    ));
    let source = match PagureSource::new(service, doer.clone()) {
        Ok(source) => source,
        Err(err) => panic!("case {}: constructing source: {err}", case.name),
    };

    let mut repos = Vec::new();
    let mut errors = Vec::new();
    for result in source.list_repos().await {
        match (result.repo, result.err) {
            (Some(repo), _) => repos.push(diff::repo_json(&repo)),
            (None, Some(err)) => errors.push(err),
            (None, None) => errors.push(String::new()),
        }
    }

    let check = if case.check {
        Some(match source.check_connection().await {
            Ok(()) => String::new(),
            Err(err) => err,
        })
    } else {
        None
    };

    diff::envelope(repos, errors, doer.requests(), "authorization", check)
}

#[test]
fn pagure_source_diff() {
    let Some((cases, rust_dir)) = diff::load_cases() else {
        return;
    };
    std::fs::create_dir_all(&rust_dir).expect("create output dir");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for case in &cases {
        let envelope = runtime.block_on(run_case(case));
        diff::write_envelope(&rust_dir, &case.name, &envelope);
    }
}
