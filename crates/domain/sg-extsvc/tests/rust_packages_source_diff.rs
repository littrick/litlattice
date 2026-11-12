//! Differential harness for the Rust package source and crates.io client.
//! Replays `test_ws/p9-go/data/rust_packages_source/corpus.json`; see
//! `tests/support/mod.rs` and `internal/repos/zz_m9_14a_packages_oracle_test.go`.

mod support;

use serde_json::Value;

use sg_extsvc::packages::{PackagesClient, PackagesSource};
use sg_extsvc::{Source, KIND_RUSTPACKAGES};

use support::{
    arc_doer, body_result, check_json, error_result, repo_json, runtime, service, setup,
    write_case, Case, Envelope, Op,
};

async fn run_op(client: &sg_extsvc::rust_packages::CratesClient, op: &Op) -> Value {
    match op.op.as_str() {
        "crates_get" => match client.get(&op.url).await {
            Ok(body) => body_result(&body),
            Err(err) => error_result(err),
        },
        other => panic!("unknown rust op {other}"),
    }
}

fn run_case(case: &Case, runtime: &tokio::runtime::Runtime) -> Value {
    let doer = arc_doer(case);
    let source = PackagesSource::new_rust(service(case, KIND_RUSTPACKAGES), doer.clone())
        .expect("construct rust source");

    let mut envelope = Envelope::new();
    if case.source {
        for result in runtime.block_on(source.list_repos()) {
            match (result.repo, result.err) {
                (Some(repo), _) => envelope.repos.push(repo_json(&repo)),
                (None, Some(err)) => envelope.errors.push(Value::String(err)),
                (None, None) => envelope.errors.push(Value::String(String::new())),
            }
        }
    }
    if case.check {
        envelope.check = check_json(Some(true), runtime.block_on(source.check_connection()));
    }

    let PackagesClient::Rust(client) = source.client() else {
        panic!("rust source has a crates client");
    };
    for op in &case.ops {
        envelope.results.push(runtime.block_on(run_op(client, op)));
    }

    envelope.finish(&doer)
}

#[test]
fn rust_packages_source_diff() {
    let Some((cases, rust_dir)) = setup() else {
        return;
    };
    let rt = runtime();
    for case in &cases {
        let envelope = run_case(case, &rt);
        write_case(&rust_dir, &case.name, &envelope);
    }
}
