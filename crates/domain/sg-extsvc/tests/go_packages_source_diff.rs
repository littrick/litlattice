//! Differential harness for the Go module package source and proxy client.
//! Replays `test_ws/p9-go/data/go_packages_source/corpus.json`; see
//! `tests/support/mod.rs` and `internal/repos/zz_m9_14a_packages_oracle_test.go`.

mod support;

use serde_json::{json, Value};

use sg_extsvc::packages::{PackagesClient, PackagesSource};
use sg_extsvc::{Source, KIND_GOMODULES};

use support::{
    arc_doer, body_result, check_json, error_result, repo_json, runtime, service, setup,
    write_case, Case, Envelope, Op,
};

async fn run_op(client: &sg_extsvc::go_packages::GoModProxyClient, op: &Op) -> Value {
    match op.op.as_str() {
        "gomodproxy_version" => match client.get_version(&op.module, &op.version).await {
            Ok(version) => json!({"path": version.path, "version": version.version}),
            Err(err) => error_result(err),
        },
        "gomodproxy_zip" => match client.get_zip(&op.module, &op.version).await {
            Ok(body) => body_result(&body),
            Err(err) => error_result(err),
        },
        other => panic!("unknown go op {other}"),
    }
}

fn run_case(case: &Case, runtime: &tokio::runtime::Runtime) -> Value {
    let doer = arc_doer(case);
    let source = PackagesSource::new_go(service(case, KIND_GOMODULES), doer.clone())
        .expect("construct go source");

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

    let PackagesClient::Go(client) = source.client() else {
        panic!("go source has a gomodproxy client");
    };
    for op in &case.ops {
        envelope.results.push(runtime.block_on(run_op(client, op)));
    }

    envelope.finish(&doer)
}

#[test]
fn go_packages_source_diff() {
    let Some((cases, rust_dir)) = setup() else {
        return;
    };
    let rt = runtime();
    for case in &cases {
        let envelope = run_case(case, &rt);
        write_case(&rust_dir, &case.name, &envelope);
    }
}
