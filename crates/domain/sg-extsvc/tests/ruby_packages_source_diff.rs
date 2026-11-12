//! Differential harness for the Ruby package source and RubyGems client.
//! Replays `test_ws/p9-go/data/ruby_packages_source/corpus.json`; see
//! `tests/support/mod.rs` and `internal/repos/zz_m9_14a_packages_oracle_test.go`.

mod support;

use serde_json::Value;

use sg_extsvc::packages::{PackagesClient, PackagesSource};
use sg_extsvc::{Source, KIND_RUBYPACKAGES};

use support::{
    arc_doer, body_result, check_json, error_result, repo_json, runtime, service, setup,
    write_case, Case, Envelope, Op,
};

async fn run_op(client: &sg_extsvc::ruby_packages::RubyGemsClient, op: &Op) -> Value {
    match op.op.as_str() {
        "rubygems_get" => match client.get_package_contents(&op.name, &op.version).await {
            Ok(body) => body_result(&body),
            Err(err) => error_result(err),
        },
        other => panic!("unknown ruby op {other}"),
    }
}

fn run_case(case: &Case, runtime: &tokio::runtime::Runtime) -> Value {
    let doer = arc_doer(case);
    let source = PackagesSource::new_ruby(service(case, KIND_RUBYPACKAGES), doer.clone())
        .expect("construct ruby source");

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

    let PackagesClient::Ruby(client) = source.client() else {
        panic!("ruby source has a RubyGems client");
    };
    for op in &case.ops {
        envelope.results.push(runtime.block_on(run_op(client, op)));
    }

    envelope.finish(&doer)
}

#[test]
fn ruby_packages_source_diff() {
    let Some((cases, rust_dir)) = setup() else {
        return;
    };
    let rt = runtime();
    for case in &cases {
        let envelope = run_case(case, &rt);
        write_case(&rust_dir, &case.name, &envelope);
    }
}
