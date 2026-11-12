//! Differential harness for the npm package source and registry client.
//! Replays `test_ws/p9-go/data/npm_packages_source/corpus.json`; see
//! `tests/support/mod.rs` and `internal/repos/zz_m9_14a_packages_oracle_test.go`.

mod support;

use serde_json::{json, Value};

use sg_extsvc::packages::{PackagesClient, PackagesSource};
use sg_extsvc::reposource_packages::NpmPackageName;
use sg_extsvc::{Source, KIND_NPMPACKAGES};

use support::{
    arc_doer, body_result, check_json, error_result, repo_json, runtime, service, setup,
    write_case, Case, Envelope, Op,
};

async fn run_op(client: &sg_extsvc::npm_packages::NpmClient, op: &Op) -> Value {
    match op.op.as_str() {
        "npm_package_info" => {
            let pkg = match NpmPackageName::new(&op.scope, &op.name) {
                Ok(pkg) => pkg,
                Err(err) => return error_result(err),
            };
            match client.get_package_info(&pkg).await {
                Ok(info) => json!({
                    "description": info.description,
                    "versions": info.versions.keys().cloned().collect::<Vec<_>>(),
                }),
                Err(err) => error_result(err),
            }
        }
        "npm_dependency_info" => {
            let pkg = match NpmPackageName::new(&op.scope, &op.name) {
                Ok(pkg) => pkg,
                Err(err) => return error_result(err),
            };
            match client.get_dependency_info(&pkg, &op.version).await {
                Ok(info) => json!({
                    "description": info.description,
                    "tarball": info.dist.tarball_url,
                }),
                Err(err) => error_result(err),
            }
        }
        "npm_fetch_tarball" => match client.fetch_tarball(&op.url).await {
            Ok(body) => body_result(&body),
            Err(err) => error_result(err),
        },
        other => panic!("unknown npm op {other}"),
    }
}

fn run_case(case: &Case, runtime: &tokio::runtime::Runtime) -> Value {
    let doer = arc_doer(case);
    let source = PackagesSource::new_npm(service(case, KIND_NPMPACKAGES), doer.clone())
        .expect("construct npm source");

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

    let PackagesClient::Npm(client) = source.client() else {
        panic!("npm source has an npm client");
    };
    for op in &case.ops {
        envelope.results.push(runtime.block_on(run_op(client, op)));
    }

    envelope.finish(&doer)
}

#[test]
fn npm_packages_source_diff() {
    let Some((cases, rust_dir)) = setup() else {
        return;
    };
    let rt = runtime();
    for case in &cases {
        let envelope = run_case(case, &rt);
        write_case(&rust_dir, &case.name, &envelope);
    }
}
