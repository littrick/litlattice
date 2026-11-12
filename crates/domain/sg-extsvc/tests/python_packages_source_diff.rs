//! Differential harness for the Python package source and PyPI client.
//! Replays `test_ws/p9-go/data/python_packages_source/corpus.json`; see
//! `tests/support/mod.rs` and `internal/repos/zz_m9_14a_packages_oracle_test.go`.

mod support;

use serde_json::{json, Value};

use sg_extsvc::packages::{PackagesClient, PackagesSource};
use sg_extsvc::python_packages::File;
use sg_extsvc::{Source, KIND_PYTHONPACKAGES};

use support::{
    arc_doer, body_result, check_json, error_result, repo_json, runtime, service, setup,
    write_case, Case, Envelope, Op,
};

fn file_json(file: &File) -> Value {
    json!({
        "name": file.name,
        "url": file.url,
        "dataGpgSig": file.data_gpg_sig,
        "dataRequiresPython": file.data_requires_python,
    })
}

async fn run_op(client: &sg_extsvc::python_packages::PyPiClient, op: &Op) -> Value {
    match op.op.as_str() {
        "pypi_project" => match client.project(&op.project).await {
            Ok(files) => json!({"files": files.iter().map(file_json).collect::<Vec<_>>()}),
            Err(err) => error_result(err),
        },
        "pypi_version" => match client.version(&op.project, &op.version).await {
            Ok(file) => json!({"file": file_json(&file)}),
            Err(err) => error_result(err),
        },
        "pypi_download" => match client.download(&op.url).await {
            Ok(body) => body_result(&body),
            Err(err) => error_result(err),
        },
        other => panic!("unknown python op {other}"),
    }
}

fn run_case(case: &Case, runtime: &tokio::runtime::Runtime) -> Value {
    let doer = arc_doer(case);
    let source =
        PackagesSource::new_python(service(case, KIND_PYTHONPACKAGES), doer.clone())
            .expect("construct python source");

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

    let PackagesClient::Python(client) = source.client() else {
        panic!("python source has a PyPI client");
    };
    for op in &case.ops {
        envelope.results.push(runtime.block_on(run_op(client, op)));
    }

    envelope.finish(&doer)
}

#[test]
fn python_packages_source_diff() {
    let Some((cases, rust_dir)) = setup() else {
        return;
    };
    let rt = runtime();
    for case in &cases {
        let envelope = run_case(case, &rt);
        write_case(&rust_dir, &case.name, &envelope);
    }
}
