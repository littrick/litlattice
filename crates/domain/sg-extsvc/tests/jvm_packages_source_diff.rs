//! Differential harness for the JVM package source (Maven dependency
//! parsing). Replays `test_ws/p9-go/data/jvm_packages_source/corpus.json`; see
//! `tests/support/mod.rs` and `internal/repos/zz_m9_14a_packages_oracle_test.go`.
//!
//! There are no registry operations: the baseline's coursier-backed `Get` is
//! commented out and is an explicit not-ported boundary.

mod support;

use serde_json::Value;

use sg_extsvc::packages::PackagesSource;
use sg_extsvc::{Source, KIND_JVMPACKAGES};

use support::{
    arc_doer, check_json, repo_json, runtime, service, setup, write_case, Case, Envelope,
};

fn run_case(case: &Case, runtime: &tokio::runtime::Runtime) -> Value {
    assert!(case.ops.is_empty(), "jvm cases have no registry operations");
    let doer = arc_doer(case);
    let source = PackagesSource::new_jvm(service(case, KIND_JVMPACKAGES))
        .expect("construct jvm source");

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

    envelope.finish(&doer)
}

#[test]
fn jvm_packages_source_diff() {
    let Some((cases, rust_dir)) = setup() else {
        return;
    };
    let rt = runtime();
    for case in &cases {
        let envelope = run_case(case, &rt);
        write_case(&rust_dir, &case.name, &envelope);
    }
}
