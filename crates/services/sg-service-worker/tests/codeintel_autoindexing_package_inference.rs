#![forbid(unsafe_code)]

//! M9.8de differential harness (Rust side): the package → repository/revision
//! recognizers (`inference/infer.go`, `InferRepositoryAndRevision`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/inference/infer_harness_test.go`.
//! This is a pure-function comparison: no database is needed.
//! `test_ws/p9-go/run_codeintel_autoindexing_package_inference_diff.sh` runs
//! both sides and diffs the canonical `sip ` lines.

use sg_service_worker::jobs::codeintel_autoindexing_enqueuer::VersionedPackageRepo;
use sg_service_worker::jobs::codeintel_autoindexing_package_inference::infer_repository_and_revision;

#[derive(serde::Serialize)]
struct Snapshot {
    case: &'static str,
    ok: bool,
    repo: String,
    rev: String,
}

struct Case {
    name: &'static str,
    scheme: &'static str,
    package_name: &'static str,
    version: &'static str,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "go-github-tag",
            scheme: "gomod",
            package_name: "https://github.com/sourcegraph/sourcegraph",
            version: "v2.3.2",
        },
        Case {
            name: "go-github-submodule",
            scheme: "gomod",
            package_name: "https://github.com/aws/aws-sdk-go-v2/credentials",
            version: "v0.1.0",
        },
        Case {
            name: "go-github-commit",
            scheme: "gomod",
            package_name: "https://github.com/sourcegraph/sourcegraph",
            version: "v0.0.0-de0123456789",
        },
        Case {
            name: "go-github-long-commit",
            scheme: "gomod",
            package_name: "https://github.com/acme/lib",
            version: "v1.2.3-abcdef0123456789",
        },
        Case {
            name: "go-non-github",
            scheme: "gomod",
            package_name: "https://gitlab.com/a/b",
            version: "v1.0.0",
        },
        Case {
            name: "go-uppercase-hex",
            scheme: "gomod",
            package_name: "https://github.com/a/b",
            version: "v1.0.0-ABC123",
        },
        Case {
            name: "go-version-no-commit",
            scheme: "gomod",
            package_name: "https://github.com/a/b",
            version: "v1.2.3",
        },
        Case {
            name: "go-version-no-v-prefix",
            scheme: "gomod",
            package_name: "https://github.com/a/b",
            version: "1.2.3-abc",
        },
        Case {
            name: "go-name-without-scheme",
            scheme: "gomod",
            package_name: "github.com/a/b",
            version: "v1.0.0",
        },
        Case {
            name: "jvm-basic",
            scheme: "semanticdb",
            package_name: "com.example:lib",
            version: "1.2.3",
        },
        Case {
            name: "jvm-empty-version",
            scheme: "semanticdb",
            package_name: "g",
            version: "",
        },
        Case {
            name: "npm-unscoped",
            scheme: "npm",
            package_name: "mypackage",
            version: "1.0.0",
        },
        Case {
            name: "npm-scoped",
            scheme: "npm",
            package_name: "@myscope/mypackage",
            version: "1.0.0",
        },
        Case {
            name: "npm-dotted-name",
            scheme: "npm",
            package_name: "highlight.js",
            version: "2.0.0",
        },
        Case {
            name: "npm-dotted-scope",
            scheme: "npm",
            package_name: "@dinero.js/core",
            version: "1.0.0",
        },
        Case {
            name: "npm-invalid-space",
            scheme: "npm",
            package_name: "bad name",
            version: "1.0.0",
        },
        Case {
            name: "npm-empty-name",
            scheme: "npm",
            package_name: "@scope/",
            version: "1.0.0",
        },
        Case {
            name: "npm-at-in-name",
            scheme: "npm",
            package_name: "a@b",
            version: "1.0.0",
        },
        Case {
            name: "rust-plain",
            scheme: "rust-analyzer",
            package_name: "serde",
            version: "1.0.100",
        },
        Case {
            name: "rust-with-at",
            scheme: "rust-analyzer",
            package_name: "serde@1.0",
            version: "1.0.100",
        },
        Case {
            name: "python-plain",
            scheme: "python",
            package_name: "requests",
            version: "2.31.0",
        },
        Case {
            name: "python-with-eq",
            scheme: "python",
            package_name: "requests==2.0",
            version: "2.31.0",
        },
        Case {
            name: "ruby-plain",
            scheme: "scip-ruby",
            package_name: "rake",
            version: "13.0.6",
        },
        Case {
            name: "ruby-with-at",
            scheme: "scip-ruby",
            package_name: "rake@13",
            version: "13.0.6",
        },
        Case {
            name: "unknown-scheme",
            scheme: "nonsense",
            package_name: "x",
            version: "1",
        },
        Case {
            name: "empty-package",
            scheme: "",
            package_name: "",
            version: "",
        },
    ]
}

#[test]
fn autoindexing_package_inference_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_PACKAGE_INFERENCE_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }

    for case in cases() {
        let package = VersionedPackageRepo {
            scheme: case.scheme.to_owned(),
            name: case.package_name.to_owned(),
            version: case.version.to_owned(),
        };
        let snapshot = match infer_repository_and_revision(&package) {
            Some((repo, rev)) => Snapshot {
                case: case.name,
                ok: true,
                repo,
                rev,
            },
            None => Snapshot {
                case: case.name,
                ok: false,
                repo: String::new(),
                rev: String::new(),
            },
        };
        println!(
            "sip {}",
            serde_json::to_string(&snapshot).expect("json snapshot")
        );
    }
}
