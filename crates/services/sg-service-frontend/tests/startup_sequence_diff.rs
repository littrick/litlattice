#![forbid(unsafe_code)]

//! M9.16c differential harness (Rust side): the pure startup-sequence pieces
//! ported in `service.rs`:
//!
//! * `oobmigration.NewVersionFromString` / `CompareVersions`
//!   (`internal/oobmigration/version.go`);
//! * `upgradestore.IsValidUpgrade`
//!   (`internal/version/upgradestore/upgrade.go`);
//! * `Runner.Validate` (`internal/oobmigration/runner.go`), against the
//!   in-memory migration list rather than the versions table.
//!
//! Mirrors `internal/oobmigration/zz_m9_16c_startup_oracle_test.go`
//! (UNTRACKED, gated by `SG_M916C_STARTUP=1`). Both sides consume
//! `data/startup_sequence/corpus.json` and write one JSON object per case
//! (sorted keys) to `M916C_STARTUP_OUT`.
//!
//! The Redis wait (`waitForRedis`) and the `InitDB`/metadata-sync ordering are
//! covered by the unit tests in `service.rs`; this diff checks the pure
//! version and validation semantics.

use std::fs;

use serde_json::{json, Value};
use sg_service_frontend::service::{
    compare_versions, is_valid_upgrade, new_version_from_string, validate_migrations, OobMigration,
    OobVersion, VersionOrder,
};

fn version(value: &Value) -> OobVersion {
    OobVersion {
        major: value["major"].as_i64().expect("major") as i32,
        minor: value["minor"].as_i64().expect("minor") as i32,
    }
}

#[test]
fn startup_sequence_matches_go() {
    if std::env::var("SG_M916C_STARTUP_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M916C_STARTUP_CORPUS").expect("M916C_STARTUP_CORPUS");
    let output_path = std::env::var("M916C_STARTUP_OUT").expect("M916C_STARTUP_OUT");
    let corpus: Value = serde_json::from_slice(&fs::read(&corpus_path).expect("read corpus"))
        .expect("parse corpus");
    let cases = corpus["cases"].as_array().expect("cases");
    let mut lines = Vec::with_capacity(cases.len());
    for case in cases {
        let kind = case["kind"].as_str().expect("kind");
        let name = case["name"].as_str().expect("name");
        let result = match kind {
            "nv" => {
                let raw = case["value"].as_str().expect("value");
                match new_version_from_string(raw) {
                    Some(version) => format!("ok:{}.{}", version.major, version.minor),
                    None => "invalid".to_owned(),
                }
            }
            "cmp" => match compare_versions(version(&case["a"]), version(&case["b"])) {
                VersionOrder::Before => "before".to_owned(),
                VersionOrder::Equal => "equal".to_owned(),
                VersionOrder::After => "after".to_owned(),
            },
            "upg" => {
                let previous = case["previous"].as_str();
                let latest = case["latest"].as_str().expect("latest");
                format!("valid:{}", is_valid_upgrade(previous, latest))
            }
            "oob" => {
                let migrations: Vec<OobMigration> = case["migrations"]
                    .as_array()
                    .expect("migrations")
                    .iter()
                    .map(|migration| OobMigration {
                        id: migration["id"].as_i64().expect("id") as i32,
                        introduced: version(&migration["introduced"]),
                        deprecated: migration["deprecated"].as_object().map(|_| version(&migration["deprecated"])),
                        progress: migration["progress"].as_f64().expect("progress"),
                    })
                    .collect();
                match validate_migrations(
                    version(&case["current"]),
                    version(&case["first"]),
                    &migrations,
                ) {
                    Ok(()) => "ok".to_owned(),
                    Err(error) => format!("error:{error}"),
                }
            }
            other => panic!("unknown kind {other}"),
        };
        lines.push(
            serde_json::to_string(&json!({"name": name, "kind": kind, "result": result}))
                .expect("serialize"),
        );
    }
    lines.sort();
    let mut out = lines.join("\n");
    out.push('\n');
    fs::write(&output_path, out).expect("write output");
}
