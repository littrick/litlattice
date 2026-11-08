#![forbid(unsafe_code)]

//! M9.8a differential harness (Rust side): the worker framework configuration.
//!
//! Builds the worker configuration exactly as the service's `configure` does
//! (`WorkerConfigs::load`) and prints, in canonical lines, the effective
//! allowlist/blocklist, the outcome of `Config.Validate` (what `svcmain`
//! calls) and of `BaseConfig.Validate` (where `LoadConfig` stores the error
//! the override never reads), `shouldRunJob` for sample names, and the 56-name
//! registry.
//!
//! The Go counterpart is `cmd/worker/shared/config_harness_test.go`;
//! `test_ws/p9-go/run_worker_config_diff.sh` runs both once per environment
//! scenario and compares the outputs.

use sg_core::EnvReader;
use sg_service_worker::config::WorkerConfigs;
use sg_service_worker::registry;

#[test]
fn worker_config_matches_go_harness() {
    let mut env = EnvReader::new();
    let configs = WorkerConfigs::load(&mut env, None);

    println!("registry count={}", registry::count());
    for name in registry::JOB_NAMES {
        println!("registry name={name}");
    }

    // `LoadConfig` shadows the package-level `config`: the returned value has
    // an empty Jobs-adjacent config, while `loadConfigs` fills the global one
    // that `shouldRunJob` consults. Print both.
    print_lists(
        "cfg",
        &configs.returned.job_allowlist,
        &configs.returned.job_blocklist,
    );
    print_validation(
        "cfg",
        configs.returned.validate().err().map(|err| err.to_string()),
        configs.returned.base_validation_error(),
    );

    print_lists(
        "global",
        &configs.global.job_allowlist,
        &configs.global.job_blocklist,
    );
    print_validation(
        "global",
        configs.global.validate().err().map(|err| err.to_string()),
        configs.global.base_validation_error(),
    );

    for name in ["insights-job", "webhook-log-janitor", "nope-xyz", ""] {
        println!("should_run name={name:?} -> {}", configs.should_run(name));
    }
}

fn print_lists(prefix: &str, allowlist: &[String], blocklist: &[String]) {
    println!("{prefix} allowlist=[{}]", allowlist.join(","));
    println!("{prefix} blocklist=[{}]", blocklist.join(","));
}

fn print_validation(prefix: &str, validate: Option<String>, base_validate: Option<String>) {
    match validate {
        Some(message) => println!("{prefix} validate={}", escape(&message)),
        None => println!("{prefix} validate=nil"),
    }
    match base_validate {
        Some(message) => println!("{prefix} base_validate={}", escape(&message)),
        None => println!("{prefix} base_validate=nil"),
    }
}

/// Keep multi-line multierror messages on one canonical line, like the Go
/// harness.
fn escape(message: &str) -> String {
    message.replace('\n', "\\n").replace('\t', "\\t")
}
