//! M9.8eg differential harness (test-only): the deployment default site
//! configuration (`confdefaults`) and its `RedactAndHashSecrets` snapshot.
//!
//! Prints canonical `raw <name> ...` / `rd <name> ...` lines, one per
//! deployment default, with newlines/tabs escaped so each document stays on
//! one line. The Go counterpart is
//! `internal/database/conf_site_config_harness_test.go`.

use sg_service_frontend::deploy_defaults::{
    default_site_config_json, default_site_config_redacted_json,
};

fn flat(document: &str) -> String {
    document
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
}

#[test]
fn deploy_defaults_harness() {
    for name in ["k8s", "dev", "docker-container"] {
        let raw = default_site_config_json(name).expect("default document");
        let redacted = default_site_config_redacted_json(name).expect("redacted document");
        println!("raw {name} {}", flat(raw));
        println!("rd {name} {}", flat(redacted));
    }
}
