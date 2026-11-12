//! M9.19c differential harness: `conf.RedactAndHashSecrets`
//! (`internal/conf/validate.go`).
//!
//! Replays `test_ws/p9-go/data/configuration_redact/corpus.json` through
//! [`redact_and_hash_secrets`], writing the same JSON document as the Go oracle
//! (`internal/conf/zz_m919c_redact_oracle_test.go`, UNTRACKED, gated by
//! `SG_M919C_REDACT=1`). Only the success flag and the redacted document are
//! compared; error texts are each implementation's own.
//!
//! Gated on `M919C_REDACT_CORPUS`/`M919C_REDACT_OUT`;
//! `run_configuration_redact_diff.sh` sets them.

use serde::Deserialize;
use serde_json::json;
use sg_service_frontend::configuration_source::redact_and_hash_secrets;

#[derive(Deserialize)]
struct Case {
    name: String,
    site: String,
}

#[test]
fn configuration_redact_diff() {
    let (Ok(corpus_path), Ok(out_path)) = (
        std::env::var("M919C_REDACT_CORPUS"),
        std::env::var("M919C_REDACT_OUT"),
    ) else {
        eprintln!("M919C_REDACT_CORPUS/M919C_REDACT_OUT not set; skipping");
        return;
    };
    let cases: Vec<Case> =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let results: Vec<_> = cases
        .iter()
        .map(|case| match redact_and_hash_secrets(&case.site) {
            Ok(site) => json!({"name": case.name, "ok": true, "site": site}),
            Err(_) => json!({"name": case.name, "ok": false, "site": ""}),
        })
        .collect();

    std::fs::write(&out_path, format!("{}\n", json!(results))).expect("write output");
}
