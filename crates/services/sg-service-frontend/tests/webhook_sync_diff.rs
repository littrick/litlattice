//! Differential harness for inbound repository webhooks
//! (`cmd/frontend/internal/repos/webhooks`, ported in
//! `sg_service_frontend::webhooks`).
//!
//! The untracked Go oracle
//! (`cmd/frontend/internal/repos/webhooks/zz_m9_16b_webhook_oracle_test.go`)
//! decodes each push payload with the real code-host event types and runs the
//! production external-ID extractors. The Rust side parses the same JSON body
//! and must yield the same external ID or error string.
//!
//! Inert unless `M916B_CORPUS` and `M916B_OUT` are set.

use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::webhooks::{parse_push_event, CodeHostKind};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    kind: String,
    body: Value,
}

fn kind_from_str(kind: &str) -> Option<CodeHostKind> {
    Some(match kind {
        "github" => CodeHostKind::GitHub,
        "gitlab" => CodeHostKind::GitLab,
        "bitbucketServer" => CodeHostKind::BitbucketServer,
        "bitbucketCloud" => CodeHostKind::BitbucketCloud,
        _ => return None,
    })
}

#[test]
fn webhook_sync_diff() {
    let (Ok(corpus_path), Ok(out)) = (
        std::env::var("M916B_CORPUS"),
        std::env::var("M916B_OUT"),
    ) else {
        return;
    };
    let raw = fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    for case in &cases {
        let kind = kind_from_str(&case.kind).expect("known code host");
        let body = serde_json::to_vec(&case.body).expect("serialize body");
        let envelope = match parse_push_event(kind, &body) {
            Ok(event) => json!({ "external_id": event.external_id }),
            Err(error) => json!({ "error": error }),
        };
        fs::write(
            Path::new(&out)
                .join("rust")
                .join(format!("{}.json", case.name)),
            serde_json::to_vec(&envelope).expect("serialize envelope"),
        )
        .expect("write rust envelope");
        println!("RUST {}", case.name);
    }
}
