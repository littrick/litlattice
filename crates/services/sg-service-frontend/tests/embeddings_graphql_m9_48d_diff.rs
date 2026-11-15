//! M9.48d differential of the embeddings GraphQL resolver helpers.
//!
//! The Go oracle (`cmd/frontend/internal/embeddings/resolvers/
//! zz_m9_48d_embeddings_oracle_test.go`) drives the real (unexported)
//! resolvers; this harness drives the ported helpers and emits the same
//! canonical `{data, errors}` JSON per case.
//!
//! Env:
//!
//! * `SG_M9_48D_EMBEDDINGS_DIFF=1`
//! * `M9_48D_EMBEDDINGS_CORPUS` — JSON `[{name, op, ...}]`
//! * `M9_48D_EMBEDDINGS_OUT` — output directory

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use sg_db::repo_embeddings::RepoEmbeddingStatsRow;
use sg_service_frontend::embeddings_graphql::is_context_required_for_chat_query;
use sg_service_frontend::embeddings_graphql_surface::{
    datetime_string, extract_line_range, marshal_job_id, stats_data, upper_state,
};
use sg_service_frontend::graphql_resolvers::unmarshal_relay_id;

#[derive(Deserialize)]
struct Case {
    name: String,
    op: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    queued: String,
    #[serde(default)]
    started: Option<String>,
    #[serde(default)]
    finished: Option<String>,
    #[serde(default, rename = "processAfter")]
    process_after: Option<String>,
    #[serde(default)]
    heartbeat: Option<String>,
    #[serde(default)]
    code: Stats,
    #[serde(default)]
    text: Stats,
    #[serde(default)]
    content: String,
    #[serde(default)]
    start: i32,
    #[serde(default)]
    end: i32,
    #[serde(default)]
    query: String,
    #[serde(default)]
    id: i32,
}

#[derive(Default, Deserialize)]
struct Stats {
    #[serde(default)]
    scheduled: i64,
    #[serde(default)]
    embedded: i64,
    #[serde(default)]
    skipped: BTreeMap<String, i64>,
}

#[derive(Serialize)]
struct Envelope {
    data: Value,
    errors: Vec<String>,
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("RFC3339")
}

fn datetime(value: Option<OffsetDateTime>) -> Option<String> {
    value.map(datetime_string)
}

fn run_case(case: &Case) -> Result<Value, String> {
    match case.op.as_str() {
        "job_state" => Ok(json!({"state": upper_state(&case.state)})),
        "job_times" => {
            let queued = parse_time(&case.queued);
            let started = case.started.as_deref().map(parse_time);
            let finished = case.finished.as_deref().map(parse_time);
            let process_after = case.process_after.as_deref().map(parse_time);
            let heartbeat = case.heartbeat.as_deref().map(parse_time);
            // Erratum: the baseline checks `ProcessAfter == nil` before
            // returning the heartbeat time.
            let last_heartbeat = if process_after.is_none() {
                None
            } else {
                heartbeat
            };
            Ok(json!({
                "queuedAt": Some(datetime_string(queued)),
                "startedAt": datetime(started),
                "finishedAt": datetime(finished),
                "processAfter": datetime(process_after),
                "lastHeartbeatAt": datetime(last_heartbeat),
            }))
        }
        "stats" => {
            let row = RepoEmbeddingStatsRow {
                code_files_scheduled: case.code.scheduled,
                code_files_embedded: case.code.embedded,
                code_files_skipped: case.code.skipped.clone(),
                text_files_scheduled: case.text.scheduled,
                text_files_embedded: case.text.embedded,
                text_files_skipped: case.text.skipped.clone(),
            };
            let stats = stats_data(&row);
            Ok(json!({
                "filesScheduled": stats.files_scheduled,
                "filesEmbedded": stats.files_embedded,
                "filesSkipped": stats.files_skipped,
            }))
        }
        "extract" => Ok(json!({
            "content": String::from_utf8_lossy(&extract_line_range(
                case.content.as_bytes(),
                case.start,
                case.end,
            ))
            .into_owned(),
        })),
        "context_required" => Ok(json!({
            "value": is_context_required_for_chat_query(&case.query),
        })),
        "relay" => Ok(json!({"id": marshal_job_id(case.id)})),
        "relay_roundtrip" => Ok(json!({
            "id": unmarshal_relay_id(&marshal_job_id(case.id))?,
        })),
        other => Err(format!("unknown op {other}")),
    }
}

#[tokio::test]
async fn embeddings_graphql_m9_48d_diff_harness() {
    if std::env::var("SG_M9_48D_EMBEDDINGS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_48D_EMBEDDINGS_CORPUS").expect("corpus env");
    let out = std::env::var("M9_48D_EMBEDDINGS_OUT").expect("out env");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    // Keep the unused-import warning away while documenting the async harness
    // shape shared with the other GraphQL diffs.
    let _ = Duration::from_secs(0);
    for case in cases {
        let (data, errors) = match run_case(&case) {
            Ok(value) => (value, Vec::new()),
            Err(error) => (Value::Null, vec![error]),
        };
        let envelope = Envelope { data, errors };
        let body = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
            .expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
