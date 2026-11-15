//! M9.48d differential of the Cody context resolver helpers.
//!
//! The Go oracle (`cmd/frontend/internal/context/resolvers/
//! zz_m9_48d_cody_oracle_test.go`) drives the real (unexported) helpers; this
//! harness drives the ported helpers and emits the same canonical
//! `{data, errors}` JSON per case.
//!
//! Env:
//!
//! * `SG_M9_48D_CODY_DIFF=1`
//! * `M9_48D_CODY_CORPUS` — JSON `[{name, op, ...}]`
//! * `M9_48D_CODY_OUT` — output directory

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use sg_service_frontend::cody_context_graphql::{
    count_lines, identity_rank, process_url_context, send_intent_request,
    IntentBackendConfig, IntentHttpResponse, IntentHttpTransport, CHUNK_SIZE_RUNES,
};

#[derive(Deserialize)]
struct Case {
    name: String,
    op: String,
    #[serde(default)]
    content: String,
    #[serde(default, rename = "numRunes")]
    num_runes: usize,
    #[serde(default, rename = "startLine")]
    start_line: i32,
    #[serde(default, rename = "responseBody")]
    response_body: String,
    #[serde(default, rename = "authHeader")]
    auth_header: String,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    count: usize,
    #[serde(default)]
    body: String,
}

#[derive(Serialize)]
struct Envelope {
    data: Value,
    errors: Vec<String>,
}

struct Captured {
    authorization: String,
    content_type: String,
    body: String,
}

struct CapturingTransport {
    captured: Mutex<Option<Captured>>,
    response: IntentHttpResponse,
}

#[async_trait]
impl IntentHttpTransport for CapturingTransport {
    async fn post(
        &self,
        _url: &str,
        auth_header: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<IntentHttpResponse, String> {
        *self.captured.lock().unwrap() = Some(Captured {
            authorization: auth_header.to_owned(),
            content_type: content_type.to_owned(),
            body: String::from_utf8_lossy(body).into_owned(),
        });
        Ok(self.response.clone())
    }
}

async fn run_case(case: &Case) -> Result<Value, String> {
    match case.op.as_str() {
        "count_lines" => Ok(json!({
            "count": count_lines(&case.content, case.num_runes),
        })),
        "chunk_end" => {
            let count = count_lines(&case.content, CHUNK_SIZE_RUNES);
            Ok(json!({
                "count": count,
                "endLine": case.start_line + count - 1,
            }))
        }
        "intent" => {
            let transport = CapturingTransport {
                captured: Mutex::new(None),
                response: IntentHttpResponse {
                    status: case.status,
                    body: case.response_body.as_bytes().to_vec(),
                },
            };
            let backend = IntentBackendConfig {
                url: "http://intent.invalid/predict".to_owned(),
                auth_header: case.auth_header.clone(),
            };
            let response =
                send_intent_request(&transport, &backend, br#"{"query":"what is this"}"#).await?;
            let captured = transport.captured.lock().unwrap().take().unwrap_or(Captured {
                authorization: String::new(),
                content_type: String::new(),
                body: String::new(),
            });
            Ok(json!({
                "intent": response.intent,
                "score": response.score,
                "searchScore": response.search_score,
                "authorization": captured.authorization,
                "contentType": captured.content_type,
                "body": captured.body,
            }))
        }
        "rank_identity" => {
            let used: Vec<Value> = identity_rank(case.count)
                .into_iter()
                .map(|item| json!({"index": item.index, "score": item.score}))
                .collect();
            Ok(json!({"ranker": "identity", "used": used}))
        }
        "url" => process_url_context(&IntentHttpResponse {
            status: case.status,
            body: case.body.as_bytes().to_vec(),
        })
        .map(|data| json!({"title": data.title, "content": data.content})),
        other => Err(format!("unknown op {other}")),
    }
}

#[tokio::test]
async fn cody_context_diff_harness() {
    if std::env::var("SG_M9_48D_CODY_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_48D_CODY_CORPUS").expect("corpus env");
    let out = std::env::var("M9_48D_CODY_OUT").expect("out env");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    for case in cases {
        let (data, errors) = match run_case(&case).await {
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
