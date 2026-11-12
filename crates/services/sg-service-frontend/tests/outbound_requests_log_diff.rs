#![forbid(unsafe_code)]

//! M9.21b differential harness (Rust side): the outbound request log
//! middleware (`internal/httpcli/redis_logger_middleware.go`) and the
//! `Query.outboundRequests` read path against a real local Redis server.
//!
//! Replays `test_ws/p9-go/data/outbound_requests_log/corpus.json` through
//! `sg-httpcli`'s middleware and reader and prints the canonical `M921B ...`
//! lines both sides must agree on. `id`, `startedAt`, `duration`,
//! `creationStackFrame` and `callStackFrame` are normalized by
//! `run_outbound_requests_log_diff.sh`; every other JSON byte (field order,
//! map ordering, HTML escaping, headers, body, status, error) must match.
//!
//! Env:
//!
//! * `SG_M921B_DIFF=1`
//! * `M921B_CORPUS` (corpus path)
//! * `M921B_REDIS_ADDR` (default `127.0.0.1:6379`)

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use sg_httpcli::{
    get_outbound_request_log_item, get_outbound_request_log_items, log_request,
    outbound_requests_fifo, DoResult, Doer, OutboundRequestLogItem,
    OutboundRequestLogSettings, RequestSpec, ResponseSpec,
};
use sg_redis::KeyValue;

/// The exact Go Redis list key (`v2:outbound-requests`).
const REDIS_KEY: &str = "v2:outbound-requests";

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    limit: i32,
    #[serde(default)]
    is_dev: bool,
    #[serde(default)]
    redact: bool,
    #[serde(default)]
    steps: Vec<Step>,
    #[serde(default)]
    after_index: Option<usize>,
    #[serde(default)]
    get_by_id_index: Option<usize>,
    #[serde(default)]
    missing_key: Option<String>,
}

#[derive(Deserialize)]
struct Step {
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<Vec<String>>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    response: Option<ResponseCase>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct ResponseCase {
    status: i32,
    #[serde(default)]
    headers: Vec<Vec<String>>,
}

fn header_pairs(headers: &[Vec<String>]) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|pair| {
            assert_eq!(pair.len(), 2, "header pair must have two elements");
            (pair[0].clone(), pair[1].clone())
        })
        .collect()
}

/// Injected transport: returns the corpus response/error without any network.
struct FakeDoer {
    result: DoResult,
}

#[async_trait]
impl Doer for FakeDoer {
    async fn do_request(&self, _req: &RequestSpec) -> DoResult {
        self.result.clone()
    }
}

fn urls(items: &[OutboundRequestLogItem]) -> String {
    items
        .iter()
        .map(|item| item.url.clone())
        .collect::<Vec<_>>()
        .join(",")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbound_requests_log_matches_go() {
    if std::env::var("SG_M921B_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M921B_CORPUS").expect("M921B_CORPUS");
    let endpoint =
        std::env::var("M921B_REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("read corpus"))
            .expect("corpus json");
    let kv = KeyValue::connect(&endpoint).await.expect("connect redis");

    let mut lines: Vec<String> = Vec::new();
    for case in &corpus.cases {
        run_case(&kv, case, &mut lines).await;
    }

    let body = format!("{}\n", lines.join("\n"));
    match std::env::var("M921B_OUT") {
        Ok(out_dir) => {
            std::fs::create_dir_all(&out_dir).expect("create out dir");
            std::fs::write(format!("{out_dir}/out.txt"), body).expect("write out.txt");
        }
        Err(_) => print!("{body}"),
    }
}

async fn run_case(kv: &KeyValue, case: &Case, lines: &mut Vec<String>) {
    // Isolate cases on the exact production key.
    kv.del(REDIS_KEY).await.expect("clear key");
    let fifo = outbound_requests_fifo(kv.clone(), case.limit);
    let settings = OutboundRequestLogSettings {
        limit: case.limit,
        redact_sensitive_headers: case.redact,
        is_dev: case.is_dev,
    };

    for step in &case.steps {
        let request = RequestSpec {
            method: step.method.clone(),
            url: step.url.clone(),
            headers: header_pairs(&step.headers),
            body: step.body.as_ref().map(|body| body.as_bytes().to_vec()),
        };
        let doer = FakeDoer {
            result: DoResult {
                response: step.response.as_ref().map(|response| ResponseSpec {
                    status_code: response.status,
                    headers: header_pairs(&response.headers),
                }),
                error: step.error.clone(),
            },
        };
        let _ = log_request(&doer, &settings, &fifo, &request).await;
        // Keep the nanosecond IDs distinct, like the Go oracle's sleep.
        tokio::time::sleep(Duration::from_millis(3)).await;
    }

    let raw = fifo.all().await.expect("read fifo");
    lines.push(format!("M921B CASE {}", case.name));
    let mut keys = kv.keys(REDIS_KEY).await.expect("keys");
    keys.sort();
    lines.push(format!("M921B KEYS {}", keys.join(",")));
    lines.push(format!("M921B COUNT {}", raw.len()));
    for (index, bytes) in raw.iter().enumerate() {
        lines.push(format!(
            "M921B RAW {index} {}",
            String::from_utf8_lossy(bytes)
        ));
    }

    let logged = get_outbound_request_log_items(&fifo, "", case.limit)
        .await
        .expect("read all");
    lines.push(format!("M921B READ_ALL {}", urls(&logged)));

    if let Some(index) = case.after_index {
        if let Some(anchor) = logged.get(index) {
            let after = anchor.id.clone();
            let after_items = get_outbound_request_log_items(&fifo, &after, case.limit)
                .await
                .expect("read after");
            lines.push(format!("M921B READ_AFTER {}", urls(&after_items)));
        }
    }

    if let Some(index) = case.get_by_id_index {
        if let Some(anchor) = logged.get(index) {
            match get_outbound_request_log_item(&fifo, &anchor.id).await {
                Ok(item) => lines.push(format!("M921B GET_BY_ID {}", item.url)),
                Err(error) => lines.push(format!("M921B GET_BY_ID ERR {error}")),
            }
        }
    }

    if let Some(key) = &case.missing_key {
        match get_outbound_request_log_item(&fifo, key).await {
            Ok(item) => lines.push(format!("M921B GET_MISSING OK {}", item.url)),
            Err(error) => lines.push(format!("M921B GET_MISSING {error}")),
        }
    }

    kv.del(REDIS_KEY).await.expect("clear key");
}
