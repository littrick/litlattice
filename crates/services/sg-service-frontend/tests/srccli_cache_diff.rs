//! M9.19c differential harness: the src-cli version handler's lazy
//! 10-minute cache (`cmd/frontend/internal/httpapi/src_cli.go`).
//!
//! Replays `test_ws/p9-go/data/srccli_cache/corpus.json` through
//! [`SrcCliVersionHandler`] with an injected [`SrcCliVersionSource`] and
//! [`Clock`], writing the same JSON document as the Go oracle
//! (`cmd/frontend/internal/httpapi/zz_m919c_srccli_cache_oracle_test.go`,
//! UNTRACKED, gated by `SG_M919C_SRCCLI=1`).
//!
//! Gated on `M919C_SRCCLI_CORPUS`/`M919C_SRCCLI_OUT`; `run_srccli_cache_diff.sh`
//! sets them.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::Method;
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::json;
use sg_service_frontend::external_api::{
    src_cli_response_with, Clock, SrcCliVersionHandler, SrcCliVersionSource,
};

#[derive(Deserialize)]
struct Corpus {
    max_stale_seconds: u64,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    responses: Vec<Scripted>,
    requests: Vec<RequestSpec>,
}

#[derive(Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Scripted {
    Ok { version: String },
    Status { status: u16 },
    Body { body: String },
    Error { message: String },
}

#[derive(Deserialize)]
struct RequestSpec {
    advance_seconds: u64,
    method: String,
    rest: String,
}

/// Replays the corpus responses, keeping the last one once exhausted.
struct ScriptedSource {
    responses: Vec<Scripted>,
    index: Mutex<usize>,
    calls: AtomicUsize,
}

impl SrcCliVersionSource for ScriptedSource {
    fn version(&self) -> Result<String, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut index = self.index.lock().expect("scripted source lock");
        let position = (*index).min(self.responses.len() - 1);
        *index += 1;
        match self.responses[position].clone() {
            Scripted::Ok { version } => Ok(version),
            Scripted::Status { status } => Err(format!("unexpected status code: {status}")),
            Scripted::Body { .. } => {
                Err("reading version from response payload: invalid JSON".to_owned())
            }
            Scripted::Error { message } => {
                Err(format!("getting version from Sourcegraph: {message}"))
            }
        }
    }
}

struct FakeClock {
    base: Instant,
    offset: Mutex<Duration>,
}

impl FakeClock {
    fn advance(&self, duration: Duration) {
        *self.offset.lock().expect("fake clock lock") += duration;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        self.base + *self.offset.lock().expect("fake clock lock")
    }
}

#[tokio::test]
async fn srccli_cache_diff() {
    let (Ok(corpus_path), Ok(out_path)) = (
        std::env::var("M919C_SRCCLI_CORPUS"),
        std::env::var("M919C_SRCCLI_OUT"),
    ) else {
        eprintln!("M919C_SRCCLI_CORPUS/M919C_SRCCLI_OUT not set; skipping");
        return;
    };
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let mut results = Vec::new();
    for case in &corpus.cases {
        let source = Arc::new(ScriptedSource {
            responses: case.responses.clone(),
            index: Mutex::new(0),
            calls: AtomicUsize::new(0),
        });
        let clock = Arc::new(FakeClock {
            base: Instant::now(),
            offset: Mutex::new(Duration::ZERO),
        });
        let handler = SrcCliVersionHandler::with_max_stale(
            source.clone(),
            clock.clone(),
            Duration::from_secs(corpus.max_stale_seconds),
        );

        for (index, request) in case.requests.iter().enumerate() {
            if request.advance_seconds != 0 {
                clock.advance(Duration::from_secs(request.advance_seconds));
            }
            let method = match request.method.as_str() {
                "GET" => Method::GET,
                "HEAD" => Method::HEAD,
                other => panic!("unsupported method {other}"),
            };
            let response = src_cli_response_with(&method, &request.rest, &handler);
            let status = response.status().as_u16();
            let header = |name: &str| {
                response
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            let location = header("location");
            let content_type = header("content-type");
            let body = response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            results.push(json!({
                "name": case.name,
                "index": index,
                "method": request.method,
                "rest": request.rest,
                "status": status,
                "location": location,
                "content_type": content_type,
                "body": String::from_utf8_lossy(&body),
                "fetches": source.calls.load(Ordering::SeqCst),
            }));
        }
    }

    std::fs::write(&out_path, format!("{}\n", json!(results))).expect("write output");
}
