#![forbid(unsafe_code)]

//! Differential harness for the M9.23b outbound Batch Changes webhooks.
//!
//! Reads the shared corpus at `M923B_CORPUS`, drives
//! [`sg_extsvc::batches_webhooks::enqueue_batch_change`] /
//! [`enqueue_changeset`] with a canned internal GraphQL response, and writes
//! the produced payloads, the captured GraphQL requests and the resulting
//! `outbound_webhook_jobs` rows to `M923B_OUT`.
//!
//! The untracked Go oracle
//! (`internal/batches/webhooks/zz_m923b_webhooks_oracle_test.go`) runs the real
//! `webhooks.Enqueue` path over the same corpus and database; the runner script
//! compares the two outputs.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use sg_extsvc::batches_webhooks::{
    batch_change_gql_id, changeset_gql_id, enqueue_batch_change, enqueue_changeset,
    relay_id_kind, WebhookEnqueueSink, EVENT_TYPES,
};
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};
use sg_service_worker::jobs::outbound_webhooks::store::WebhookJobQueue;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    event_type: String,
    id: i64,
    internal_api_url: String,
    response: Value,
}

#[derive(Serialize, Default, Clone)]
struct RequestOut {
    method: String,
    url: String,
    content_type: String,
    body: String,
}

#[derive(Serialize)]
struct EventTypeOut {
    key: String,
    description: String,
}

#[derive(Serialize)]
struct CaseOut {
    name: String,
    ok: bool,
    gql_id: String,
    gql_id_kind: String,
    request: RequestOut,
    event_type: Option<String>,
    payload: Option<String>,
}

#[derive(Serialize)]
struct RowOut {
    event_type: String,
    scope: Option<String>,
    key_id: Option<String>,
    payload: String,
}

#[derive(Serialize)]
struct Out {
    event_types: Vec<EventTypeOut>,
    cases: Vec<CaseOut>,
    rows: Vec<RowOut>,
}

/// Records the request and replies with the canned GraphQL response.
struct FakeHttp {
    response: Vec<u8>,
    last: Mutex<RequestOut>,
}

#[async_trait::async_trait]
impl HttpDoer for FakeHttp {
    async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let content_type = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("Content-Type"))
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        *self.last.lock().expect("request lock") = RequestOut {
            method: request.method,
            url: request.url,
            content_type,
            body: String::from_utf8_lossy(&request.body).into_owned(),
        };
        Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: self.response.clone(),
        })
    }
}

/// Writes every enqueued payload to the real queue table and remembers it.
struct QueueSink {
    queue: WebhookJobQueue,
    last: Mutex<Option<(String, Option<String>, Vec<u8>)>>,
}

#[async_trait::async_trait]
impl WebhookEnqueueSink for QueueSink {
    async fn enqueue(
        &self,
        event_type: &str,
        scope: Option<&str>,
        payload: &[u8],
    ) -> Result<(), String> {
        self.queue
            .create(event_type, scope, payload)
            .await
            .map_err(|err| err.to_string())?;
        *self.last.lock().expect("sink lock") =
            Some((event_type.to_owned(), scope.map(str::to_owned), payload.to_vec()));
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn batches_webhooks_diff() {
    let Ok(corpus_path) = std::env::var("M923B_CORPUS") else {
        eprintln!("M923B_CORPUS unset; skipping");
        return;
    };
    let out_path = std::env::var("M923B_OUT").expect("M923B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");

    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("decode corpus");
    let pool = PgPool::connect(&dsn).await.expect("connect database");

    let queue = WebhookJobQueue::new(pool.clone(), None);
    let sink = QueueSink {
        queue,
        last: Mutex::new(None),
    };

    let mut out = Out {
        event_types: EVENT_TYPES
            .iter()
            .map(|(key, description)| EventTypeOut {
                key: (*key).to_owned(),
                description: (*description).to_owned(),
            })
            .collect(),
        cases: Vec::with_capacity(corpus.cases.len()),
        rows: Vec::new(),
    };

    for case in &corpus.cases {
        *sink.last.lock().expect("sink lock") = None;
        let http = FakeHttp {
            response: serde_json::to_vec(&case.response).expect("encode response"),
            last: Mutex::new(RequestOut::default()),
        };

        let changeset = case.kind == "changeset";
        let gql_id = if changeset {
            changeset_gql_id(case.id)
        } else {
            batch_change_gql_id(case.id)
        };
        let result = if changeset {
            enqueue_changeset(&sink, &http, &case.internal_api_url, &case.event_type, &gql_id).await
        } else {
            enqueue_batch_change(&sink, &http, &case.internal_api_url, &case.event_type, &gql_id)
                .await
        };

        let request = http.last.lock().expect("request lock").clone();
        let recorded = sink.last.lock().expect("sink lock").take();
        let (event_type, payload) = match (result.is_ok(), recorded) {
            (true, Some((event_type, _scope, payload))) => (
                Some(event_type),
                Some(String::from_utf8_lossy(&payload).into_owned()),
            ),
            _ => (None, None),
        };
        out.cases.push(CaseOut {
            name: case.name.clone(),
            ok: result.is_ok(),
            gql_id_kind: relay_id_kind(&gql_id).unwrap_or_default(),
            gql_id,
            request,
            event_type,
            payload,
        });
    }

    let rows = sqlx::query(
        r#"
SELECT event_type, scope, encryption_key_id, payload
FROM outbound_webhook_jobs
ORDER BY id ASC
"#,
    )
    .fetch_all(&pool)
    .await
    .expect("read queue rows");
    for row in rows {
        use sqlx::Row;
        out.rows.push(RowOut {
            event_type: row.get("event_type"),
            scope: row.get("scope"),
            key_id: row.get("encryption_key_id"),
            payload: String::from_utf8_lossy(&row.get::<Vec<u8>, _>("payload")).into_owned(),
        });
    }

    let encoded = serde_json::to_string_pretty(&out).expect("encode output");
    std::fs::write(&out_path, format!("{encoded}\n")).expect("write output");
    eprintln!("m923b cases={} rows={}", out.cases.len(), out.rows.len());
}
