//! M9.8it differential harness: `sg-embeddings` vs the Go oracle
//! (`internal/embeddings/embed/client/zz_m9it_oracle_test.go`).
//!
//! The corpus scripts every HTTP response, so requests (method/path/query/
//! selected headers/body), response parsing, retry-on-null, failed indices,
//! dimensions, model identifiers and 429 retry-after mapping are compared
//! against the real Go clients.
//!
//! Gated by `SG_M9IT_DIFF=1`; reads `M9IT_CORPUS`, writes `M9IT_OUT`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_embeddings::modeltransformations;
use sg_embeddings::{
    format_utc_rfc3339_seconds, new_client, EmbeddingsError, EmbeddingsResults, HttpRequest,
    HttpResponse, HttpTransport, Provider, ProviderConfig,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Corpus {
    client_cases: Vec<ClientCase>,
    model_cases: Vec<ModelCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientCase {
    name: String,
    provider: String,
    #[serde(default = "default_method")]
    method: String,
    model: String,
    dimensions: i64,
    access_token: String,
    #[serde(default)]
    endpoint_path: String,
    inputs: Vec<String>,
    #[serde(default)]
    responses: Vec<ScriptedResponse>,
}

fn default_method() -> String {
    "documents".to_owned()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScriptedResponse {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    body_text: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelCase {
    name: String,
    model: String,
    query: String,
    documents: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct Scripted {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Scripted {
    fn from_corpus(response: &ScriptedResponse) -> Self {
        let body = match &response.body {
            Some(body) => serde_json::to_vec(body).expect("serialize scripted body"),
            None => response.body_text.clone().into_bytes(),
        };
        Self {
            status: if response.status == 0 {
                200
            } else {
                response.status
            },
            headers: response
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            body,
        }
    }
}

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    path: String,
    query: String,
    headers: BTreeMap<String, String>,
    body: Value,
}

#[derive(Debug, Default)]
struct ScriptState {
    responses: Vec<Scripted>,
    index: usize,
    requests: Vec<RecordedRequest>,
}

/// A scripted, recording [`HttpTransport`]: replays the corpus responses in
/// order and records every request the client issues.
struct ScriptTransport {
    state: Mutex<ScriptState>,
}

impl ScriptTransport {
    fn new(responses: Vec<Scripted>) -> Self {
        Self {
            state: Mutex::new(ScriptState {
                responses,
                ..ScriptState::default()
            }),
        }
    }

    fn requests_snapshot(&self) -> Vec<RecordedRequest> {
        self.state
            .lock()
            .expect("script transport lock")
            .requests
            .clone()
    }

    fn handle(&self, request: HttpRequest) -> HttpResponse {
        let mut state = self.state.lock().expect("script transport lock");
        let parsed = url::Url::parse(&request.url).expect("request url");
        state.requests.push(RecordedRequest {
            method: request.method,
            path: parsed.path().to_owned(),
            query: parsed.query().unwrap_or("").to_owned(),
            headers: request
                .headers
                .iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
                .collect(),
            body: if request.body.is_empty() {
                Value::Null
            } else {
                match serde_json::from_slice(&request.body) {
                    Ok(body) => body,
                    Err(_) => Value::String(String::from_utf8_lossy(&request.body).into_owned()),
                }
            },
        });
        if state.index >= state.responses.len() {
            return HttpResponse {
                status: 599,
                headers: Vec::new(),
                body: b"unexpected request".to_vec(),
            };
        }
        let scripted = state.responses[state.index].clone();
        state.index += 1;
        HttpResponse {
            status: scripted.status,
            headers: scripted.headers,
            body: scripted.body,
        }
    }
}

#[async_trait]
impl HttpTransport for ScriptTransport {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        Ok(self.handle(request))
    }
}

fn describe_dimensions(client: &Arc<dyn sg_embeddings::EmbeddingsClient>) -> Value {
    match client.get_dimensions() {
        Ok(dimensions) => json!(dimensions),
        Err(message) => json!({"error": message}),
    }
}

fn describe_result(result: Result<EmbeddingsResults, EmbeddingsError>, base_url: &str) -> Value {
    match result {
        Err(EmbeddingsError::RateLimitExceeded(retry_after)) => match retry_after {
            sg_embeddings::RetryAfter::Seconds(seconds) => {
                json!({"error": {"kind": "rate-limit", "retryAfter": {"seconds": seconds}}})
            }
            sg_embeddings::RetryAfter::Date(date) => json!({
                "error": {
                    "kind": "rate-limit",
                    "retryAfter": {"date": format_utc_rfc3339_seconds(date)},
                }
            }),
        },
        Err(EmbeddingsError::Other(message)) => json!({
            "error": {
                "kind": "other",
                "message": message.replace(base_url, "{mock}"),
            }
        }),
        Ok(results) if results.is_empty_response() => json!({"empty": true}),
        Ok(results) => json!({
            "ok": {
                "embeddings": results
                    .embeddings
                    .iter()
                    .map(|value| format!("{value:.6}"))
                    .collect::<Vec<_>>(),
                "failed": results.failed,
                "dimensions": results.dimensions,
            }
        }),
    }
}

async fn run_client_case(case: &ClientCase) -> Value {
    let provider = Provider::try_from_name(&case.provider)
        .unwrap_or_else(|error| panic!("{}: {error}", case.name));
    let base_url = "http://test.local";
    let endpoint = match provider {
        Provider::AzureOpenAI => base_url.to_owned(),
        Provider::Sourcegraph | Provider::OpenAI => format!("{base_url}{}", case.endpoint_path),
    };
    let config = ProviderConfig {
        provider,
        access_token: case.access_token.clone(),
        model: case.model.clone(),
        endpoint,
        dimensions: case.dimensions,
    };
    let transport = Arc::new(ScriptTransport::new(
        case.responses.iter().map(Scripted::from_corpus).collect(),
    ));
    let transport_dyn: Arc<dyn HttpTransport> = transport.clone();
    let client =
        new_client(&config, transport_dyn).unwrap_or_else(|error| panic!("{}: {error}", case.name));

    let dimensions = describe_dimensions(&client);
    let result = if case.method == "query" {
        let query = case.inputs.first().cloned().unwrap_or_default();
        client.get_query_embedding(query).await
    } else {
        client.get_document_embeddings(case.inputs.clone()).await
    };
    let described = describe_result(result, base_url);
    let model_identifier = client.get_model_identifier();

    let requests = transport
        .requests_snapshot()
        .into_iter()
        .map(|request| {
            json!({
                "method": request.method,
                "path": request.path,
                "query": request.query,
                "headers": request.headers,
                "body": request.body,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "name": case.name,
        "provider": case.provider,
        "modelIdentifier": model_identifier,
        "dimensions": dimensions,
        "requests": requests,
        "result": described,
    })
}

#[tokio::test]
async fn embeddings_client_diff() {
    if std::env::var("SG_M9IT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IT_CORPUS").expect("M9IT_CORPUS");
    let out_path = std::env::var("M9IT_OUT").expect("M9IT_OUT");
    let corpus: Corpus = serde_json::from_slice(
        &std::fs::read(&corpus_path).expect("read embeddings client corpus"),
    )
    .expect("parse embeddings client corpus");

    let mut client_cases = Vec::with_capacity(corpus.client_cases.len());
    for case in &corpus.client_cases {
        client_cases.push(run_client_case(case).await);
    }

    let model_cases = corpus
        .model_cases
        .iter()
        .map(|case| {
            json!({
                "name": case.name,
                "query": modeltransformations::apply_to_query(&case.query, &case.model),
                "documents": modeltransformations::apply_to_documents(
                    &case.documents,
                    &case.model,
                ),
            })
        })
        .collect::<Vec<_>>();

    std::fs::write(
        &out_path,
        serde_json::to_string_pretty(&json!({
            "clientCases": client_cases,
            "modelCases": model_cases,
        }))
        .expect("serialize output"),
    )
    .expect("write output");
}
