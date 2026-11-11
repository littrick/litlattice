//! `internal/embeddings/embed/client/azureopenai/client.go`.
//!
//! The baseline delegates the wire protocol to `github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai`
//! v0.5.0. This module emits the exact request the SDK emits (see
//! `azopenai/client.go:302` and `custom_client.go:225`):
//!
//! ```text
//! POST {endpoint}/openai/deployments/{url.PathEscape(model)}/embeddings?api-version=2024-02-15-preview
//! api-key: <token>
//! Accept: application/json
//! Content-Type: application/json
//! {"input": ["..."], "model": "<model>"}
//! ```
//!
//! Azure embeds one input at a time; any per-item failure (HTTP error or three
//! null responses) is reported as a failed index rather than a top-level
//! error. There is no rate-limit mapping, matching the baseline.
//!
//! The SDK's default retry policy is part of the request shape: retryable
//! statuses (408, 429, 500, 502, 503, 504) and transport errors are attempted
//! up to `azcore`'s three retries with exponential backoff, honoring
//! `retry-after-ms`/`x-ms-retry-after-ms`/`retry-after` (capped at 60s).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::modeltransformations;
use crate::{
    header, json_body, path_escape, EmbeddingsClient, EmbeddingsError, EmbeddingsResults,
    HttpRequest, HttpResponse, HttpTransport,
};

/// `api-version` pinned by `azopenai` v0.5.0.
pub const AZURE_API_VERSION: &str = "2024-02-15-preview";

/// `azcore`'s `defaultMaxRetries`; total attempts are this plus the first try.
const MAX_RETRIES: usize = 3;
/// `azcore`'s default `RetryDelay`.
const RETRY_DELAY_MILLIS: u64 = 800;
/// `azcore`'s default `MaxRetryDelay`.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
/// `azcore`'s default `StatusCodes`.
const RETRYABLE_STATUS_CODES: [u16; 6] = [408, 429, 500, 502, 503, 504];

/// `calcDelay`: `((2 ^ try) - 1) * RetryDelay`, with `try` 1-based. The
/// baseline's [0.8, 1.3) jitter only changes timing, not observable requests.
fn calc_delay(retry: usize) -> Duration {
    Duration::from_millis(RETRY_DELAY_MILLIS * ((1u64 << retry) - 1))
}

/// `shared.RetryAfter`: retry-after-ms, x-ms-retry-after-ms, then retry-after
/// (seconds or an HTTP date).
fn retry_after_delay(headers: &[(String, String)]) -> Option<Duration> {
    for (name, is_seconds) in [
        ("retry-after-ms", false),
        ("x-ms-retry-after-ms", false),
        ("retry-after", true),
    ] {
        let Some(value) = header(headers, name) else {
            continue;
        };
        if let Ok(number) = value.parse::<i64>() {
            if number > 0 {
                let number = u64::try_from(number).unwrap_or(u64::MAX);
                return Some(if is_seconds {
                    Duration::from_secs(number)
                } else {
                    Duration::from_millis(number)
                });
            }
        }
        if is_seconds {
            if let Some(date) = crate::parse_rfc1123(value) {
                let delay = date - OffsetDateTime::now_utc();
                if delay > time::SignedDuration::ZERO {
                    return Some(delay.unsigned_abs());
                }
            }
        }
    }
    None
}

pub struct AzureOpenAiClient {
    transport: Arc<dyn HttpTransport>,
    model: String,
    dimensions: i64,
    endpoint: String,
    access_token: String,
}

impl AzureOpenAiClient {
    pub fn new(
        transport: Arc<dyn HttpTransport>,
        model: String,
        dimensions: i64,
        endpoint: String,
        access_token: String,
    ) -> Self {
        Self {
            transport,
            model,
            dimensions,
            endpoint,
            access_token,
        }
    }
}

/// `azopenai.EmbeddingsOptions` (`input`, `model` = deployment name).
#[derive(Debug, Serialize)]
struct EmbeddingsOptions<'a> {
    input: &'a [String],
    model: &'a str,
}

/// `azopenai.GetEmbeddingsResponse.Embeddings`.
#[derive(Debug, Default, Deserialize)]
struct GetEmbeddingsResponse {
    #[serde(default)]
    data: Vec<EmbeddingItem>,
}

/// `azopenai.EmbeddingItem`.
#[derive(Debug, Default, Deserialize)]
struct EmbeddingItem {
    #[serde(default)]
    #[allow(dead_code)]
    index: Option<i64>,
    /// `null` decodes to `None` (the baseline's nil slice).
    #[serde(default)]
    embedding: Option<Vec<f32>>,
}

impl AzureOpenAiClient {
    fn request_url(&self) -> String {
        format!(
            "{}/openai/deployments/{}/embeddings?api-version={AZURE_API_VERSION}",
            self.endpoint.trim_end_matches('/'),
            path_escape(&self.model)
        )
    }

    fn status_error(&self, response: &HttpResponse) -> EmbeddingsError {
        let body = String::from_utf8_lossy(&response.body[..response.body.len().min(1024)]);
        EmbeddingsError::Other(format!(
            "azure openai embeddings failed with status {}: {body}",
            response.status
        ))
    }

    async fn do_request(&self, input: &str) -> Result<GetEmbeddingsResponse, EmbeddingsError> {
        let input = vec![input.to_owned()];
        let body = EmbeddingsOptions {
            input: &input,
            model: &self.model,
        };
        let request = HttpRequest {
            method: "POST".to_owned(),
            url: self.request_url(),
            headers: vec![
                ("api-key".to_owned(), self.access_token.clone()),
                ("Accept".to_owned(), "application/json".to_owned()),
                ("Content-Type".to_owned(), "application/json".to_owned()),
            ],
            body: json_body(&body)?,
        };

        // `azcore`'s retry policy: attempts are 1-based, and `MAX_RETRIES`
        // retries follow the initial attempt.
        let mut attempt = 1usize;
        loop {
            let response = match self.transport.execute(request.clone()).await {
                Ok(response) => response,
                Err(error) => {
                    if attempt > MAX_RETRIES {
                        return Err(EmbeddingsError::Other(error));
                    }
                    tokio::time::sleep(calc_delay(attempt)).await;
                    attempt += 1;
                    continue;
                }
            };

            if response.status == 200 {
                return serde_json::from_slice(&response.body)
                    .map_err(|err| EmbeddingsError::Other(err.to_string()));
            }

            if !RETRYABLE_STATUS_CODES.contains(&response.status) || attempt > MAX_RETRIES {
                return Err(self.status_error(&response));
            }

            let delay = match retry_after_delay(&response.headers) {
                Some(delay) if delay > MAX_RETRY_DELAY => {
                    // The SDK does not retry when the server asks for more than
                    // its maximum delay.
                    return Err(self.status_error(&response));
                }
                Some(delay) => delay,
                None => calc_delay(attempt),
            };
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> Result<EmbeddingsResults, EmbeddingsError> {
        for text in &texts {
            if text.is_empty() {
                // Azure OpenAI errors on any empty string in `input`.
                return Err(EmbeddingsError::Other(
                    "cannot generate embeddings for an empty string".to_owned(),
                ));
            }
        }

        let dimensionality = usize::try_from(self.dimensions.max(0)).unwrap_or(0);
        let mut embeddings = Vec::with_capacity(texts.len() * dimensionality);
        let mut failed = Vec::new();
        for (index, input) in texts.iter().enumerate() {
            // Azure OpenAI only supports a single input per request.
            match self
                .request_single_embedding_with_retry_on_null(&input.replace('\n', " "), 3)
                .await
            {
                Ok(data) => embeddings.extend_from_slice(&data),
                Err(_) => {
                    failed.push(index as i64);
                    // Reslice to provide a zero value embedding for the failed
                    // chunk.
                    embeddings.resize(embeddings.len() + dimensionality, 0.0);
                }
            }
        }

        Ok(EmbeddingsResults {
            embeddings,
            failed,
            dimensions: dimensionality,
        })
    }

    async fn request_single_embedding_with_retry_on_null(
        &self,
        input: &str,
        retries: usize,
    ) -> Result<Vec<f32>, EmbeddingsError> {
        for _ in 0..retries {
            let response = self.do_request(input).await?;
            if response.data.len() != 1 {
                continue;
            }
            let embedding = response.data[0].embedding.clone().unwrap_or_default();
            if embedding.is_empty() {
                continue;
            }
            return Ok(embedding);
        }
        Err(EmbeddingsError::Other(format!(
            "null response for embedding after {retries} retries"
        )))
    }
}

#[async_trait]
impl EmbeddingsClient for AzureOpenAiClient {
    async fn get_query_embedding(
        &self,
        query: String,
    ) -> Result<EmbeddingsResults, EmbeddingsError> {
        let transformed =
            modeltransformations::apply_to_query(&query, &self.get_model_identifier());
        self.get_embeddings(vec![transformed]).await
    }

    async fn get_document_embeddings(
        &self,
        documents: Vec<String>,
    ) -> Result<EmbeddingsResults, EmbeddingsError> {
        let transformed =
            modeltransformations::apply_to_documents(&documents, &self.get_model_identifier());
        self.get_embeddings(transformed).await
    }

    fn get_dimensions(&self) -> Result<usize, String> {
        if self.dimensions <= 0 {
            return Err("invalid config for embeddings.dimensions, must be > 0".to_owned());
        }
        Ok(self.dimensions as usize)
    }

    fn get_model_identifier(&self) -> String {
        format!("azure-openai/{}", self.model)
    }
}
