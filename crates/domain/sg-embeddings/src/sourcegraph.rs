//! `internal/embeddings/embed/client/sourcegraph/client.go`: the Cody Gateway
//! embeddings client.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::modeltransformations;
use crate::{
    header, http_error_message, json_body, EmbeddingsClient, EmbeddingsError, EmbeddingsResults,
    HttpRequest, HttpTransport, RetryAfter,
};

/// The default model whose identifier already carries its provider prefix.
const DEFAULT_MODEL_IDENTIFIER: &str = "openai/text-embedding-ada-002";

pub struct SourcegraphClient {
    transport: Arc<dyn HttpTransport>,
    model: String,
    dimensions: i64,
    endpoint: String,
    access_token: String,
}

impl SourcegraphClient {
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

/// `codygateway.EmbeddingsRequest`; `IsQuery` has no `omitempty`, so it is
/// always serialized as `false` for indexing calls.
#[derive(Debug, Serialize)]
struct EmbeddingsRequest<'a> {
    model: &'a str,
    input: &'a [String],
    #[serde(rename = "isQuery")]
    is_query: bool,
}

/// `codygateway.EmbeddingsResponse`.
#[derive(Debug, Default, Deserialize)]
struct EmbeddingsResponse {
    #[serde(default)]
    embeddings: Vec<Embedding>,
    #[serde(default)]
    #[allow(dead_code)]
    model: String,
    #[serde(default)]
    dimensions: i64,
}

/// `codygateway.Embedding`.
#[derive(Debug, Default, Deserialize)]
struct Embedding {
    #[serde(default)]
    index: i64,
    /// `null` decodes to `None` (the baseline's nil slice).
    #[serde(default)]
    data: Option<Vec<f32>>,
}

impl SourcegraphClient {
    async fn do_request(
        &self,
        request: EmbeddingsRequest<'_>,
    ) -> Result<EmbeddingsResponse, EmbeddingsError> {
        let mut request_headers = vec![
            ("Content-Type".to_owned(), "application/json".to_owned()),
            (
                "Authorization".to_owned(),
                format!("Bearer {}", self.access_token),
            ),
        ];
        if request.input.len() > 1 {
            request_headers.push((
                "X-Cody-Embed-Batch-Size".to_owned(),
                request.input.len().to_string(),
            ));
        }
        let response = self
            .transport
            .execute(HttpRequest {
                method: "POST".to_owned(),
                url: self.endpoint.clone(),
                headers: request_headers,
                body: json_body(&request)?,
            })
            .await
            .map_err(EmbeddingsError::Other)?;

        if response.status != 200 {
            if response.status == 429 {
                if let Some(retry_after) = header(&response.headers, "retry-after") {
                    // Two valid formats: integer seconds or an RFC1123 date.
                    if let Ok(seconds) = retry_after.parse::<i64>() {
                        return Err(EmbeddingsError::RateLimitExceeded(RetryAfter::Seconds(
                            seconds,
                        )));
                    }
                    if let Some(date) = crate::parse_rfc1123(retry_after) {
                        return Err(EmbeddingsError::RateLimitExceeded(RetryAfter::Date(date)));
                    }
                    // Unparseable: fall through to the generic error.
                }
            }
            return Err(EmbeddingsError::Other(http_error_message(
                "POST",
                &self.endpoint,
                response.status,
                &response.body,
            )));
        }

        serde_json::from_slice(&response.body)
            .map_err(|err| EmbeddingsError::Other(err.to_string()))
    }

    async fn get_embeddings(
        &self,
        texts: Vec<String>,
    ) -> Result<EmbeddingsResults, EmbeddingsError> {
        let response = self
            .do_request(EmbeddingsRequest {
                model: &self.model,
                input: &texts,
                is_query: false,
            })
            .await?;

        if response.embeddings.is_empty() {
            return Ok(EmbeddingsResults::default());
        }

        // Ensure embedding responses are sorted in the original order.
        let mut embeddings = response.embeddings;
        embeddings.sort_by_key(|embedding| embedding.index);

        let dimensionality = usize::try_from(response.dimensions.max(0)).unwrap_or(0);
        let mut flat = Vec::with_capacity(embeddings.len() * dimensionality);
        let mut failed = Vec::new();
        for embedding in embeddings {
            match embedding.data {
                Some(data) if !data.is_empty() => flat.extend_from_slice(&data),
                _ => {
                    let index = usize::try_from(embedding.index.max(0)).unwrap_or(0);
                    match self
                        .request_single_embedding_with_retry_on_null(
                            &self.model,
                            texts.get(index).cloned().unwrap_or_default(),
                            3,
                        )
                        .await
                    {
                        Ok(data) => flat.extend_from_slice(&data),
                        Err(_) => {
                            failed.push(embedding.index);
                            // Reslice to provide a zero value embedding for the
                            // failed chunk.
                            flat.resize(flat.len() + dimensionality, 0.0);
                        }
                    }
                }
            }
        }

        Ok(EmbeddingsResults {
            embeddings: flat,
            failed,
            dimensions: dimensionality,
        })
    }

    async fn request_single_embedding_with_retry_on_null(
        &self,
        model: &str,
        input: String,
        retries: usize,
    ) -> Result<Vec<f32>, EmbeddingsError> {
        for _ in 0..retries {
            let response = self
                .do_request(EmbeddingsRequest {
                    model,
                    input: std::slice::from_ref(&input),
                    is_query: false,
                })
                .await?;
            if response.embeddings.len() != 1 {
                continue;
            }
            // Go compares `len(nil) == 0` against the configured dimensions.
            let data = response.embeddings[0].data.clone().unwrap_or_default();
            if data.len() as i64 != self.dimensions {
                continue;
            }
            return Ok(data);
        }
        Err(EmbeddingsError::Other(format!(
            "null response for embedding after {retries} retries"
        )))
    }
}

#[async_trait]
impl EmbeddingsClient for SourcegraphClient {
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
        // Special-case the default model, since it already includes the
        // provider name (lets customers migrate from OpenAI to Cody Gateway
        // without invalidating their index).
        if self.model.eq_ignore_ascii_case(DEFAULT_MODEL_IDENTIFIER) {
            return DEFAULT_MODEL_IDENTIFIER.to_owned();
        }
        format!("sourcegraph/{}", self.model)
    }
}
