//! `internal/embeddings/embed/client/openai/client.go`.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::modeltransformations;
use crate::{
    http_error_message, json_body, EmbeddingsClient, EmbeddingsError, EmbeddingsResults,
    HttpRequest, HttpTransport,
};

pub struct OpenAiClient {
    transport: Arc<dyn HttpTransport>,
    model: String,
    dimensions: i64,
    endpoint: String,
    access_token: String,
}

impl OpenAiClient {
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

/// `openaiEmbeddingAPIRequest`.
#[derive(Debug, Serialize)]
struct EmbeddingsApiRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

/// `openaiEmbeddingAPIResponse`.
#[derive(Debug, Default, Deserialize)]
struct EmbeddingsApiResponse {
    #[serde(default)]
    data: Vec<EmbeddingsApiResponseData>,
}

/// `openaiEmbeddingAPIResponseData`.
#[derive(Debug, Default, Deserialize)]
struct EmbeddingsApiResponseData {
    #[serde(default)]
    index: i64,
    /// `null` decodes to `None` (the baseline's nil slice).
    #[serde(default)]
    embedding: Option<Vec<f32>>,
}

impl OpenAiClient {
    async fn do_request(
        &self,
        request: EmbeddingsApiRequest<'_>,
    ) -> Result<EmbeddingsApiResponse, EmbeddingsError> {
        let response = self
            .transport
            .execute(HttpRequest {
                method: "POST".to_owned(),
                url: self.endpoint.clone(),
                headers: vec![
                    ("Content-Type".to_owned(), "application/json".to_owned()),
                    (
                        "Authorization".to_owned(),
                        format!("Bearer {}", self.access_token),
                    ),
                ],
                body: json_body(&request)?,
            })
            .await
            .map_err(EmbeddingsError::Other)?;

        if response.status != 200 {
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
        for text in &texts {
            if text.is_empty() {
                // The OpenAI API errors on any empty string in `input`, so
                // fail fast instead of making retryable requests.
                return Err(EmbeddingsError::Other(
                    "cannot generate embeddings for an empty string".to_owned(),
                ));
            }
        }

        let response = self
            .do_request(EmbeddingsApiRequest {
                model: &self.model,
                input: &texts,
            })
            .await?;

        if response.data.is_empty() {
            return Ok(EmbeddingsResults::default());
        }

        // Ensure embedding responses are sorted in the original order.
        let mut data = response.data;
        data.sort_by_key(|embedding| embedding.index);

        let dimensionality = data[0].embedding.as_ref().map(Vec::len).unwrap_or_default();
        let mut embeddings = Vec::with_capacity(data.len() * dimensionality);
        let mut failed = Vec::new();
        for embedding in data {
            match embedding.embedding {
                Some(values) if !values.is_empty() => embeddings.extend_from_slice(&values),
                _ => {
                    let index = usize::try_from(embedding.index.max(0)).unwrap_or(0);
                    match self
                        .request_single_embedding_with_retry_on_null(
                            texts.get(index).cloned().unwrap_or_default(),
                            3,
                        )
                        .await
                    {
                        // `resp.Data[0].Embedding`.
                        Ok(response) => {
                            if let Some(values) = response
                                .data
                                .first()
                                .and_then(|item| item.embedding.clone())
                            {
                                embeddings.extend_from_slice(&values);
                            }
                        }
                        Err(_) => {
                            failed.push(embedding.index);
                            // Reslice to provide a zero value embedding for the
                            // failed chunk.
                            embeddings.resize(embeddings.len() + dimensionality, 0.0);
                        }
                    }
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
        input: String,
        retries: usize,
    ) -> Result<EmbeddingsApiResponse, EmbeddingsError> {
        for _ in 0..retries {
            let response = self
                .do_request(EmbeddingsApiRequest {
                    model: &self.model,
                    input: std::slice::from_ref(&input),
                })
                .await?;
            if response.data.len() != 1 {
                continue;
            }
            let has_embedding = response.data[0]
                .embedding
                .as_ref()
                .is_some_and(|embedding| !embedding.is_empty());
            if !has_embedding {
                continue;
            }
            return Ok(response);
        }
        Err(EmbeddingsError::Other(format!(
            "null response for embedding after {retries} retries"
        )))
    }
}

#[async_trait]
impl EmbeddingsClient for OpenAiClient {
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
        format!("openai/{}", self.model)
    }
}
