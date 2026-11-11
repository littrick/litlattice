#![forbid(unsafe_code)]

//! Cody Gateway embeddings client (`internal/embeddings/client.go` plus the
//! request/response types in `internal/embeddings/types.go`).
//!
//! This is the HTTP dependency of `CodyContextClient`: the dotcom client never
//! talks to the gateway directly, it asks an [`EmbeddingsSearch`] implementation
//! (baseline `embeddings.Client`). [`HttpEmbeddingsClient`] reproduces the
//! baseline wire shape:
//!
//! * `POST <endpoint>/search`;
//! * `Content-Type: application/json`;
//! * body = JSON of [`EmbeddingsSearchParameters`];
//! * non-200 → `Embeddings.Search http status <status>: <first 200 bytes>`;
//! * 200 → decode [`EmbeddingCombinedSearchResults`].
//!
//! The transport is injected ([`EmbeddingsHttpTransport`]); the differential
//! harness uses a fake transport so no network is touched. Mult-endpoint
//! rendezvous partitioning from `endpoint.Map` is not ported (see the M9.9c
//! overview §4): the client uses one endpoint.
//!
//! Baseline: `dev/origin/repo` @ `c864f15af`.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// `embeddings.EmbeddingsSearchParameters` (`types.go:51-59`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingsSearchParameters {
    #[serde(rename = "repoNames")]
    pub repo_names: Vec<String>,
    #[serde(rename = "repoIDs")]
    pub repo_ids: Vec<i32>,
    pub query: String,
    #[serde(rename = "codeResultsCount")]
    pub code_results_count: i32,
    #[serde(rename = "textResultsCount")]
    pub text_results_count: i32,
    #[serde(rename = "useDocumentRanks")]
    pub use_document_ranks: bool,
}

/// `embeddings.SearchScoreDetails` (`types.go:182-190`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchScoreDetails {
    #[serde(default)]
    pub score: i32,
    #[serde(rename = "similarityScore", default)]
    pub similarity_score: i32,
    #[serde(rename = "rankScore", default)]
    pub rank_score: i32,
}

impl SearchScoreDetails {
    /// `EmbeddingSearchResult.Score`.
    pub fn score_sum(&self) -> i32 {
        // Go `int32` addition wraps on overflow.
        self.rank_score.wrapping_add(self.similarity_score)
    }
}

/// `embeddings.EmbeddingSearchResult` (`types.go:158-175`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingSearchResult {
    #[serde(rename = "repoName")]
    pub repo_name: String,
    #[serde(default)]
    pub revision: String,
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "startLine", default)]
    pub start_line: i32,
    #[serde(rename = "endLine", default)]
    pub end_line: i32,
    #[serde(rename = "scoreDetails", default)]
    pub score_details: SearchScoreDetails,
}

/// `embeddings.EmbeddingCombinedSearchResults` (`types.go:126-129`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingCombinedSearchResults {
    #[serde(rename = "codeResults", default)]
    pub code_results: Vec<EmbeddingSearchResult>,
    #[serde(rename = "textResults", default)]
    pub text_results: Vec<EmbeddingSearchResult>,
}

impl EmbeddingCombinedSearchResults {
    /// `EmbeddingSearchResults.MergeTruncate` (`types.go:136-150`): sort by
    /// descending score, keep at most `max`.
    pub fn merge_truncate(&self, other: &Self, max_code: i32, max_text: i32) -> Self {
        let mut code = self.code_results.clone();
        code.extend(other.code_results.iter().cloned());
        sort_and_truncate(&mut code, max_code);

        let mut text = self.text_results.clone();
        text.extend(other.text_results.iter().cloned());
        sort_and_truncate(&mut text, max_text);
        Self {
            code_results: code,
            text_results: text,
        }
    }
}

/// `sort.Slice(results, score desc)` + truncation. The baseline sort is not
/// stable; the corpus avoids equal-score ties, keeping the comparison
/// deterministic.
fn sort_and_truncate(results: &mut Vec<EmbeddingSearchResult>, max: i32) {
    results.sort_by_key(|result| std::cmp::Reverse(result.score_details.score_sum()));
    if max >= 0 && (results.len() as i64) > i64::from(max) {
        results.truncate(max as usize);
    }
}

/// One outbound HTTP request. The differential harness records exactly these
/// fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsHttpRequest {
    pub method: String,
    pub url: String,
    pub content_type: String,
    pub body: String,
}

/// One HTTP response (status + body text).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsHttpResponse {
    pub status: u16,
    pub body: String,
}

/// Injectable HTTP transport for the embeddings service (no network in tests).
#[async_trait]
pub trait EmbeddingsHttpTransport: Send + Sync {
    async fn execute(
        &self,
        request: &EmbeddingsHttpRequest,
    ) -> Result<EmbeddingsHttpResponse, String>;
}

/// Production transport over `reqwest`. The baseline `httpcli.InternalDoer`
/// defaults to no timeout (`SRC_HTTP_CLI_INTERNAL_TIMEOUT=0`); `timeout` keeps
/// that default while allowing a configured override.
#[derive(Debug, Clone)]
pub struct ReqwestEmbeddingsHttpClient {
    client: reqwest::Client,
    pub timeout: Option<Duration>,
}

impl ReqwestEmbeddingsHttpClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            timeout: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }
}

impl Default for ReqwestEmbeddingsHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EmbeddingsHttpTransport for ReqwestEmbeddingsHttpClient {
    async fn execute(
        &self,
        request: &EmbeddingsHttpRequest,
    ) -> Result<EmbeddingsHttpResponse, String> {
        let mut builder = self
            .client
            .request(
                reqwest::Method::from_bytes(request.method.as_bytes())
                    .map_err(|error| error.to_string())?,
                &request.url,
            )
            .header(reqwest::header::CONTENT_TYPE, &request.content_type)
            .body(request.body.clone());
        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
        }
        let response = builder.send().await.map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        let body = response.text().await.map_err(|error| error.to_string())?;
        Ok(EmbeddingsHttpResponse { status, body })
    }
}

/// The baseline `embeddings.Client` interface.
#[async_trait]
pub trait EmbeddingsSearch: Send + Sync {
    async fn search(
        &self,
        parameters: &EmbeddingsSearchParameters,
    ) -> Result<EmbeddingCombinedSearchResults, String>;
}

/// `embeddings.client` restricted to one endpoint.
pub struct HttpEmbeddingsClient {
    pub endpoint: String,
    pub transport: std::sync::Arc<dyn EmbeddingsHttpTransport>,
}

impl HttpEmbeddingsClient {
    pub fn new(
        endpoint: impl Into<String>,
        transport: std::sync::Arc<dyn EmbeddingsHttpTransport>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            transport,
        }
    }

    /// `(*client).httpPost(ctx, "search", endpoint, args)`.
    pub fn request_for(&self, parameters: &EmbeddingsSearchParameters) -> EmbeddingsHttpRequest {
        // `if !strings.HasSuffix(url, "/") { url += "/" }`.
        let mut base = self.endpoint.clone();
        if !base.ends_with('/') {
            base.push('/');
        }
        EmbeddingsHttpRequest {
            method: "POST".to_owned(),
            url: format!("{base}search"),
            content_type: "application/json".to_owned(),
            body: serde_json::to_string(parameters).expect("serialize embeddings parameters"),
        }
    }
}

#[async_trait]
impl EmbeddingsSearch for HttpEmbeddingsClient {
    async fn search(
        &self,
        parameters: &EmbeddingsSearchParameters,
    ) -> Result<EmbeddingCombinedSearchResults, String> {
        let request = self.request_for(parameters);
        let response = self.transport.execute(&request).await?;
        if response.status != 200 {
            // Best-effort inclusion of the body, limited to 200 bytes.
            let bytes = response.body.as_bytes();
            let truncated = &bytes[..bytes.len().min(200)];
            return Err(format!(
                "Embeddings.Search http status {}: {}",
                response.status,
                String::from_utf8_lossy(truncated)
            ));
        }
        let mut combined: EmbeddingCombinedSearchResults =
            serde_json::from_str(&response.body).map_err(|error| error.to_string())?;
        // `Search` merges each partition then truncates to the requested
        // counts (`MergeTruncate`); one partition makes this a sort+truncate.
        sort_and_truncate(&mut combined.code_results, parameters.code_results_count);
        sort_and_truncate(&mut combined.text_results, parameters.text_results_count);
        Ok(combined)
    }
}
