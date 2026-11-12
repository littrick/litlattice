#![forbid(unsafe_code)]

//! `cmd/embeddings/shared`: configuration, the legacy JSON search handler and
//! the startup surface of the standalone `embeddings` binary.
//!
//! Ported:
//!
//! * `shared.Config.Load`/`Validate` — the `EMBEDDINGS_*` upload-store
//!   configuration (backend/credentials/bucket), `EMBEDDINGS_CACHE_SIZE`
//!   (`go-humanize.ParseBytes`, default `6GiB`) and the collected validation
//!   errors;
//! * `NewHandler` — the `POST /search` mux, including the baseline's exact
//!   error texts and status codes;
//! * `searchRepoEmbeddingIndexes` ordering: query embedding first, then the
//!   per-repo index lookup/model check/merge-truncate.
//!
//! Not ported (see the M9.17c overview):
//!
//! * the DB-backed repo/embedding-job stores and the LRU index cache
//!   (`repo_embedding_index_cache.go`);
//! * the real object-storage download and the `sg-embeddings` provider
//!   construction from site config; the binary exposes an injected
//!   [`EmbeddingBackend`] seam instead;
//! * the actor/tenant/feature-flag/tracing HTTP middleware stack.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub mod backend;
pub mod index;

/// `shared.Config`'s `defaultEmbeddingsCacheSize`.
pub const DEFAULT_EMBEDDINGS_CACHE_SIZE: &str = "6GiB";

/// `embeddings.ObjectStorageConfig`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub backend: String,
    pub manage_bucket: bool,
    pub bucket: String,
    pub s3_region: String,
    pub s3_endpoint: String,
    pub s3_use_path_style: bool,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub s3_session_token: Option<String>,
    pub gcs_project_id: String,
    pub gcs_credentials_file: Option<String>,
    pub gcs_credentials_file_contents: Option<String>,
    pub embeddings_cache_size: u64,
    /// Errors accumulated by `Load` (`BaseConfig` + object-storage validation).
    pub errors: Vec<String>,
}

/// `env.GetBool` (`strconv.ParseBool`) accepted spellings.
pub fn parse_strconv_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `go-humanize.ParseBytes`: an optional number followed by an SI or IEC unit
/// suffix. The error text is the baseline's `humanize` text (`strconv.ParseFloat:
/// parsing "...": invalid syntax` for a malformed number).
pub fn parse_bytes(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("strconv.ParseFloat: parsing \"\": invalid syntax".to_owned());
    }

    let number_end = trimmed
        .char_indices()
        .find(|(_, ch)| !(ch.is_ascii_digit() || *ch == '.' || *ch == '-' || *ch == '+'))
        .map(|(index, _)| index)
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(number_end);
    let suffix = suffix.trim().to_ascii_lowercase();
    let suffix = suffix.strip_suffix('b').unwrap_or(&suffix).to_owned();

    let value: f64 = number
        .parse()
        .map_err(|_| format!("strconv.ParseFloat: parsing {number:?}: invalid syntax"))?;

    let multiplier: f64 = match suffix.as_str() {
        "" => 1.0,
        "k" => 1_000.0,
        "m" => 1_000_000.0,
        "g" => 1_000_000_000.0,
        "t" => 1_000_000_000_000.0,
        "p" => 1_000_000_000_000_000.0,
        "e" => 1_000_000_000_000_000_000.0,
        "ki" => 1024.0,
        "mi" => 1024.0 * 1024.0,
        "gi" => 1024.0 * 1024.0 * 1024.0,
        "ti" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "pi" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "ei" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => {
            return Err(format!(
                "unknown unit {other:?} in {raw:?}"
            ))
        }
    };

    Ok((value * multiplier) as u64)
}

/// `deploy.BlobstoreDefaultEndpoint`: the single-container deploy type is
/// `deploy.SingleDocker = "docker-container"` (the `DEPLOY_TYPE` value), not
/// "single-container".
fn blobstore_default_endpoint(deploy_type: &str) -> &'static str {
    if deploy_type == "docker-container" {
        "http://127.0.0.1:9000"
    } else {
        "http://blobstore:9000"
    }
}

fn get_required(
    get: &mut dyn FnMut(&str, &str) -> String,
    errors: &mut Vec<String>,
    name: &str,
    default: &str,
) -> String {
    let value = get(name, default);
    if value.is_empty() {
        errors.push(format!(
            "invalid value \"\" for {name}: no value supplied"
        ));
        return String::new();
    }
    value
}

fn get_bool(
    get: &mut dyn FnMut(&str, &str) -> String,
    errors: &mut Vec<String>,
    name: &str,
    default: &str,
) -> bool {
    let raw = get(name, default);
    match parse_strconv_bool(&raw) {
        Some(value) => value,
        None => {
            errors.push(format!(
                "invalid bool {raw:?} for {name}: strconv.ParseBool: parsing {raw:?}: invalid syntax"
            ));
            false
        }
    }
}

impl Config {
    /// `(*Config).Load`: returns `Err` with the `MustGetBytes` panic text when
    /// `EMBEDDINGS_CACHE_SIZE` is not a byte size (the baseline panics).
    pub fn load_with(get: &mut dyn FnMut(&str, &str) -> String) -> Result<Config, String> {
        let mut errors = Vec::new();
        let mut config = Config::default();

        config.backend = get_required(
            get,
            &mut errors,
            "EMBEDDINGS_UPLOAD_BACKEND",
            "blobstore",
        )
        .to_ascii_lowercase();
        config.manage_bucket = get_bool(
            get,
            &mut errors,
            "EMBEDDINGS_UPLOAD_MANAGE_BUCKET",
            "false",
        );
        config.bucket = get_required(get, &mut errors, "EMBEDDINGS_UPLOAD_BUCKET", "embeddings");

        if config.backend != "blobstore" && config.backend != "s3" && config.backend != "gcs" {
            errors.push(format!(
                "invalid backend {:?} for EMBEDDINGS_UPLOAD_BACKEND: must be S3, GCS, or Blobstore",
                config.backend
            ));
        }

        if config.backend == "blobstore" || config.backend == "s3" {
            config.s3_region = get_required(
                get,
                &mut errors,
                "EMBEDDINGS_UPLOAD_AWS_REGION",
                "us-east-1",
            );
            let deploy_type = get("DEPLOY_TYPE", "");
            config.s3_endpoint = get_required(
                get,
                &mut errors,
                "EMBEDDINGS_UPLOAD_AWS_ENDPOINT",
                blobstore_default_endpoint(&deploy_type),
            );
            config.s3_use_path_style = get_bool(
                get,
                &mut errors,
                "EMBEDDINGS_UPLOAD_AWS_USE_PATH_STYLE",
                "false",
            );
            let ec2_role_credentials = get_bool(
                get,
                &mut errors,
                "EMBEDDINGS_UPLOAD_AWS_USE_EC2_ROLE_CREDENTIALS",
                "false",
            );
            if !ec2_role_credentials {
                config.s3_access_key_id = get_required(
                    get,
                    &mut errors,
                    "EMBEDDINGS_UPLOAD_AWS_ACCESS_KEY_ID",
                    "AKIAIOSFODNN7EXAMPLE",
                );
                config.s3_secret_access_key = get_required(
                    get,
                    &mut errors,
                    "EMBEDDINGS_UPLOAD_AWS_SECRET_ACCESS_KEY",
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                );
                config.s3_session_token =
                    Some(get("EMBEDDINGS_UPLOAD_AWS_SESSION_TOKEN", "")).filter(|v| !v.is_empty());
            }
        } else if config.backend == "gcs" {
            config.gcs_project_id = get_required(
                get,
                &mut errors,
                "EMBEDDINGS_UPLOAD_GCP_PROJECT_ID",
                "",
            );
            config.gcs_credentials_file =
                Some(get("EMBEDDINGS_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE", ""))
                    .filter(|v| !v.is_empty());
            config.gcs_credentials_file_contents = Some(get(
                "EMBEDDINGS_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE_CONTENT",
                "",
            ))
            .filter(|v| !v.is_empty());
        }

        let raw_cache_size = get("EMBEDDINGS_CACHE_SIZE", DEFAULT_EMBEDDINGS_CACHE_SIZE);
        config.embeddings_cache_size = parse_bytes(&raw_cache_size).map_err(|_| {
            format!(
                "parsing environment variable \"EMBEDDINGS_CACHE_SIZE\". Expected valid time.Duration, got {raw_cache_size:?}"
            )
        })?;
        config.errors = errors;
        Ok(config)
    }

    /// Load from a map (the differential harness supplies the environment).
    pub fn load_from_map(values: &BTreeMap<String, String>) -> Result<Config, String> {
        let mut get = |name: &str, default: &str| -> String {
            values.get(name).cloned().unwrap_or_else(|| default.to_owned())
        };
        Self::load_with(&mut get)
    }

    /// `Config.Validate`: no errors is `None`; one error is its text; several
    /// are the baseline `MultiError` rendering
    /// (`N errors occurred:\n\t* e1\n\t* e2`).
    pub fn validate_error(&self) -> Option<String> {
        match self.errors.len() {
            0 => None,
            1 => Some(self.errors[0].clone()),
            n => Some(format!("{n} errors occurred:\n\t* {}", self.errors.join("\n\t* "))),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `embeddings.EmbeddingsSearchParameters`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct EmbeddingsSearchParameters {
    #[serde(default, rename = "repoNames")]
    pub repo_names: Vec<String>,
    #[serde(default, rename = "repoIDs")]
    pub repo_ids: Vec<i32>,
    #[serde(default)]
    pub query: String,
    #[serde(default, rename = "codeResultsCount")]
    pub code_results_count: i64,
    #[serde(default, rename = "textResultsCount")]
    pub text_results_count: i64,
    #[serde(default, rename = "useDocumentRanks")]
    pub use_document_ranks: bool,
}

/// `embeddings.SearchScoreDetails`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchScoreDetails {
    pub score: i32,
    #[serde(rename = "similarityScore")]
    pub similarity_score: i32,
    #[serde(rename = "rankScore")]
    pub rank_score: i32,
}

/// `embeddings.EmbeddingSearchResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingSearchResult {
    #[serde(rename = "repoName")]
    pub repo_name: String,
    pub revision: String,
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "startLine")]
    pub start_line: i64,
    #[serde(rename = "endLine")]
    pub end_line: i64,
    #[serde(rename = "scoreDetails")]
    pub score_details: SearchScoreDetails,
}

impl EmbeddingSearchResult {
    /// `EmbeddingSearchResult.Score`.
    pub fn score(&self) -> i32 {
        self.score_details.rank_score + self.score_details.similarity_score
    }
}

/// The subset of `embeddings.RepoEmbeddingIndex` the handler reads. The
/// similarity search itself is the injected backend's job.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoEmbeddingIndex {
    pub embeddings_model: String,
    pub repo_name: String,
    pub revision: String,
    pub code_results: Vec<EmbeddingSearchResult>,
    pub text_results: Vec<EmbeddingSearchResult>,
}

impl RepoEmbeddingIndex {
    /// `RepoEmbeddingIndex.IsModelCompatible`.
    pub fn is_model_compatible(&self, model: &str) -> bool {
        self.embeddings_model.is_empty() || self.embeddings_model == model
    }
}

/// A handler failure carrying the two error classes the HTTP handler treats
/// specially (`errcode.IsNotFound` and `client.RateLimitExceededError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingError {
    pub message: String,
    pub not_found: bool,
    pub rate_limited: bool,
}

impl EmbeddingError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
            rate_limited: false,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: true,
            rate_limited: false,
        }
    }

    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
            rate_limited: true,
        }
    }
}

impl std::fmt::Display for EmbeddingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for EmbeddingError {}

/// `getQueryEmbedding` + the index download and similarity search behind
/// `getRepoEmbeddingIndex`.
#[async_trait::async_trait]
pub trait EmbeddingBackend: Send + Sync {
    async fn get_query_embedding(
        &self,
        query: &str,
    ) -> Result<(Vec<f32>, String), EmbeddingError>;

    async fn get_repo_embedding_index(
        &self,
        repo_id: i32,
        repo_name: &str,
        query_embedding: &[f32],
        code_results_count: i64,
        text_results_count: i64,
        use_document_ranks: bool,
    ) -> Result<RepoEmbeddingIndex, EmbeddingError>;
}

/// One HTTP response, materialised for the differential harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

impl HandlerResponse {
    fn http_error(status: u16, message: &str) -> Self {
        // `http.Error` writes `text/plain; charset=utf-8`, adds a newline and
        // sets `X-Content-Type-Options: nosniff`.
        Self {
            status,
            content_type: "text/plain; charset=utf-8".to_owned(),
            body: format!("{message}\n").into_bytes(),
        }
    }
}

/// `shared.NewHandler`.
pub struct Handler {
    backend: Arc<dyn EmbeddingBackend>,
}

impl Handler {
    pub fn new(backend: Arc<dyn EmbeddingBackend>) -> Self {
        Self { backend }
    }

    /// Serve one request against `/search`. Any other path is a 404 from the
    /// mux, represented by `path != "/search"`.
    pub async fn handle(&self, path: &str, method: &str, body: &[u8]) -> HandlerResponse {
        if path != "/search" {
            return HandlerResponse::http_error(404, "404 page not found");
        }
        if method != "POST" {
            return HandlerResponse::http_error(400, &format!("unsupported method {method}"));
        }

        let params: EmbeddingsSearchParameters = match parse_search_parameters(body) {
            Ok(params) => params,
            Err(err) => {
                return HandlerResponse::http_error(
                    400,
                    &format!("could not parse request body: {err}"),
                )
            }
        };

        match self.search(&params).await {
            Ok(results) => HandlerResponse {
                status: 200,
                content_type: "application/json".to_owned(),
                body: encode_results(&results),
            },
            Err(err) if err.not_found => HandlerResponse::http_error(400, &err.message),
            Err(err) => {
                // `NewHandler` logs every non-not-found search error before
                // choosing the status code.
                tracing::error!(error = %err.message, "error searching embedding index");
                let status = if err.rate_limited { 429 } else { 500 };
                HandlerResponse::http_error(
                    status,
                    &format!("error searching embedding index: {}", err.message),
                )
            }
        }
    }

    async fn search(
        &self,
        params: &EmbeddingsSearchParameters,
    ) -> Result<EmbeddingCombinedSearchResults, EmbeddingError> {
        // `searchRepoEmbeddingIndexes`: the query embedding comes first.
        let (float_query, query_model) = self.backend.get_query_embedding(&params.query).await?;

        let mut result = EmbeddingCombinedSearchResults::default();
        for (index, repo_name) in params.repo_names.iter().enumerate() {
            let repo_id = params.repo_ids.get(index).copied().unwrap_or_default();
            let embedding_index = self
                .backend
                .get_repo_embedding_index(
                    repo_id,
                    repo_name,
                    &float_query,
                    params.code_results_count,
                    params.text_results_count,
                    params.use_document_ranks,
                )
                .await
                .map_err(|err| EmbeddingError {
                    // `errors.Wrapf` keeps the not-found/rate-limit cause, so
                    // the handler's status mapping still sees it.
                    message: format!(
                        "getting repo embedding index for repo {repo_name:?}: {}",
                        err.message
                    ),
                    not_found: err.not_found,
                    rate_limited: err.rate_limited,
                })?;

            if !embedding_index.is_model_compatible(&query_model) {
                return Err(EmbeddingError::other(format!(
                    "embeddings model in config ({query_model}) does not match the embeddings model for the index ({}). Embedding index for repo {repo_name:?} must be reindexed with the new model",
                    embedding_index.embeddings_model
                )));
            }

            result.merge_truncate(
                embedding_index.code_results,
                params.code_results_count,
                true,
            );
            result.merge_truncate(
                embedding_index.text_results,
                params.text_results_count,
                false,
            );
        }
        Ok(result)
    }
}

/// Parse the JSON body the way `json.NewDecoder(r.Body).Decode` reports the two
/// deterministic failures: an empty body is `EOF` and a truncated object is
/// `unexpected EOF`.
fn parse_search_parameters(body: &[u8]) -> Result<EmbeddingsSearchParameters, String> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Err("EOF".to_owned());
    }
    serde_json::from_slice(body).map_err(|err| {
        if err.is_eof() {
            "unexpected EOF".to_owned()
        } else {
            err.to_string()
        }
    })
}

/// `embeddings.EmbeddingCombinedSearchResults`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddingCombinedSearchResults {
    pub code_results: Vec<EmbeddingSearchResult>,
    pub text_results: Vec<EmbeddingSearchResult>,
}

impl EmbeddingCombinedSearchResults {
    /// `EmbeddingSearchResults.MergeTruncate`.
    pub fn merge_truncate(&mut self, other: Vec<EmbeddingSearchResult>, max: i64, code: bool) {
        let target = if code {
            &mut self.code_results
        } else {
            &mut self.text_results
        };
        target.extend(other);
        target.sort_by(|left, right| right.score().cmp(&left.score()));
        if max >= 0 && target.len() > max as usize {
            target.truncate(max as usize);
        }
    }
}

fn encode_results(results: &EmbeddingCombinedSearchResults) -> Vec<u8> {
    // `json.NewEncoder(w).Encode`: compact JSON plus a trailing newline. Nil
    // slices encode as `null` like the baseline.
    let value = serde_json::json!({
        "codeResults": if results.code_results.is_empty() { serde_json::Value::Null } else { serde_json::to_value(&results.code_results).expect("results") },
        "textResults": if results.text_results.is_empty() { serde_json::Value::Null } else { serde_json::to_value(&results.text_results).expect("results") },
    });
    let mut body = serde_json::to_vec(&value).expect("encode results");
    body.push(b'\n');
    body
}
