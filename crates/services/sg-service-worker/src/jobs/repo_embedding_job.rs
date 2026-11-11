#![forbid(unsafe_code)]

//! `cmd/worker/internal/embeddings/repo/{worker.go,handler.go}` and
//! `internal/embeddings/{embed,index_storage,index_name,quantize,types}`: the
//! `repo-embedding-job` worker.
//!
//! The job dequeues `repo_embedding_jobs` rows, resolves the repository and
//! its revision, optionally downloads a previous index for an incremental
//! update, splits files into embeddable chunks (`384`/`256`/`224` token
//! thresholds), batches embedding requests (`SRC_EMBEDDINGS_BATCH_SIZE`,
//! default `512`), writes the code/text indexes to object storage and records
//! per-flush progress in `repo_embedding_job_stats`.
//!
//! Every external dependency is a seam so the differential harness can drive
//! the exact same code without network or database access:
//!
//! * [`EmbeddingsClient`] — `embed/client.EmbeddingsClient`;
//! * [`ContextService`] — `embed.ContextService` (`SplitIntoEmbeddableChunks`);
//! * [`FileReadLister`] — `embed.FileReadLister` (`Read`/`List`/`Diff`);
//! * [`GitServer`] — the gitserver operations the fetcher and revision check
//!   use (`GetDefaultBranch`, `ReadFile`, `ReadDir`, `ChangedFiles`);
//! * [`RankingService`] — `ranking.Service.GetDocumentRanks`;
//! * [`RepoEmbeddingJobStore`] — `Repos().Get` and
//!   `UpdateRepoEmbeddingJobStats`;
//! * [`ObjectStorageBackend`] — reused from
//!   [`crate::jobs::exhaustive_search_service`] (`Get`/`Upload`/`Delete`).
//!
//! The production [`RepoEmbeddingJob`] assembles the worker with the
//! `EMBEDDINGS_UPLOAD_*` object storage, the gitserver client, the PG job
//! store and the PG ranking service. [`LiveEmbeddingsClientFactory`] selects
//! the sourcegraph / OpenAI / Azure OpenAI client from the live site
//! configuration (see [`embeddings_provider_config`] for the baseline
//! defaults) and injects the client through [`SgEmbeddingsClientAdapter`].
//! [`NotPortedEmbeddingsClientFactory`] is retained only as a test double.
//!
//! ## Deviations
//!
//! * **Index encoding**: the baseline streams a gob-encoded, chunked index.
//!   Rust writes `SGEMB1\n` followed by JSON. Existing gob indexes cannot be
//!   read back, so the first post-switch job performs a full reindex (the
//!   download error is handled exactly like "no previous index").
//! * **Binary detection**: `binary.IsBinary` falls back to
//!   `http.DetectContentType`'s `text/` sniffing. Only the WHATWG
//!   `text/plain` control-character check is ported.
//! * **Logging/metrics/tracing**: not ported (consistent with the other
//!   worker jobs).
//! * **Feature flags**: `featureflag.WithFlags` is not injected; the embed
//!   path does not read feature flags.
//!
//! See `docs/overviews/m9_8io_repo_embedding_job_overview.md`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sg_config::{LiveConfig, SiteConfiguration};
use sg_db::OffsetDateTime;
use sg_licensing::license_key_based_access_token;
use sg_objectstorage::{GcsConfig, S3Config, StorageConfig};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

use crate::framework::dbworker::{
    DbBackedRoutine, DbRecord, DbWorkerHandler, DbWorkerStore, WorkerOptions, WorkerRoutine,
};
use crate::framework::BackgroundRoutine;
use crate::jobs::batches::parse_strconv_bool;
use crate::jobs::exhaustive_search_service::{new_object_storage, ObjectStorageBackend};
use crate::jobs::repo_embedding_scheduler::{embeddings_config, force_allow_embeddings};
use crate::jobs::{Job, JobConfig, JobDeps};
use crate::paths::{compile, GlobPattern};

// ---------------------------------------------------------------------------
// Constants (handler.go:43-56, embed/files.go:12-15)
// ---------------------------------------------------------------------------

/// `embedEntireFileTokensThreshold`.
pub const EMBED_ENTIRE_FILE_TOKENS_THRESHOLD: usize = 384;
/// `embeddingChunkTokensThreshold`.
pub const EMBEDDING_CHUNK_TOKENS_THRESHOLD: usize = 256;
/// `embeddingChunkEarlySplitTokensThreshold` (`chunk - 32`).
pub const EMBEDDING_CHUNK_EARLY_SPLIT_TOKENS_THRESHOLD: usize =
    EMBEDDING_CHUNK_TOKENS_THRESHOLD - 32;
/// `embeddingsTolerableFailureRatio`.
pub const EMBEDDINGS_TOLERABLE_FAILURE_RATIO: f64 = 0.10;
/// `env.MustGetInt("SRC_EMBEDDINGS_BATCH_SIZE", 512, ...)`.
pub const DEFAULT_EMBEDDINGS_BATCH_SIZE: usize = 512;
/// The environment variable backing [`DEFAULT_EMBEDDINGS_BATCH_SIZE`].
pub const EMBEDDINGS_BATCH_SIZE_ENV: &str = "SRC_EMBEDDINGS_BATCH_SIZE";
/// `MIN_EMBEDDABLE_FILE_SIZE`.
pub const MIN_EMBEDDABLE_FILE_SIZE: usize = 32;
/// `MAX_LINE_LENGTH`.
pub const MAX_LINE_LENGTH: usize = 2048;
/// `CHARS_PER_TOKEN`.
pub const CHARS_PER_TOKEN: usize = 4;
/// `defaultMaxCodeEmbeddingsPerRepo`.
pub const DEFAULT_MAX_CODE_EMBEDDINGS_PER_REPO: i64 = 3_072_000;
/// `defaultMaxTextEmbeddingsPerRepo`.
pub const DEFAULT_MAX_TEXT_EMBEDDINGS_PER_REPO: i64 = 512_000;
/// `embeddingsMaxFileSizeBytes`.
pub const EMBEDDINGS_MAX_FILE_SIZE_BYTES: i64 = 1_000_000;
/// `repo_embedding_job_worker`.
pub const WORKER_NAME: &str = "repo_embedding_job_worker";
/// `WorkerOptions.Interval`.
pub const WORKER_INTERVAL: Duration = Duration::from_secs(10);
/// `WorkerOptions.HeartbeatInterval`.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// `splitOptions`.
pub fn split_options() -> SplitOptions {
    SplitOptions {
        no_split_tokens_threshold: EMBED_ENTIRE_FILE_TOKENS_THRESHOLD,
        chunk_tokens_threshold: EMBEDDING_CHUNK_TOKENS_THRESHOLD,
        chunk_early_split_tokens_threshold: EMBEDDING_CHUNK_EARLY_SPLIT_TOKENS_THRESHOLD,
    }
}

// ---------------------------------------------------------------------------
// Context splitting (`internal/codeintel/context/split.go`)
// ---------------------------------------------------------------------------

/// `codeintelContext.SplitOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitOptions {
    pub no_split_tokens_threshold: usize,
    pub chunk_tokens_threshold: usize,
    pub chunk_early_split_tokens_threshold: usize,
}

/// `codeintelContext.EmbeddableChunk`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddableChunk {
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "startLine")]
    pub start_line: i64,
    #[serde(rename = "endLine")]
    pub end_line: i64,
    pub content: String,
}

/// `splittableLinePrefixes`.
pub const SPLITTABLE_LINE_PREFIXES: [&str; 10] = [
    "//", "#", "/*", "func", "var", "const", "fn", "public", "private", "type",
];

/// `isSplittableLine`.
pub fn is_splittable_line(line: &str) -> bool {
    if line.trim().is_empty() {
        return true;
    }
    SPLITTABLE_LINE_PREFIXES
        .iter()
        .any(|prefix| line.starts_with(prefix))
}

/// `EstimateTokens`: `ceil(byte length / 4)`.
pub fn estimate_tokens(text: &str) -> usize {
    text.len().div_ceil(CHARS_PER_TOKEN)
}

/// `SplitIntoEmbeddableChunks`.
pub fn split_into_embeddable_chunks(
    text: &str,
    file_name: &str,
    options: SplitOptions,
) -> Vec<EmbeddableChunk> {
    if estimate_tokens(text) < options.no_split_tokens_threshold {
        return vec![EmbeddableChunk {
            file_name: file_name.to_owned(),
            start_line: 0,
            end_line: text.matches('\n').count() as i64 + 1,
            content: text.to_owned(),
        }];
    }

    let mut chunks = Vec::new();
    let mut start_line = 0usize;
    let mut tokens_sum = 0usize;
    let lines: Vec<&str> = text.split('\n').collect();

    for (i, line) in lines.iter().enumerate() {
        if tokens_sum > options.chunk_tokens_threshold
            || (tokens_sum > options.chunk_early_split_tokens_threshold && is_splittable_line(line))
        {
            add_chunk(&mut chunks, &lines, &mut start_line, i, file_name);
            tokens_sum = 0;
        }
        tokens_sum += estimate_tokens(line);
    }

    if tokens_sum > 0 {
        add_chunk(&mut chunks, &lines, &mut start_line, lines.len(), file_name);
    }

    chunks
}

fn add_chunk(
    chunks: &mut Vec<EmbeddableChunk>,
    lines: &[&str],
    start_line: &mut usize,
    end_line: usize,
    file_name: &str,
) {
    let content = lines[*start_line..end_line].join("\n");
    if !content.is_empty() {
        chunks.push(EmbeddableChunk {
            file_name: file_name.to_owned(),
            start_line: *start_line as i64,
            end_line: end_line as i64,
            content,
        });
    }
    *start_line = end_line;
}

/// `ContextService`.
#[async_trait]
pub trait ContextService: Send + Sync {
    async fn split_into_embeddable_chunks(
        &self,
        text: &str,
        file_name: &str,
        options: SplitOptions,
    ) -> Result<Vec<EmbeddableChunk>, String>;
}

/// `context.Service`: delegates to the pure splitter.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultContextService;

#[async_trait]
impl ContextService for DefaultContextService {
    async fn split_into_embeddable_chunks(
        &self,
        text: &str,
        file_name: &str,
        options: SplitOptions,
    ) -> Result<Vec<EmbeddableChunk>, String> {
        Ok(split_into_embeddable_chunks(text, file_name, options))
    }
}

// ---------------------------------------------------------------------------
// File classification and filters (`internal/embeddings/embed/files.go`)
// ---------------------------------------------------------------------------

/// Skip reasons (`SkipReason*`).
pub const SKIP_REASON_BINARY: &str = "binary";
pub const SKIP_REASON_SMALL: &str = "small";
pub const SKIP_REASON_LARGE: &str = "large";
pub const SKIP_REASON_AUTOGENERATED: &str = "autogenerated";
pub const SKIP_REASON_LONG_LINE: &str = "longLine";
pub const SKIP_REASON_EXCLUDED: &str = "excluded";
pub const SKIP_REASON_NOT_INCLUDED: &str = "notIncluded";
pub const SKIP_REASON_MAX_EMBEDDINGS: &str = "maxEmbeddings";

/// `autogeneratedFileHeaders`.
const AUTOGENERATED_FILE_HEADERS: [&[u8]; 4] = [
    b"autogenerated file",
    b"lockfile",
    b"generated by",
    b"do not edit",
];

/// `TextFileExtensions`.
pub const TEXT_FILE_EXTENSIONS: [&str; 18] = [
    "adoc",
    "asciidoc",
    "creole",
    "md",
    "markdown",
    "mediawiki",
    "muse",
    "org",
    "pod",
    "pod6",
    "rest",
    "rdoc",
    "rst",
    "rmd",
    "textile",
    "txt",
    "wiki",
    "wikitext",
];

/// `DefaultExcludedFilePathPatterns`.
pub const DEFAULT_EXCLUDED_FILE_PATH_PATTERNS: [&str; 11] = [
    ".*ignore",
    ".gitattributes",
    ".mailmap",
    "*.csv",
    "*.svg",
    "*.xml",
    "__fixtures__/",
    "node_modules/",
    "testdata/",
    "mocks/",
    "vendor/",
];

/// `CompileGlobPatterns`: invalid patterns are skipped.
pub fn compile_glob_patterns(patterns: &[String]) -> Vec<GlobPattern> {
    patterns
        .iter()
        .filter_map(|pattern| compile(pattern).ok())
        .collect()
}

/// `GetDefaultExcludedFilePathPatterns`.
pub fn default_excluded_file_path_patterns() -> Vec<GlobPattern> {
    let patterns: Vec<String> = DEFAULT_EXCLUDED_FILE_PATH_PATTERNS
        .iter()
        .map(|pattern| (*pattern).to_owned())
        .collect();
    compile_glob_patterns(&patterns)
}

pub fn is_excluded_file_path_match(file_path: &str, patterns: &[GlobPattern]) -> bool {
    patterns.iter().any(|pattern| pattern.match_path(file_path))
}

pub fn is_included_file_path_match(file_path: &str, patterns: &[GlobPattern]) -> bool {
    patterns.is_empty() || patterns.iter().any(|pattern| pattern.match_path(file_path))
}

/// `filepath.Ext` (the suffix beginning at the final dot in the final path
/// element, including the dot).
fn go_filepath_ext(path: &str) -> &str {
    let base = path.rsplit('/').next().unwrap_or(path);
    match base.rfind('.') {
        Some(index) => &base[index..],
        None => "",
    }
}

/// `filepath.Base` for slash-separated paths.
fn go_filepath_base(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/";
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed)
}

/// `IsValidTextFile`.
pub fn is_valid_text_file(file_name: &str) -> bool {
    let ext = go_filepath_ext(file_name);
    let ext = ext.strip_prefix('.').unwrap_or(ext).to_lowercase();
    if TEXT_FILE_EXTENSIONS.contains(&ext.as_str()) {
        return true;
    }
    go_filepath_base(file_name)
        .to_lowercase()
        .starts_with("license")
}

/// `binary.IsBinary`'s UTF-8 fast path plus `http.DetectContentType`'s
/// `text/plain` sniff (Go's `textSig` signature over the first 512 bytes).
pub fn is_binary(content: &[u8]) -> bool {
    if std::str::from_utf8(content).is_ok() {
        return false;
    }
    let prefix = &content[..content.len().min(512)];
    // `textSig`: text unless a C0 control byte other than \t \n \f \r or
    // ESC (0x1B) is present. Go's check does not treat DEL as binary.
    let text = prefix
        .iter()
        .all(|&byte| !matches!(byte, 0x00..=0x08 | 0x0B | 0x0E..=0x1A | 0x1C..=0x1F));
    !text
}

fn trim_space_len(content: &[u8]) -> usize {
    let start = content
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(content.len());
    let end = content
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)
        .unwrap_or(start);
    end.saturating_sub(start)
}

/// `isEmbeddableFileContent`.
pub fn is_embeddable_file_content(content: &[u8]) -> (bool, &'static str) {
    if is_binary(content) {
        return (false, SKIP_REASON_BINARY);
    }
    if trim_space_len(content) < MIN_EMBEDDABLE_FILE_SIZE {
        return (false, SKIP_REASON_SMALL);
    }

    let lines: Vec<&[u8]> = content.split(|&byte| byte == b'\n').collect();
    let header_lines = &lines[..lines.len().min(5)];
    let file_header: Vec<u8> = header_lines
        .iter()
        .enumerate()
        .flat_map(|(index, line)| {
            let mut bytes = Vec::with_capacity(line.len() + 1);
            if index > 0 {
                bytes.push(b'\n');
            }
            bytes.extend_from_slice(line);
            bytes
        })
        .map(|byte| byte.to_ascii_lowercase())
        .collect();
    for header in AUTOGENERATED_FILE_HEADERS {
        if file_header
            .windows(header.len())
            .any(|window| window == header)
        {
            return (false, SKIP_REASON_AUTOGENERATED);
        }
    }

    if lines.iter().any(|line| line.len() > MAX_LINE_LENGTH) {
        return (false, SKIP_REASON_LONG_LINE);
    }

    (true, "")
}

// ---------------------------------------------------------------------------
// Embedding client and index types
// ---------------------------------------------------------------------------

/// `client.EmbeddingsResults`.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingsResults {
    pub embeddings: Vec<f32>,
    pub failed: Vec<i64>,
    pub dimensions: usize,
}

impl EmbeddingsResults {
    fn row(&self, n: usize) -> &[f32] {
        &self.embeddings[n * self.dimensions..(n + 1) * self.dimensions]
    }
}

/// `client.RateLimitExceededError` vs any other failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingsClientError {
    RateLimitExceeded,
    Other(String),
}

impl std::fmt::Display for EmbeddingsClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimitExceeded => f.write_str("rate limit exceeded"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

/// `client.EmbeddingsClient` (the query-embedding method is unused here).
#[async_trait]
pub trait EmbeddingsClient: Send + Sync {
    async fn get_document_embeddings(
        &self,
        documents: Vec<String>,
    ) -> Result<EmbeddingsResults, EmbeddingsClientError>;

    fn get_dimensions(&self) -> Result<usize, String>;

    fn get_model_identifier(&self) -> String;
}

/// `embed.NewEmbeddingsClient`, injectable for the differential harness.
pub trait EmbeddingsClientFactory: Send + Sync {
    fn new_client(&self, config: &EmbeddingsJobConfig)
        -> Result<Arc<dyn EmbeddingsClient>, String>;
}

/// Explicit failure for providers the slice deliberately does not support
/// (retained as a test double; production uses
/// [`LiveEmbeddingsClientFactory`]).
#[derive(Debug, Default, Clone, Copy)]
pub struct NotPortedEmbeddingsClientFactory;

impl EmbeddingsClientFactory for NotPortedEmbeddingsClientFactory {
    fn new_client(
        &self,
        config: &EmbeddingsJobConfig,
    ) -> Result<Arc<dyn EmbeddingsClient>, String> {
        Err(format!(
            "not ported: embeddings client for provider {:?}",
            config.provider
        ))
    }
}

/// Adapts an `sg-embeddings` client to the io trait.
pub struct SgEmbeddingsClientAdapter {
    inner: Arc<dyn sg_embeddings::EmbeddingsClient>,
}

impl SgEmbeddingsClientAdapter {
    pub fn new(inner: Arc<dyn sg_embeddings::EmbeddingsClient>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl EmbeddingsClient for SgEmbeddingsClientAdapter {
    async fn get_document_embeddings(
        &self,
        documents: Vec<String>,
    ) -> Result<EmbeddingsResults, EmbeddingsClientError> {
        self.inner
            .get_document_embeddings(documents)
            .await
            .map(|results| EmbeddingsResults {
                embeddings: results.embeddings,
                failed: results.failed,
                dimensions: results.dimensions,
            })
            .map_err(|err| match err {
                sg_embeddings::EmbeddingsError::RateLimitExceeded(_) => {
                    EmbeddingsClientError::RateLimitExceeded
                }
                other => EmbeddingsClientError::Other(other.to_string()),
            })
    }

    fn get_dimensions(&self) -> Result<usize, String> {
        self.inner.get_dimensions()
    }

    fn get_model_identifier(&self) -> String {
        self.inner.get_model_identifier()
    }
}

/// `conf.GetEmbeddingsConfig`'s provider fields (endpoint/model/dimensions/
/// access token), with the baseline defaults. `None` mirrors the nil config
/// (the gate itself is shared with the scheduler's port).
pub fn embeddings_provider_config(
    site: &SiteConfiguration,
    force_allow: bool,
) -> Option<sg_embeddings::ProviderConfig> {
    // Exact nil semantics (cody, internal flag, legacy enabled, credentials).
    crate::jobs::repo_embedding_scheduler::embeddings_config(site, force_allow)?;

    let raw = site.raw();
    let embeddings = raw.get("embeddings").filter(|value| !value.is_null());
    let string = |key: &str| {
        embeddings
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };

    let provider_name = {
        let provider = string("provider");
        if provider.is_empty() {
            "sourcegraph".to_owned()
        } else {
            provider
        }
    };
    let provider = sg_embeddings::Provider::try_from_name(&provider_name).ok()?;

    // `If endpoint is not set, fall back to URL, it's the previous name`.
    let mut endpoint = {
        let endpoint = string("endpoint");
        if endpoint.is_empty() {
            string("url")
        } else {
            endpoint
        }
    };
    let mut model = string("model");
    let mut dimensions = embeddings
        .and_then(|value| value.get("dimensions"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    let access_token = match provider {
        sg_embeddings::Provider::Sourcegraph => {
            let token = string("accessToken");
            if token.is_empty() {
                // `getSourcegraphProviderAccessToken`: the license-derived
                // token when none is configured.
                let license_key = site.license_key();
                if license_key.is_empty() {
                    return None;
                }
                license_key_based_access_token(&license_key)
            } else {
                token
            }
        }
        sg_embeddings::Provider::OpenAI | sg_embeddings::Provider::AzureOpenAI => {
            string("accessToken")
        }
    };

    match provider {
        sg_embeddings::Provider::Sourcegraph => {
            if endpoint.is_empty() {
                endpoint = "https://cody-gateway.sourcegraph.com/v1/embeddings".to_owned();
            }
            if model.is_empty() {
                model = "openai/text-embedding-ada-002".to_owned();
            }
            model = model.to_lowercase();
            if dimensions <= 0 && model == "openai/text-embedding-ada-002" {
                dimensions = 1536;
            }
        }
        sg_embeddings::Provider::OpenAI => {
            if endpoint.is_empty() {
                endpoint = "https://api.openai.com/v1/embeddings".to_owned();
            }
            if model.is_empty() {
                model = "text-embedding-ada-002".to_owned();
            }
            model = model.to_lowercase();
            if dimensions <= 0 && model == "text-embedding-ada-002" {
                dimensions = 1536;
            }
        }
        sg_embeddings::Provider::AzureOpenAI => {
            model = model.to_lowercase();
        }
    }
    Some(sg_embeddings::ProviderConfig {
        provider,
        access_token,
        model,
        endpoint,
        dimensions,
    })
}

/// The production factory: recomputes the provider config from the live site
/// configuration and builds the matching `sg-embeddings` client.
pub struct LiveEmbeddingsClientFactory {
    pub live_config: Arc<LiveConfig>,
    pub transport: Arc<dyn sg_embeddings::HttpTransport>,
}

impl EmbeddingsClientFactory for LiveEmbeddingsClientFactory {
    fn new_client(
        &self,
        _config: &EmbeddingsJobConfig,
    ) -> Result<Arc<dyn EmbeddingsClient>, String> {
        let site = self.live_config.current().site.clone();
        let provider_config = embeddings_provider_config(&site, force_allow_embeddings())
            .ok_or_else(|| {
                "embeddings are not configured (conf.GetEmbeddingsConfig returned nil)".to_owned()
            })?;
        let inner = sg_embeddings::new_client(&provider_config, Arc::clone(&self.transport))?;
        Ok(Arc::new(SgEmbeddingsClientAdapter::new(inner)))
    }
}

/// `embeddings.RepoEmbeddingRowMetadata`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoEmbeddingRowMetadata {
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "startLine")]
    pub start_line: i64,
    #[serde(rename = "endLine")]
    pub end_line: i64,
}

/// `embeddings.EmbeddingIndex`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingIndex {
    pub embeddings: Vec<i8>,
    pub column_dimension: i64,
    pub row_metadata: Vec<RepoEmbeddingRowMetadata>,
    pub ranks: Vec<f32>,
}

impl EmbeddingIndex {
    fn filter(&mut self, to_remove: &HashSet<&str>, ranks: &RepoPathRanks) {
        let dimension = self.column_dimension.max(0) as usize;
        self.ranks = Vec::with_capacity(self.row_metadata.len());
        let mut cursor = 0usize;
        for i in 0..self.row_metadata.len() {
            let metadata = self.row_metadata[i].clone();
            if to_remove.contains(metadata.file_name.as_str()) {
                continue;
            }
            self.row_metadata[cursor] = metadata.clone();
            self.ranks
                .push(ranks.paths.get(&metadata.file_name).copied().unwrap_or(0.0) as f32);
            let source = i * dimension;
            let target = cursor * dimension;
            for k in 0..dimension {
                self.embeddings[target + k] = self.embeddings[source + k];
            }
            cursor += 1;
        }
        self.row_metadata.truncate(cursor);
        self.ranks.truncate(cursor);
        self.embeddings.truncate(cursor * dimension);
    }

    fn append(&mut self, other: &EmbeddingIndex) {
        self.row_metadata.extend(other.row_metadata.iter().cloned());
        self.ranks.extend_from_slice(&other.ranks);
        self.embeddings.extend_from_slice(&other.embeddings);
    }
}

/// `embeddings.RepoEmbeddingIndex`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RepoEmbeddingIndex {
    pub repo_name: String,
    pub revision: String,
    pub embeddings_model: String,
    pub code_index: EmbeddingIndex,
    pub text_index: EmbeddingIndex,
}

impl RepoEmbeddingIndex {
    /// `IsModelCompatible`.
    pub fn is_model_compatible(&self, model: &str) -> bool {
        self.embeddings_model.is_empty() || self.embeddings_model == model
    }
}

/// `embeddings.Quantize`.
pub fn quantize(input: &[f32]) -> Vec<i8> {
    input
        .iter()
        .map(|&value| {
            let value = value.clamp(-1.0, 1.0);
            (f64::from(value) * 127.0).round() as i8
        })
        .collect()
}

/// `GetRepoEmbeddingIndexName`.
pub fn repo_embedding_index_name(repo_id: i32) -> String {
    format!("{repo_id}.embeddingindex")
}

/// `GetRepoEmbeddingIndexNameDeprecated`.
pub fn repo_embedding_index_name_deprecated(repo_name: &str) -> String {
    let fs_safe: String = repo_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect();
    let hash = md5::compute(repo_name.as_bytes());
    format!("{fs_safe}_{hash:x}.embeddingindex")
}

/// The Rust index header (see the module deviations).
pub const INDEX_MAGIC: &[u8] = b"SGEMB1\n";

// ---------------------------------------------------------------------------
// Ranking and repo job store seams
// ---------------------------------------------------------------------------

/// `types.RepoPathRanks` (mirrors `sg-codeintel-ranking`).
pub use sg_codeintel_ranking::RepoPathRanks;

/// `ranking.Service.GetDocumentRanks`.
#[async_trait]
pub trait RankingService: Send + Sync {
    async fn get_document_ranks(&self, repo_name: &str) -> Result<RepoPathRanks, String>;
}

/// The production ranking service.
pub struct PgRankingService {
    pool: PgPool,
}

impl PgRankingService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RankingService for PgRankingService {
    async fn get_document_ranks(&self, repo_name: &str) -> Result<RepoPathRanks, String> {
        sg_codeintel_ranking::get_document_ranks(&self.pool, repo_name)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `types.Repo` reduced to the fields the handler reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRecord {
    pub id: i32,
    pub name: String,
}

/// `RepoEmbeddingJobsStore` operations used by the handler.
#[async_trait]
pub trait RepoEmbeddingJobStore: Send + Sync {
    /// `db.Repos().Get(id)` (including blocked repos, excluding deleted).
    async fn get_repo(&self, repo_id: i32) -> Result<RepoRecord, String>;

    /// `UpdateRepoEmbeddingJobStats`.
    async fn update_repo_embedding_job_stats(
        &self,
        job_id: i64,
        stats: &EmbedRepoStats,
    ) -> Result<(), String>;
}

/// The production PG store.
pub struct PgRepoEmbeddingJobStore {
    pool: PgPool,
}

impl PgRepoEmbeddingJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RepoEmbeddingJobStore for PgRepoEmbeddingJobStore {
    async fn get_repo(&self, repo_id: i32) -> Result<RepoRecord, String> {
        let row = sqlx::query(
            "SELECT id, name, blocked FROM repo WHERE id = $1 AND deleted_at IS NULL LIMIT 1",
        )
        .bind(repo_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| err.to_string())?
        .ok_or_else(|| format!("repo not found: id={repo_id}"))?;
        let blocked: Option<serde_json::Value> = row.try_get("blocked").ok();
        if blocked.is_some_and(|value| !value.is_null()) {
            return Err(format!("repo is blocked: id={repo_id}"));
        }
        Ok(RepoRecord {
            id: row.get("id"),
            name: row.get("name"),
        })
    }

    async fn update_repo_embedding_job_stats(
        &self,
        job_id: i64,
        stats: &EmbedRepoStats,
    ) -> Result<(), String> {
        let code_skipped = serde_json::to_string(&stats.code_index_stats.files_skipped)
            .map_err(|err| err.to_string())?;
        let text_skipped = serde_json::to_string(&stats.text_index_stats.files_skipped)
            .map_err(|err| err.to_string())?;
        sqlx::query(
            r#"
INSERT INTO repo_embedding_job_stats (
	job_id, is_incremental,
	code_files_total, code_files_embedded, code_chunks_embedded,
	code_chunks_excluded, code_files_skipped, code_bytes_embedded,
	text_files_total, text_files_embedded, text_chunks_embedded,
	text_chunks_excluded, text_files_skipped, text_bytes_embedded
) VALUES (
	$1, $2, $3, $4, $5, $6, $7::jsonb, $8,
	$9, $10, $11, $12, $13::jsonb, $14
)
ON CONFLICT (job_id) DO UPDATE SET
	is_incremental = EXCLUDED.is_incremental,
	code_files_total = EXCLUDED.code_files_total,
	code_files_embedded = EXCLUDED.code_files_embedded,
	code_chunks_embedded = EXCLUDED.code_chunks_embedded,
	code_chunks_excluded = EXCLUDED.code_chunks_excluded,
	code_files_skipped = EXCLUDED.code_files_skipped,
	code_bytes_embedded = EXCLUDED.code_bytes_embedded,
	text_files_total = EXCLUDED.text_files_total,
	text_files_embedded = EXCLUDED.text_files_embedded,
	text_chunks_embedded = EXCLUDED.text_chunks_embedded,
	text_chunks_excluded = EXCLUDED.text_chunks_excluded,
	text_files_skipped = EXCLUDED.text_files_skipped,
	text_bytes_embedded = EXCLUDED.text_bytes_embedded"#,
        )
        .bind(job_id)
        .bind(stats.is_incremental)
        .bind(stats.code_index_stats.files_scheduled)
        .bind(stats.code_index_stats.files_embedded)
        .bind(stats.code_index_stats.chunks_embedded)
        .bind(stats.code_index_stats.chunks_excluded)
        .bind(code_skipped)
        .bind(stats.code_index_stats.bytes_embedded)
        .bind(stats.text_index_stats.files_scheduled)
        .bind(stats.text_index_stats.files_embedded)
        .bind(stats.text_index_stats.chunks_embedded)
        .bind(stats.text_index_stats.chunks_excluded)
        .bind(text_skipped)
        .bind(stats.text_index_stats.bytes_embedded)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// File listing/reading (`handler.go` revisionFetcher)
// ---------------------------------------------------------------------------

/// `embed.FileEntry`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub name: String,
    pub size: i64,
}

/// `embed.FileReadLister`.
#[async_trait]
pub trait FileReadLister: Send + Sync {
    async fn read(&self, file_name: &str) -> Result<Vec<u8>, String>;

    async fn list(&self) -> Result<Vec<FileEntry>, String>;

    async fn diff(&self, old_commit: &str) -> Result<(Vec<FileEntry>, Vec<String>), String>;
}

/// The gitserver operations the handler and the revision fetcher use.
#[async_trait]
pub trait GitServer: Send + Sync {
    async fn get_default_branch(&self, repo: &str) -> Result<(String, String), String>;

    async fn read_file(&self, repo: &str, commit: &str, path: &str) -> Result<Vec<u8>, String>;

    async fn read_dir(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<sg_git::DirEntry>, String>;

    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<sg_git::ChangedFile>, String>;
}

#[async_trait]
impl GitServer for sg_git::GitserverClient {
    async fn get_default_branch(&self, repo: &str) -> Result<(String, String), String> {
        self.default_branch_commit(repo)
            .await
            .map_err(|err| err.to_string())
    }

    async fn read_file(&self, repo: &str, commit: &str, path: &str) -> Result<Vec<u8>, String> {
        sg_git::GitserverClient::read_file(self, repo, commit, path)
            .await
            .map_err(|err| err.to_string())
    }

    async fn read_dir(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<sg_git::DirEntry>, String> {
        self.read_dir_entries(repo, commit, path, recursive)
            .await
            .map_err(|err| err.to_string())
    }

    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<sg_git::ChangedFile>, String> {
        sg_git::GitserverClient::changed_files(self, repo, base, head)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `revisionFetcher`.
pub struct RevisionFetcher {
    git: Arc<dyn GitServer>,
    pub repo: String,
    pub revision: String,
}

impl RevisionFetcher {
    pub fn new(
        git: Arc<dyn GitServer>,
        repo: impl Into<String>,
        revision: impl Into<String>,
    ) -> Self {
        Self {
            git,
            repo: repo.into(),
            revision: revision.into(),
        }
    }

    /// `validateRevision`.
    pub async fn validate_revision(&self) -> Result<(), String> {
        if self.revision.is_empty() {
            self.git.get_default_branch(&self.repo).await?;
            return Err(format!(
                "could not get latest commit for repo {}",
                self.repo
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl FileReadLister for RevisionFetcher {
    async fn read(&self, file_name: &str) -> Result<Vec<u8>, String> {
        self.git
            .read_file(&self.repo, &self.revision, file_name)
            .await
    }

    async fn list(&self) -> Result<Vec<FileEntry>, String> {
        let entries = self
            .git
            .read_dir(&self.repo, &self.revision, "", true)
            .await?;
        Ok(entries
            .into_iter()
            .filter(|entry| !entry.is_dir)
            .map(|entry| FileEntry {
                name: entry.name,
                size: entry.size,
            })
            .collect())
    }

    async fn diff(&self, old_commit: &str) -> Result<(Vec<FileEntry>, Vec<String>), String> {
        let changed = self
            .git
            .changed_files(&self.repo, old_commit, &self.revision)
            .await
            .map_err(|err| format!("iterating over changed files in git diff: {err}"))?;

        let mut to_remove = Vec::new();
        let mut changed_new = Vec::new();
        for file in changed {
            match file.status {
                sg_git::ChangeStatus::Deleted => to_remove.push(file.path),
                sg_git::ChangeStatus::Modified => {
                    to_remove.push(file.path.clone());
                    changed_new.push(file.path);
                }
                sg_git::ChangeStatus::Added => changed_new.push(file.path),
                sg_git::ChangeStatus::TypeChanged => {}
            }
        }

        let all_files = self.list().await?;
        let changed_new: HashSet<String> = changed_new.into_iter().collect();
        let to_index = all_files
            .into_iter()
            .filter(|file| changed_new.contains(&file.name))
            .collect();
        Ok((to_index, to_remove))
    }
}

// ---------------------------------------------------------------------------
// Object storage (`internal/embeddings/index_storage.go`)
// ---------------------------------------------------------------------------

/// `UploadRepoEmbeddingIndex`.
pub async fn upload_repo_embedding_index(
    store: &dyn ObjectStorageBackend,
    key: &str,
    index: &RepoEmbeddingIndex,
) -> Result<(), String> {
    let mut bytes = Vec::with_capacity(INDEX_MAGIC.len() + 1024);
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(&serde_json::to_vec(index).map_err(|err| err.to_string())?);
    store.upload(key, bytes).await.map(|_bytes| ())
}

fn decode_index(bytes: &[u8]) -> Result<RepoEmbeddingIndex, String> {
    let payload = bytes
        .strip_prefix(INDEX_MAGIC)
        .ok_or_else(|| "unrecognized embedding index format (expected SGEMB1)".to_owned())?;
    serde_json::from_slice(payload).map_err(|err| err.to_string())
}

/// `downloadRepoEmbeddingIndex`.
async fn download_repo_embedding_index_at(
    store: &dyn ObjectStorageBackend,
    key: &str,
) -> Result<RepoEmbeddingIndex, String> {
    let bytes = store.get(key).await?;
    decode_index(&bytes)
}

/// `DownloadRepoEmbeddingIndex` (repo id key, then the deprecated repo name
/// key).
pub async fn download_repo_embedding_index(
    store: &dyn ObjectStorageBackend,
    repo_id: i32,
    repo_name: &str,
) -> Result<RepoEmbeddingIndex, String> {
    let primary = repo_embedding_index_name(repo_id);
    match download_repo_embedding_index_at(store, &primary).await {
        Ok(index) => Ok(index),
        Err(primary_error) => {
            let deprecated = repo_embedding_index_name_deprecated(repo_name);
            download_repo_embedding_index_at(store, &deprecated)
                .await
                .map_err(|deprecated_error| format!("{primary_error}; {deprecated_error}"))
        }
    }
}

/// `UpdateRepoEmbeddingIndex`.
pub async fn update_repo_embedding_index(
    store: &dyn ObjectStorageBackend,
    key: &str,
    previous: &mut RepoEmbeddingIndex,
    new: &RepoEmbeddingIndex,
    to_remove: &[String],
    ranks: &RepoPathRanks,
) -> Result<(), String> {
    previous.revision = new.revision.clone();
    previous.embeddings_model = new.embeddings_model.clone();

    let to_remove_set: HashSet<&str> = to_remove.iter().map(String::as_str).collect();
    previous.code_index.filter(&to_remove_set, ranks);
    previous.text_index.filter(&to_remove_set, ranks);

    previous.code_index.append(&new.code_index);
    previous.text_index.append(&new.text_index);

    upload_repo_embedding_index(store, key, previous).await
}

// ---------------------------------------------------------------------------
// Embedding core (`internal/embeddings/embed/embed.go`)
// ---------------------------------------------------------------------------

/// `embed.FileFilters`.
#[derive(Debug, Clone, Default)]
pub struct FileFilters {
    pub exclude_patterns: Vec<GlobPattern>,
    pub include_patterns: Vec<GlobPattern>,
    pub max_file_size_bytes: i64,
}

/// `embed.EmbedRepoOpts`.
#[derive(Debug, Clone)]
pub struct EmbedRepoOpts {
    pub repo_name: String,
    pub revision: String,
    pub file_filters: FileFilters,
    pub split_options: SplitOptions,
    pub max_code_embeddings: usize,
    pub max_text_embeddings: usize,
    pub batch_size: usize,
    pub exclude_chunks: bool,
    pub tolerable_failure_ratio: f64,
    /// Non-empty when a previous index exists for `indexed_revision`.
    pub indexed_revision: String,
}

/// `bgrepo.EmbedFilesStats`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedFilesStats {
    pub files_scheduled: i64,
    pub files_embedded: i64,
    pub files_skipped: BTreeMap<String, i64>,
    pub chunks_embedded: i64,
    pub chunks_excluded: i64,
    pub bytes_embedded: i64,
}

impl EmbedFilesStats {
    pub fn new(files_total: usize) -> Self {
        Self {
            files_scheduled: files_total as i64,
            files_embedded: 0,
            files_skipped: BTreeMap::new(),
            chunks_embedded: 0,
            chunks_excluded: 0,
            bytes_embedded: 0,
        }
    }

    fn skip(&mut self, reason: &str) {
        *self.files_skipped.entry(reason.to_owned()).or_insert(0) += 1;
    }

    fn add_chunks(&mut self, count: i64, size: i64) {
        self.chunks_embedded += count;
        self.bytes_embedded += size;
    }

    fn exclude_chunks(&mut self, count: i64) {
        self.chunks_excluded += count;
    }

    fn add_file(&mut self) {
        self.files_embedded += 1;
    }
}

/// `bgrepo.EmbedRepoStats`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedRepoStats {
    pub code_index_stats: EmbedFilesStats,
    pub text_index_stats: EmbedFilesStats,
    pub is_incremental: bool,
}

/// `FlushResults`.
struct FlushResults {
    size: i64,
    count: i64,
}

/// `embedFiles`.
#[allow(clippy::too_many_arguments)]
pub async fn embed_files(
    ctx: &CancellationToken,
    client: &dyn EmbeddingsClient,
    context_service: &dyn ContextService,
    files: &[FileEntry],
    file_filters: &FileFilters,
    split_options: SplitOptions,
    reader: &dyn FileReadLister,
    max_embedding_vectors: usize,
    batch_size: usize,
    exclude_chunks_on_error: bool,
    insert: &mut (dyn FnMut(Vec<RepoEmbeddingRowMetadata>, &[f32]) + Send),
    report_progress: &mut (dyn FnMut(&EmbedFilesStats) + Send),
) -> Result<EmbedFilesStats, String> {
    let dimensions = client.get_dimensions()?;
    let mut stats = EmbedFilesStats::new(files.len());
    let mut batch: Vec<EmbeddableChunk> = Vec::new();

    for file in files {
        if ctx.is_cancelled() {
            return Err("context canceled".to_owned());
        }

        if stats.chunks_embedded as usize >= max_embedding_vectors {
            stats.skip(SKIP_REASON_MAX_EMBEDDINGS);
            continue;
        }

        if file.size > file_filters.max_file_size_bytes {
            stats.skip(SKIP_REASON_LARGE);
            continue;
        }

        if is_excluded_file_path_match(&file.name, &file_filters.exclude_patterns) {
            stats.skip(SKIP_REASON_EXCLUDED);
            continue;
        }

        if !is_included_file_path_match(&file.name, &file_filters.include_patterns) {
            stats.skip(SKIP_REASON_NOT_INCLUDED);
            continue;
        }

        let content = reader
            .read(&file.name)
            .await
            .map_err(|err| format!("error while reading a file: {err}"))?;

        let (embeddable, skip_reason) = is_embeddable_file_content(&content);
        if !embeddable {
            stats.skip(skip_reason);
            continue;
        }

        let text = String::from_utf8_lossy(&content).into_owned();
        let chunks = context_service
            .split_into_embeddable_chunks(&text, &file.name, split_options)
            .await
            .map_err(|err| format!("error while splitting file: {err}"))?;

        for chunk in chunks {
            batch.push(chunk);
            if batch.len() >= batch_size {
                if let Some(results) = flush_batch(
                    &mut batch,
                    client,
                    dimensions,
                    exclude_chunks_on_error,
                    insert,
                    report_progress,
                    &stats,
                )
                .await?
                {
                    stats.add_chunks(results.count, results.size);
                    stats.exclude_chunks(batch_size.saturating_sub(results.count as usize) as i64);
                }
            }
        }
        stats.add_file();
    }

    let current_batch = batch.len();
    if let Some(results) = flush_batch(
        &mut batch,
        client,
        dimensions,
        exclude_chunks_on_error,
        insert,
        report_progress,
        &stats,
    )
    .await?
    {
        stats.add_chunks(results.count, results.size);
        stats.exclude_chunks(current_batch.saturating_sub(results.count as usize) as i64);
    }

    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
async fn flush_batch(
    batch: &mut Vec<EmbeddableChunk>,
    client: &dyn EmbeddingsClient,
    dimensions: usize,
    exclude_chunks_on_error: bool,
    insert: &mut (dyn FnMut(Vec<RepoEmbeddingRowMetadata>, &[f32]) + Send),
    report_progress: &mut (dyn FnMut(&EmbedFilesStats) + Send),
    stats: &EmbedFilesStats,
) -> Result<Option<FlushResults>, String> {
    if batch.is_empty() {
        return Ok(None);
    }

    let batch_chunks: Vec<String> = batch.iter().map(|chunk| chunk.content.clone()).collect();
    let batch_embeddings = match client.get_document_embeddings(batch_chunks.clone()).await {
        Ok(results) => results,
        Err(err) => {
            // A rate limit always fails the job: retrying every chunk would
            // just keep hitting the limit (the baseline's
            // `errors.Is(err, &client.RateLimitExceededError{})`, using the
            // comparator from `lib/errors`, matches by type).
            if !exclude_chunks_on_error || matches!(err, EmbeddingsClientError::RateLimitExceeded) {
                return Err(format!("error while getting embeddings: {err}"));
            }
            let failed: Vec<i64> = (0..batch_chunks.len() as i64).collect();
            EmbeddingsResults {
                embeddings: vec![0.0; batch_chunks.len() * dimensions],
                failed,
                dimensions,
            }
        }
    };

    let expected = batch_chunks.len() * dimensions;
    if batch_embeddings.embeddings.len() != expected {
        return Err(format!(
            "expected embeddings for batch to have length {expected}, got {}",
            batch_embeddings.embeddings.len()
        ));
    }

    if !exclude_chunks_on_error && !batch_embeddings.failed.is_empty() {
        let index = batch_embeddings.failed[0];
        let file = batch
            .get(index.max(0) as usize)
            .map(|chunk| chunk.file_name.as_str())
            .unwrap_or("");
        return Err(format!("batch failed on file {file:?}"));
    }

    let excluded: HashSet<usize> = batch_embeddings
        .failed
        .iter()
        .filter(|&&index| index >= 0 && (index as usize) < batch.len())
        .map(|&index| index as usize)
        .collect();

    // The baseline subtracts every reported failure, including out-of-range
    // indices, and only the in-range ones are actually skipped below.
    let rows_count = batch.len().saturating_sub(batch_embeddings.failed.len());
    let mut metadata = Vec::with_capacity(batch.len());
    let mut size = 0i64;
    let mut embeddings = Vec::with_capacity(batch.len() * dimensions);
    for (index, chunk) in batch.iter().enumerate() {
        if excluded.contains(&index) {
            continue;
        }
        // `copy(batchEmbeddings.Row(cursor), batchEmbeddings.Row(idx))`
        embeddings.extend_from_slice(batch_embeddings.row(index));
        metadata.push(RepoEmbeddingRowMetadata {
            file_name: chunk.file_name.clone(),
            start_line: chunk.start_line,
            end_line: chunk.end_line,
        });
        size += chunk.content.len() as i64;
    }
    insert(metadata, &embeddings);

    batch.clear();
    report_progress(stats);
    Ok(Some(FlushResults {
        size,
        count: rows_count as i64,
    }))
}

fn new_index(num_files: usize, dimensions: usize) -> EmbeddingIndex {
    EmbeddingIndex {
        embeddings: Vec::with_capacity(num_files * dimensions / 2),
        column_dimension: dimensions as i64,
        row_metadata: Vec::with_capacity(num_files / 2),
        ranks: Vec::with_capacity(num_files / 2),
    }
}

fn insert_index(
    index: &mut EmbeddingIndex,
    ranks: &RepoPathRanks,
    metadata: Vec<RepoEmbeddingRowMetadata>,
    embeddings: &[f32],
) {
    index.embeddings.extend(quantize(embeddings));
    for row in &metadata {
        index
            .ranks
            .push(ranks.paths.get(&row.file_name).copied().unwrap_or(0.0) as f32);
    }
    index.row_metadata.extend(metadata);
}

/// `embed.EmbedRepo`.
#[allow(clippy::too_many_arguments)]
pub async fn embed_repo(
    ctx: &CancellationToken,
    client: &dyn EmbeddingsClient,
    context_service: &dyn ContextService,
    read_lister: &dyn FileReadLister,
    repo: &RepoRecord,
    ranks: &RepoPathRanks,
    opts: &EmbedRepoOpts,
    report_progress: &mut (dyn FnMut(&EmbedRepoStats) + Send),
) -> Result<(RepoEmbeddingIndex, Vec<String>, EmbedRepoStats), String> {
    let mut is_incremental = !opts.indexed_revision.is_empty();
    let (to_index, mut to_remove) = if is_incremental {
        match read_lister.diff(&opts.indexed_revision).await {
            Ok((to_index, to_remove)) => (to_index, to_remove),
            Err(_) => {
                is_incremental = false;
                (read_lister.list().await?, Vec::new())
            }
        }
    } else {
        (read_lister.list().await?, Vec::new())
    };

    let mut code_file_names = Vec::new();
    let mut text_file_names = Vec::new();
    for file in to_index {
        if is_valid_text_file(&file.name) {
            text_file_names.push(file);
        } else {
            code_file_names.push(file);
        }
    }

    let dimensions = client.get_dimensions()?;
    let mut code_index = new_index(code_file_names.len(), dimensions);
    let mut text_index = new_index(text_file_names.len(), dimensions);

    let mut stats = EmbedRepoStats {
        code_index_stats: EmbedFilesStats::new(code_file_names.len()),
        text_index_stats: EmbedFilesStats::new(text_file_names.len()),
        is_incremental,
    };

    let mut code_insert = |metadata: Vec<RepoEmbeddingRowMetadata>, embeddings: &[f32]| {
        insert_index(&mut code_index, ranks, metadata, embeddings)
    };
    let mut code_progress = |code_stats: &EmbedFilesStats| {
        stats.code_index_stats = code_stats.clone();
        report_progress(&stats);
    };
    let code_index_stats = embed_files(
        ctx,
        client,
        context_service,
        &code_file_names,
        &opts.file_filters,
        opts.split_options,
        read_lister,
        opts.max_code_embeddings,
        opts.batch_size,
        opts.exclude_chunks,
        &mut code_insert,
        &mut code_progress,
    )
    .await?;
    stats.code_index_stats = code_index_stats;

    let mut text_insert = |metadata: Vec<RepoEmbeddingRowMetadata>, embeddings: &[f32]| {
        insert_index(&mut text_index, ranks, metadata, embeddings)
    };
    let mut text_progress = |text_stats: &EmbedFilesStats| {
        stats.text_index_stats = text_stats.clone();
        report_progress(&stats);
    };
    let text_index_stats = embed_files(
        ctx,
        client,
        context_service,
        &text_file_names,
        &opts.file_filters,
        opts.split_options,
        read_lister,
        opts.max_text_embeddings,
        opts.batch_size,
        opts.exclude_chunks,
        &mut text_insert,
        &mut text_progress,
    )
    .await?;
    stats.text_index_stats = text_index_stats;

    let excluded = stats.code_index_stats.chunks_excluded + stats.text_index_stats.chunks_excluded;
    let embedded = stats.code_index_stats.chunks_embedded + stats.text_index_stats.chunks_embedded;
    let failure_ratio = excluded as f64 / (excluded + embedded) as f64;
    if failure_ratio > opts.tolerable_failure_ratio {
        return Err(format!(
            "embeddings failed at a rate ({failure_ratio:.2}) higher than acceptable ({:.2})",
            opts.tolerable_failure_ratio
        ));
    }

    let index = RepoEmbeddingIndex {
        repo_name: repo.name.clone(),
        revision: opts.revision.clone(),
        embeddings_model: client.get_model_identifier(),
        code_index,
        text_index,
    };
    to_remove.shrink_to_fit();
    Ok((index, to_remove, stats))
}

// ---------------------------------------------------------------------------
// Handler (`cmd/worker/internal/embeddings/repo/handler.go`)
// ---------------------------------------------------------------------------

/// The full `conftypes.EmbeddingsConfig` subset the handler reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsJobConfig {
    pub provider: String,
    pub incremental: bool,
    pub excluded_file_path_patterns: Vec<String>,
    pub included_file_path_patterns: Vec<String>,
    pub max_file_size_bytes: i64,
    pub max_code_embeddings_per_repo: i64,
    pub max_text_embeddings_per_repo: i64,
    pub exclude_chunk_on_error: bool,
}

fn string_array(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// `conf.GetEmbeddingsConfig` (all handler fields). `None` mirrors the nil
/// config; the gate itself is shared with the scheduler's port.
pub fn embeddings_job_config(
    site: &SiteConfiguration,
    force_allow: bool,
) -> Option<EmbeddingsJobConfig> {
    // Exact nil semantics (cody, internal flag, legacy enabled, credentials).
    embeddings_config(site, force_allow)?;

    let raw = site.raw();
    let embeddings = raw.get("embeddings").filter(|value| !value.is_null());
    let get = |key: &str| embeddings.and_then(|value| value.get(key));
    let string = |key: &str| {
        get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };

    let provider = {
        let provider = string("provider");
        if provider.is_empty() {
            "sourcegraph".to_owned()
        } else {
            provider
        }
    };

    let incremental = get("incremental")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let max_code_embeddings_per_repo = get("maxCodeEmbeddingsPerRepo")
        .and_then(serde_json::Value::as_i64)
        .filter(|value| *value != 0)
        .unwrap_or(DEFAULT_MAX_CODE_EMBEDDINGS_PER_REPO);
    let max_text_embeddings_per_repo = get("maxTextEmbeddingsPerRepo")
        .and_then(serde_json::Value::as_i64)
        .filter(|value| *value != 0)
        .unwrap_or(DEFAULT_MAX_TEXT_EMBEDDINGS_PER_REPO);
    let exclude_chunk_on_error = get("excludeChunkOnError")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);

    let mut excluded_file_path_patterns = string_array(
        embeddings.unwrap_or(&serde_json::Value::Null),
        "excludedFilePathPatterns",
    );
    let mut included_file_path_patterns = Vec::new();
    let mut max_file_size_bytes = EMBEDDINGS_MAX_FILE_SIZE_BYTES;
    if let Some(file_filters) = get("fileFilters").filter(|value| !value.is_null()) {
        included_file_path_patterns = string_array(file_filters, "includedFilePathPatterns");
        excluded_file_path_patterns.extend(string_array(file_filters, "excludedFilePathPatterns"));
        if let Some(limit) = file_filters
            .get("maxFileSizeBytes")
            .and_then(serde_json::Value::as_i64)
        {
            if limit > 0 && limit <= EMBEDDINGS_MAX_FILE_SIZE_BYTES {
                max_file_size_bytes = limit;
            }
        }
    }

    Some(EmbeddingsJobConfig {
        provider,
        incremental,
        excluded_file_path_patterns,
        included_file_path_patterns,
        max_file_size_bytes,
        max_code_embeddings_per_repo,
        max_text_embeddings_per_repo,
        exclude_chunk_on_error,
    })
}

/// `getFileFilterPathPatterns`.
pub fn get_file_filter_path_patterns(
    config: &EmbeddingsJobConfig,
) -> (Vec<GlobPattern>, Vec<GlobPattern>) {
    let mut included = Vec::new();
    let mut excluded = Vec::new();
    if !config.excluded_file_path_patterns.is_empty() {
        excluded = compile_glob_patterns(&config.excluded_file_path_patterns);
    }
    if !config.included_file_path_patterns.is_empty() {
        included = compile_glob_patterns(&config.included_file_path_patterns);
    }
    if excluded.is_empty() {
        excluded = default_excluded_file_path_patterns();
    }
    (included, excluded)
}

/// The `repo_embedding_jobs` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoEmbeddingJobRecord {
    pub id: i64,
    pub repo_id: i32,
    pub revision: String,
    pub state: String,
}

impl DbRecord for RepoEmbeddingJobRecord {
    fn record_id(&self) -> i64 {
        self.id
    }
}

/// `handler`.
pub struct RepoEmbeddingHandler {
    pub store: Arc<dyn RepoEmbeddingJobStore>,
    pub upload: Arc<dyn ObjectStorageBackend>,
    pub git: Arc<dyn GitServer>,
    pub context_service: Arc<dyn ContextService>,
    pub ranking: Arc<dyn RankingService>,
    pub client_factory: Arc<dyn EmbeddingsClientFactory>,
    pub live_config: Arc<LiveConfig>,
    pub batch_size: usize,
}

impl RepoEmbeddingHandler {
    /// `handler.Handle`.
    pub async fn handle(
        &self,
        ctx: &CancellationToken,
        record: &RepoEmbeddingJobRecord,
    ) -> Result<(), String> {
        let site = self.live_config.current().site.clone();
        let Some(config) = embeddings_job_config(&site, force_allow_embeddings()) else {
            return Ok(());
        };

        let repo = self.store.get_repo(record.repo_id).await?;

        let fetcher = RevisionFetcher::new(
            Arc::clone(&self.git),
            repo.name.clone(),
            record.revision.clone(),
        );
        fetcher.validate_revision().await?;

        let client = self.client_factory.new_client(&config)?;

        let mut previous_index = None;
        if config.incremental {
            // A failed download means "no previous embeddings index found.
            // Performing a full index".
            if let Ok(index) =
                download_repo_embedding_index(self.upload.as_ref(), repo.id, &repo.name).await
            {
                if index.is_model_compatible(&client.get_model_identifier()) {
                    previous_index = Some(index);
                }
            }
        }

        let (included, excluded) = get_file_filter_path_patterns(&config);
        let mut opts = EmbedRepoOpts {
            repo_name: repo.name.clone(),
            revision: record.revision.clone(),
            file_filters: FileFilters {
                exclude_patterns: excluded,
                include_patterns: included,
                max_file_size_bytes: config.max_file_size_bytes,
            },
            split_options: split_options(),
            max_code_embeddings: config.max_code_embeddings_per_repo.max(0) as usize,
            max_text_embeddings: config.max_text_embeddings_per_repo.max(0) as usize,
            batch_size: self.batch_size,
            exclude_chunks: config.exclude_chunk_on_error,
            tolerable_failure_ratio: EMBEDDINGS_TOLERABLE_FAILURE_RATIO,
            indexed_revision: String::new(),
        };
        if let Some(previous) = &previous_index {
            opts.indexed_revision = previous.revision.clone();
        }

        let ranks = self.ranking.get_document_ranks(&repo.name).await?;

        // `reportStats` is best-effort and only logs write errors. The callback
        // is synchronous, so writes are funneled through one ordered task; the
        // final complete report below is awaited directly after that task
        // drains, keeping it last.
        let (stats_tx, mut stats_rx) = tokio::sync::mpsc::unbounded_channel::<EmbedRepoStats>();
        let stats_store = Arc::clone(&self.store);
        let record_id = record.id;
        let stats_writer = tokio::spawn(async move {
            while let Some(stats) = stats_rx.recv().await {
                let _ = stats_store
                    .update_repo_embedding_job_stats(record_id, &stats)
                    .await;
            }
        });
        let mut report_stats = move |stats: &EmbedRepoStats| {
            let _ = stats_tx.send(stats.clone());
        };

        let result = embed_repo(
            ctx,
            client.as_ref(),
            self.context_service.as_ref(),
            &fetcher,
            &repo,
            &ranks,
            &opts,
            &mut report_stats,
        )
        .await;
        drop(report_stats);
        let _ = stats_writer.await;
        let (index, to_remove, stats) = result?;

        // Final, complete report.
        let _ = self
            .store
            .update_repo_embedding_job_stats(record.id, &stats)
            .await;

        let key = repo_embedding_index_name(repo.id);
        if stats.is_incremental {
            let mut previous = previous_index.ok_or_else(|| {
                "incremental embedding index is missing its previous index".to_owned()
            })?;
            update_repo_embedding_index(
                self.upload.as_ref(),
                &key,
                &mut previous,
                &index,
                &to_remove,
                &ranks,
            )
            .await?;
        } else {
            upload_repo_embedding_index(self.upload.as_ref(), &key, &index).await?;
        }

        // If we created a new index, the previous (deprecated, repo-name based)
        // index can be deleted best-effort.
        let deprecated = repo_embedding_index_name_deprecated(&repo.name);
        let _ = self.upload.delete(&deprecated).await;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DB worker store (`NewRepoEmbeddingJobWorkerStore`)
// ---------------------------------------------------------------------------

/// The `repo_embedding_jobs` worker store (generic dbworker SQL).
#[derive(Clone)]
pub struct RepoEmbeddingJobWorkerStore {
    pool: PgPool,
}

impl RepoEmbeddingJobWorkerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

#[async_trait]
impl DbWorkerStore for RepoEmbeddingJobWorkerStore {
    type Record = RepoEmbeddingJobRecord;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        dequeue_record(&self.pool, worker_hostname).await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let parsed: Vec<i64> = ids
            .iter()
            .map(|id| id.parse::<i64>().map_err(|err| err.to_string()))
            .collect::<Result<_, _>>()?;
        if parsed.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let rows = sqlx::query(
            r#"WITH alive_candidates AS (
	SELECT id FROM repo_embedding_jobs
	WHERE id = ANY($1) AND state = 'processing'
	ORDER BY id ASC
	FOR UPDATE
)
UPDATE repo_embedding_jobs
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel"#,
        )
        .bind(&parsed)
        .bind(now())
        .fetch_all(&self.pool)
        .await
        .map_err(|err| err.to_string())?;

        let mut known = Vec::with_capacity(rows.len());
        let mut canceled = Vec::new();
        for row in rows {
            let id: i64 = row.try_get("id").map_err(|err| err.to_string())?;
            let cancel: bool = row.try_get("cancel").map_err(|err| err.to_string())?;
            known.push(id.to_string());
            if cancel {
                canceled.push(id.to_string());
            }
        }
        known.sort();
        canceled.sort();
        Ok((known, canceled))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        let result =
            sqlx::query("UPDATE repo_embedding_jobs SET state = 'completed', finished_at = clock_timestamp() WHERE id = $1 AND state = 'processing'")
                .bind(record.id)
                .execute(&self.pool)
                .await
                .map_err(|err| err.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        let result = sqlx::query(
            "UPDATE repo_embedding_jobs\n\
             SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= $3 THEN 'failed' ELSE 'errored' END,\n\
             \tfinished_at = clock_timestamp(),\n\
             \tfailure_message = $2,\n\
             \tnum_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END\n\
             WHERE id = $1 AND state = 'processing'",
        )
        .bind(record.id)
        .bind(failure_message)
        .bind(1i32)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        let result = sqlx::query(
            "UPDATE repo_embedding_jobs\n\
             SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,\n\
             \tfinished_at = clock_timestamp(),\n\
             \tfailure_message = $2,\n\
             \tnum_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END\n\
             WHERE id = $1 AND state = 'processing'",
        )
        .bind(record.id)
        .bind(failure_message)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(result.rows_affected() > 0)
    }
}

async fn dequeue_record(
    pool: &PgPool,
    worker_hostname: &str,
) -> Result<Option<RepoEmbeddingJobRecord>, String> {
    let row = sqlx::query(
        r#"WITH potential_candidates AS (
	SELECT
		id AS candidate_id,
		ROW_NUMBER() OVER (ORDER BY repo_embedding_jobs.queued_at, repo_embedding_jobs.id) AS candidate_order
	FROM repo_embedding_jobs
	WHERE
		state = 'queued' AND
		(process_after IS NULL OR process_after <= $1)
	ORDER BY repo_embedding_jobs.queued_at, repo_embedding_jobs.id
	LIMIT 50
),
candidate AS (
	SELECT id FROM repo_embedding_jobs
	JOIN potential_candidates pc ON pc.candidate_id = id
	WHERE state IN ('queued')
	ORDER BY pc.candidate_order
	FOR UPDATE OF repo_embedding_jobs SKIP LOCKED
	LIMIT 1
),
updated_record AS (
	UPDATE repo_embedding_jobs
	SET
		state = 'processing',
		started_at = $1,
		last_heartbeat_at = $1,
		finished_at = NULL,
		failure_message = NULL,
		execution_logs = NULL,
		worker_hostname = $2
	WHERE id IN (SELECT id FROM candidate)
)
SELECT id, repo_id, revision, state
FROM repo_embedding_jobs
WHERE id IN (SELECT id FROM candidate)"#,
    )
    .bind(now())
    .bind(worker_hostname)
    .fetch_optional(pool)
    .await
    .map_err(|err| err.to_string())?;

    match row {
        None => Ok(None),
        Some(row) => Ok(Some(RepoEmbeddingJobRecord {
            id: row.try_get("id").map_err(|err| err.to_string())?,
            repo_id: row.try_get("repo_id").map_err(|err| err.to_string())?,
            revision: row.try_get("revision").map_err(|err| err.to_string())?,
            state: row.try_get("state").map_err(|err| err.to_string())?,
        })),
    }
}

/// Routes the dbworker loop to [`RepoEmbeddingHandler`].
pub struct RepoEmbeddingWorkerHandler {
    pub handler: RepoEmbeddingHandler,
}

#[async_trait]
impl DbWorkerHandler<RepoEmbeddingJobWorkerStore> for RepoEmbeddingWorkerHandler {
    async fn handle(
        &self,
        ctx: CancellationToken,
        record: &RepoEmbeddingJobRecord,
    ) -> Result<(), String> {
        self.handler.handle(&ctx, record).await
    }
}

// ---------------------------------------------------------------------------
// Job assembly (`cmd/worker/internal/embeddings/repo/worker.go`)
// ---------------------------------------------------------------------------

/// `newRepoEmbeddingJobWorker`'s worker options.
pub fn worker_options() -> WorkerOptions {
    let mut options = WorkerOptions::new(WORKER_NAME);
    options.interval = WORKER_INTERVAL;
    options.num_handlers = 1;
    options.heartbeat_interval = HEARTBEAT_INTERVAL;
    options
}

/// `embeddingsBatchSize`: `env.MustGetInt("SRC_EMBEDDINGS_BATCH_SIZE", 512,...)`.
pub fn embeddings_batch_size() -> Result<usize, String> {
    match std::env::var(EMBEDDINGS_BATCH_SIZE_ENV) {
        Ok(raw) if !raw.is_empty() => raw
            .parse::<i64>()
            .map(|value| value.max(1) as usize)
            .map_err(|err| format!("invalid int {raw:?} for {EMBEDDINGS_BATCH_SIZE_ENV}: {err}")),
        _ => Ok(DEFAULT_EMBEDDINGS_BATCH_SIZE),
    }
}

/// The `EMBEDDINGS_UPLOAD_*` object storage config (`internal/embeddings/object_storage.go`).
#[derive(Debug, Default)]
pub struct EmbeddingsObjectStorageConfig {
    storage: std::sync::Mutex<Option<StorageConfig>>,
    errs: std::sync::Mutex<Vec<String>>,
}

impl EmbeddingsObjectStorageConfig {
    pub fn storage(&self) -> Option<StorageConfig> {
        self.storage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

fn get_bool(
    env: &mut sg_core::EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> bool {
    let raw = env.get(name, default, description);
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

impl JobConfig for EmbeddingsObjectStorageConfig {
    fn load(&self, env: &mut sg_core::EnvReader) {
        let mut errs = Vec::new();

        let backend = env
            .get(
                "EMBEDDINGS_UPLOAD_BACKEND",
                "blobstore",
                "The target file service for embeddings. S3, GCS, and Blobstore are supported.",
            )
            .to_lowercase();
        let manage_bucket = get_bool(
            env,
            "EMBEDDINGS_UPLOAD_MANAGE_BUCKET",
            "false",
            "Whether or not the client should manage the target bucket configuration.",
            &mut errs,
        );
        let bucket = env.get(
            "EMBEDDINGS_UPLOAD_BUCKET",
            "embeddings",
            "The name of the bucket to store embeddings in.",
        );

        if !matches!(backend.as_str(), "blobstore" | "s3" | "gcs") {
            errs.push(format!(
                "invalid backend {backend:?} for EMBEDDINGS_UPLOAD_BACKEND: must be S3, GCS, or Blobstore"
            ));
        }

        let mut s3 = S3Config::default();
        let mut gcs = GcsConfig::default();
        if matches!(backend.as_str(), "blobstore" | "s3") {
            s3.region = env.get(
                "EMBEDDINGS_UPLOAD_AWS_REGION",
                "us-east-1",
                "The target AWS region.",
            );
            s3.endpoint = env.get(
                "EMBEDDINGS_UPLOAD_AWS_ENDPOINT",
                &sg_codeintel::lsifuploadstore::blobstore_default_endpoint(),
                "The target AWS endpoint.",
            );
            s3.use_path_style = get_bool(
                env,
                "EMBEDDINGS_UPLOAD_AWS_USE_PATH_STYLE",
                "false",
                "Whether to use path calling (vs subdomain calling).",
                &mut errs,
            );
            let ec2_role_credentials = get_bool(
                env,
                "EMBEDDINGS_UPLOAD_AWS_USE_EC2_ROLE_CREDENTIALS",
                "false",
                "Whether to use the EC2 metadata API, or use the provided static credentials.",
                &mut errs,
            );

            if !ec2_role_credentials {
                s3.access_key_id = env.get(
                    "EMBEDDINGS_UPLOAD_AWS_ACCESS_KEY_ID",
                    "AKIAIOSFODNN7EXAMPLE",
                    "An AWS access key associated with a user with access to S3.",
                );
                s3.secret_access_key = env.get(
                    "EMBEDDINGS_UPLOAD_AWS_SECRET_ACCESS_KEY",
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                    "An AWS secret key associated with a user with access to S3.",
                );
                s3.session_token = env.get(
                    "EMBEDDINGS_UPLOAD_AWS_SESSION_TOKEN",
                    "",
                    "An optional AWS session token associated with a user with access to S3.",
                );
            }
        } else if backend == "gcs" {
            gcs.project_id = env.get(
                "EMBEDDINGS_UPLOAD_GCP_PROJECT_ID",
                "",
                "The project containing the GCS bucket.",
            );
            gcs.credentials_file = env.get(
                "EMBEDDINGS_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE",
                "",
                "The path to a service account key file with access to GCS.",
            );
            gcs.credentials_file_contents = env.get(
                "EMBEDDINGS_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE_CONTENT",
                "",
                "The contents of a service account key file with access to GCS.",
            );
        }

        *self
            .storage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(
            StorageConfig {
                backend,
                manage_bucket,
                bucket,
                s3,
                gcs,
            }
            .normalize(),
        );
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), sg_core::ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match crate::config::combine_errors(&errs) {
            Some(message) => Err(sg_core::ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// `embeddings.NewObjectStorage`'s storage config.
pub fn object_storage_config(
    config: &EmbeddingsObjectStorageConfig,
) -> Result<StorageConfig, String> {
    let storage = config
        .storage()
        .ok_or_else(|| "embeddings object storage config was not loaded".to_owned())?;
    match storage.backend.as_str() {
        "s3" | "blobstore" | "gcs" => Ok(storage),
        other => Err(format!("unsupported object storage backend {other:?}")),
    }
}

/// `cmd/worker/internal/embeddings/repo.NewRepoEmbeddingJob`.
pub struct RepoEmbeddingJob {
    storage: Arc<EmbeddingsObjectStorageConfig>,
}

impl RepoEmbeddingJob {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(EmbeddingsObjectStorageConfig::default()),
        }
    }
}

impl Default for RepoEmbeddingJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for RepoEmbeddingJob {
    // `Description()` returns the empty string.
    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.storage) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let storage = object_storage_config(&self.storage)?;
        let upload: Arc<dyn ObjectStorageBackend> = new_object_storage(storage).await?;

        let unified = deps.live_config.current();
        let git: Arc<dyn GitServer> = Arc::new(sg_git::GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));

        let handler = RepoEmbeddingHandler {
            store: Arc::new(PgRepoEmbeddingJobStore::new(deps.frontend.clone())),
            upload,
            git,
            context_service: Arc::new(DefaultContextService),
            ranking: Arc::new(PgRankingService::new(deps.frontend.clone())),
            client_factory: Arc::new(LiveEmbeddingsClientFactory {
                live_config: Arc::clone(&deps.live_config),
                transport: Arc::new(sg_embeddings::ReqwestTransport::new()),
            }),
            live_config: Arc::clone(&deps.live_config),
            batch_size: embeddings_batch_size()?,
        };

        let routine = WorkerRoutine::new(
            Arc::new(RepoEmbeddingJobWorkerStore::new(deps.frontend.clone())),
            Arc::new(RepoEmbeddingWorkerHandler { handler }),
            worker_options(),
        );
        Ok(vec![BackgroundRoutine::DbBacked(
            Arc::new(routine) as Arc<dyn DbBackedRoutine>
        )])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thresholds_match_the_baseline() {
        assert_eq!(EMBED_ENTIRE_FILE_TOKENS_THRESHOLD, 384);
        assert_eq!(EMBEDDING_CHUNK_TOKENS_THRESHOLD, 256);
        assert_eq!(EMBEDDING_CHUNK_EARLY_SPLIT_TOKENS_THRESHOLD, 224);
        assert_eq!(EMBEDDINGS_TOLERABLE_FAILURE_RATIO, 0.10);
        assert_eq!(DEFAULT_EMBEDDINGS_BATCH_SIZE, 512);
    }

    #[test]
    fn short_files_are_not_split() {
        let text = "hello\nworld";
        let chunks = split_into_embeddable_chunks(text, "a.txt", split_options());
        assert_eq!(
            chunks,
            vec![EmbeddableChunk {
                file_name: "a.txt".to_owned(),
                start_line: 0,
                end_line: 2,
                content: text.to_owned(),
            }]
        );
    }

    #[test]
    fn split_uses_the_early_threshold_at_splittable_lines() {
        // The file is over the no-split threshold; the long second line
        // crosses the chunk threshold and the following `fn` line is
        // splittable, so the chunk ends before it.
        let long = "x".repeat(4 * 500);
        let text = format!("// comment\n{long}\nfn main() {{}}");
        let chunks = split_into_embeddable_chunks(&text, "a.go", split_options());
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0].start_line, 0);
        assert_eq!(
            chunks.last().map(|chunk| chunk.end_line),
            Some(text.matches('\n').count() as i64 + 1)
        );
    }

    #[test]
    fn text_file_detection_matches_the_extension_table() {
        for name in ["README.md", "notes.TXT", "LICENSE", "license.txt"] {
            assert!(is_valid_text_file(name), "{name}");
        }
        for name in ["main.rs", "data.json", "image.svg"] {
            assert!(!is_valid_text_file(name), "{name}");
        }
    }

    #[test]
    fn embeddable_content_checks_match_the_baseline() {
        // Invalid UTF-8 plus a NUL byte fails the `text/plain` sniff.
        assert_eq!(
            is_embeddable_file_content(b"\xff\x00\x01\x02"),
            (false, SKIP_REASON_BINARY)
        );
        // Valid UTF-8 is never binary, regardless of control bytes.
        assert_eq!(
            is_embeddable_file_content(b"\x00\x01\x02"),
            (false, SKIP_REASON_SMALL)
        );
        assert_eq!(
            is_embeddable_file_content(b"short"),
            (false, SKIP_REASON_SMALL)
        );
        let autogenerated = b"// autogenerated file\n".repeat(4);
        assert_eq!(
            is_embeddable_file_content(&autogenerated),
            (false, SKIP_REASON_AUTOGENERATED)
        );
        let long_line = vec![b'a'; MAX_LINE_LENGTH + 1];
        assert_eq!(
            is_embeddable_file_content(&long_line),
            (false, SKIP_REASON_LONG_LINE)
        );
        let good = b"fn main() {\n    println!(\"hello world\");\n}\n";
        assert_eq!(is_embeddable_file_content(good), (true, ""));
    }

    #[test]
    fn quantize_clamps_and_rounds() {
        assert_eq!(
            quantize(&[1.5, -1.5, 0.5, 0.0, 0.01]),
            vec![127, -127, 64, 0, 1]
        );
    }

    #[test]
    fn index_names_match_the_baseline() {
        assert_eq!(repo_embedding_index_name(42), "42.embeddingindex");
        let name = repo_embedding_index_name_deprecated("github.com/acme/repo");
        assert_eq!(
            name,
            format!(
                "github_com_acme_repo_{:x}.embeddingindex",
                md5::compute(b"github.com/acme/repo")
            )
        );
    }

    #[test]
    fn file_filters_default_to_the_excluded_table() {
        let config = EmbeddingsJobConfig {
            provider: "sourcegraph".to_owned(),
            incremental: true,
            excluded_file_path_patterns: Vec::new(),
            included_file_path_patterns: Vec::new(),
            max_file_size_bytes: EMBEDDINGS_MAX_FILE_SIZE_BYTES,
            max_code_embeddings_per_repo: DEFAULT_MAX_CODE_EMBEDDINGS_PER_REPO,
            max_text_embeddings_per_repo: DEFAULT_MAX_TEXT_EMBEDDINGS_PER_REPO,
            exclude_chunk_on_error: true,
        };
        let (included, excluded) = get_file_filter_path_patterns(&config);
        assert!(included.is_empty());
        assert_eq!(excluded.len(), DEFAULT_EXCLUDED_FILE_PATH_PATHS_COUNT);
    }

    const DEFAULT_EXCLUDED_FILE_PATH_PATHS_COUNT: usize = 11;

    #[test]
    fn job_is_registered_under_the_baseline_name() {
        let jobs = crate::jobs::all_jobs();
        let (name, job) = jobs
            .iter()
            .find(|(name, _)| *name == "repo-embedding-job")
            .expect("registry keeps the baseline name");
        assert_eq!(*name, "repo-embedding-job");
        assert_eq!(job.configs().len(), 1);
        assert_eq!(job.description(), "");
    }

    fn parse_site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn provider_config_matches_get_embeddings_config_defaults() {
        // sourcegraph: license-derived token, default endpoint/model/dims.
        let site = parse_site(r#"{"cody.enabled": true, "licenseKey": "key"}"#);
        let config = embeddings_provider_config(&site, true).expect("configured");
        assert_eq!(config.provider, sg_embeddings::Provider::Sourcegraph);
        assert_eq!(config.access_token, license_key_based_access_token("key"));
        assert_eq!(
            config.endpoint,
            "https://cody-gateway.sourcegraph.com/v1/embeddings"
        );
        assert_eq!(config.model, "openai/text-embedding-ada-002");
        assert_eq!(config.dimensions, 1536);

        // An explicit token wins; model is lowercased.
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "sourcegraph", "accessToken": "TOK", "model": "OpenAI/Text-Embedding-Ada-002"}}"#,
        );
        let config = embeddings_provider_config(&site, true).expect("configured");
        assert_eq!(config.access_token, "TOK");
        assert_eq!(config.model, "openai/text-embedding-ada-002");
        assert_eq!(config.dimensions, 1536);

        // `url` is the deprecated name of `endpoint`.
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "sourcegraph", "accessToken": "t", "url": "http://u/v1/embeddings"}}"#,
        );
        assert_eq!(
            embeddings_provider_config(&site, true)
                .expect("configured")
                .endpoint,
            "http://u/v1/embeddings"
        );

        // openai defaults.
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "openai", "accessToken": "t"}}"#,
        );
        let config = embeddings_provider_config(&site, true).expect("configured");
        assert_eq!(config.provider, sg_embeddings::Provider::OpenAI);
        assert_eq!(config.endpoint, "https://api.openai.com/v1/embeddings");
        assert_eq!(config.model, "text-embedding-ada-002");
        assert_eq!(config.dimensions, 1536);

        // azure-openai keeps the configured endpoint/dimensions and lowercases
        // the model; a token is optional at the config layer.
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "azure-openai", "endpoint": "https://x/", "model": "MY-DEPLOY", "dimensions": 8}}"#,
        );
        let config = embeddings_provider_config(&site, true).expect("configured");
        assert_eq!(config.provider, sg_embeddings::Provider::AzureOpenAI);
        assert_eq!(config.endpoint, "https://x/");
        assert_eq!(config.model, "my-deploy");
        assert_eq!(config.dimensions, 8);
        assert_eq!(config.access_token, "");

        // Unknown providers and a failed gate are nil.
        let site = parse_site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "bogus", "accessToken": "t"}}"#,
        );
        assert_eq!(embeddings_provider_config(&site, true), None);
        let site = parse_site(r#"{"cody.enabled": true, "licenseKey": "key"}"#);
        assert_eq!(embeddings_provider_config(&site, false), None);
    }

    struct FakeSgClient {
        error: Option<sg_embeddings::EmbeddingsError>,
    }

    #[async_trait]
    impl sg_embeddings::EmbeddingsClient for FakeSgClient {
        async fn get_query_embedding(
            &self,
            _query: String,
        ) -> Result<sg_embeddings::EmbeddingsResults, sg_embeddings::EmbeddingsError> {
            unreachable!()
        }

        async fn get_document_embeddings(
            &self,
            _documents: Vec<String>,
        ) -> Result<sg_embeddings::EmbeddingsResults, sg_embeddings::EmbeddingsError> {
            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(sg_embeddings::EmbeddingsResults {
                    embeddings: vec![1.0, 2.0],
                    failed: vec![1],
                    dimensions: 2,
                }),
            }
        }

        fn get_dimensions(&self) -> Result<usize, String> {
            Ok(2)
        }

        fn get_model_identifier(&self) -> String {
            "sourcegraph/model".to_owned()
        }
    }

    #[tokio::test]
    async fn adapter_maps_results_and_rate_limits() {
        let adapter = SgEmbeddingsClientAdapter::new(Arc::new(FakeSgClient { error: None }));
        let results = adapter
            .get_document_embeddings(vec!["a".to_owned()])
            .await
            .expect("ok");
        assert_eq!(results.embeddings, vec![1.0, 2.0]);
        assert_eq!(results.failed, vec![1]);
        assert_eq!(results.dimensions, 2);
        assert_eq!(adapter.get_dimensions().unwrap(), 2);
        assert_eq!(adapter.get_model_identifier(), "sourcegraph/model");

        let rate_limited = SgEmbeddingsClientAdapter::new(Arc::new(FakeSgClient {
            error: Some(sg_embeddings::EmbeddingsError::RateLimitExceeded(
                sg_embeddings::RetryAfter::Seconds(3),
            )),
        }));
        assert_eq!(
            rate_limited
                .get_document_embeddings(vec!["a".to_owned()])
                .await
                .unwrap_err(),
            EmbeddingsClientError::RateLimitExceeded
        );

        let other = SgEmbeddingsClientAdapter::new(Arc::new(FakeSgClient {
            error: Some(sg_embeddings::EmbeddingsError::Other("bomb".to_owned())),
        }));
        assert_eq!(
            other
                .get_document_embeddings(vec!["a".to_owned()])
                .await
                .unwrap_err(),
            EmbeddingsClientError::Other("bomb".to_owned())
        );
    }
}
