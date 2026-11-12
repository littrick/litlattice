#![forbid(unsafe_code)]

//! Production wiring for the standalone embeddings service:
//!
//! * [`LiveQueryEmbedding`] — `getQueryEmbedding` (`cmd/embeddings/shared/
//!   main.go:143`): site-config provider resolution, `embed.NewEmbeddingsClient`
//!   and the three error wraps;
//! * [`CachedEmbeddingIndexGetter`] — `NewCachedEmbeddingIndexGetter`
//!   (`repo_embedding_index_cache.go`): last completed job timestamp, the
//!   byte-bounded LRU, singleflight and the `downloading repo embedding index`
//!   wrap;
//! * [`ObjectStorageDownloader`] — `DownloadRepoEmbeddingIndex`
//!   (`internal/embeddings/index_storage.go`): id key, deprecated name key and
//!   the combined MultiError text.
//!
//! Every external system is a trait so the differential harness can script it:
//! [`EmbeddingsClientFactory`]/[`sg_embeddings::HttpTransport`] for the query
//! client, [`EmbeddingJobsStore`] for `repo_embedding_jobs`, and
//! [`EmbeddingIndexDownloader`] (or [`IndexObjectStore`]) for object storage.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_db::OffsetDateTime;
use sg_licensing::license_key_based_access_token;
use sg_objectstorage::{GcsConfig, ObjectStore, S3Config, StorageConfig};
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::index::{
    decode_index, repo_embedding_index_name, repo_embedding_index_name_deprecated, quantize,
    StoredRepoEmbeddingIndex,
};
use crate::{Config, EmbeddingBackend, EmbeddingError, RepoEmbeddingIndex};

/// `FORCE_ALLOW_EMBEDDINGS`.
pub const FORCE_ALLOW_EMBEDDINGS_ENV: &str = "FORCE_ALLOW_EMBEDDINGS";

fn go_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `conf.ForceAllowEmbeddings`.
pub fn force_allow_embeddings() -> bool {
    std::env::var(FORCE_ALLOW_EMBEDDINGS_ENV)
        .ok()
        .and_then(|raw| go_bool(&raw))
        .unwrap_or(false)
}

/// `conf.codyEnabled`.
fn cody_enabled(site: &SiteConfiguration) -> bool {
    let raw = site.raw();
    let enabled = raw.get("cody.enabled").and_then(serde_json::Value::as_bool);
    let completions = raw.get("completions").filter(|value| !value.is_null());
    match (enabled, completions) {
        (Some(false), _) => false,
        (Some(true), _) => true,
        (None, Some(completions)) => {
            completions
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        }
        (None, None) => false,
    }
}

fn string_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The gate half of `conf.GetEmbeddingsConfig` (the scheduler's separate
/// `EmbeddingsConfig` only adds scheduling fields). `None` is the baseline nil
/// config.
fn embeddings_gate(site: &SiteConfiguration, force_allow: bool) -> Option<()> {
    if !cody_enabled(site) || !force_allow {
        return None;
    }
    let embeddings = site
        .raw()
        .get("embeddings")
        .filter(|value| !value.is_null());
    if let Some(embeddings) = embeddings {
        if embeddings
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
        {
            return None;
        }
    }
    let provider = embeddings
        .map(|value| string_field(value, "provider"))
        .unwrap_or_default();
    let access_token = embeddings
        .map(|value| string_field(value, "accessToken"))
        .unwrap_or_default();
    let provider = if provider.is_empty() {
        if !access_token.is_empty() {
            return None;
        }
        "sourcegraph".to_owned()
    } else {
        provider
    };
    match provider.as_str() {
        "sourcegraph" => {
            if access_token.is_empty() {
                let license_key = site.license_key();
                if license_key.is_empty() {
                    return None;
                }
                let _ = license_key_based_access_token(&license_key);
            }
        }
        "openai" => {
            if access_token.is_empty() {
                return None;
            }
        }
        "azure-openai" => {
            let endpoint = embeddings
                .map(|value| string_field(value, "endpoint"))
                .unwrap_or_default();
            let model = embeddings
                .map(|value| string_field(value, "model"))
                .unwrap_or_default();
            if endpoint.is_empty() || model.is_empty() {
                return None;
            }
        }
        _ => return None,
    }
    Some(())
}

/// `conf.GetEmbeddingsConfig`'s provider fields, with the baseline defaults.
pub fn embeddings_provider_config(
    site: &SiteConfiguration,
    force_allow: bool,
) -> Option<sg_embeddings::ProviderConfig> {
    embeddings_gate(site, force_allow)?;

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

/// `embed.NewEmbeddingsClient`, injectable for the differential harness.
pub trait EmbeddingsClientFactory: Send + Sync {
    fn new_client(
        &self,
        config: &sg_embeddings::ProviderConfig,
    ) -> Result<Arc<dyn sg_embeddings::EmbeddingsClient>, String>;
}

/// The production factory.
pub struct SgEmbeddingsClientFactory {
    pub transport: Arc<dyn sg_embeddings::HttpTransport>,
}

impl EmbeddingsClientFactory for SgEmbeddingsClientFactory {
    fn new_client(
        &self,
        config: &sg_embeddings::ProviderConfig,
    ) -> Result<Arc<dyn sg_embeddings::EmbeddingsClient>, String> {
        sg_embeddings::new_client(config, Arc::clone(&self.transport))
    }
}

/// `getQueryEmbedding`. Site configuration is resolved per request, like
/// `conf.Get().SiteConfig()`.
pub struct LiveQueryEmbedding {
    pub site: SiteConfiguration,
    pub force_allow: bool,
    pub factory: Arc<dyn EmbeddingsClientFactory>,
}

impl LiveQueryEmbedding {
    pub fn new(
        site: SiteConfiguration,
        force_allow: bool,
        factory: Arc<dyn EmbeddingsClientFactory>,
    ) -> Self {
        Self {
            site,
            force_allow,
            factory,
        }
    }
}

#[async_trait]
impl crate::backend::QueryEmbeddingSource for LiveQueryEmbedding {
    async fn get_query_embedding(
        &self,
        query: &str,
    ) -> Result<(Vec<f32>, String), EmbeddingError> {
        let Some(config) = embeddings_provider_config(&self.site, self.force_allow) else {
            return Err(EmbeddingError::other(
                "embeddings not configured or disabled",
            ));
        };
        let client = self.factory.new_client(&config).map_err(|err| {
            EmbeddingError::other(format!("getting embeddings client: {err}"))
        })?;

        let results = client
            .get_query_embedding(query.to_owned())
            .await
            .map_err(|err| EmbeddingError {
                message: format!("getting query embedding: {err}"),
                not_found: false,
                rate_limited: err.is_rate_limit(),
            })?;
        if !results.failed.is_empty() {
            return Err(EmbeddingError::other(format!(
                "failed to get embeddings for query {query}"
            )));
        }
        Ok((results.embeddings, client.get_model_identifier()))
    }
}

/// The query half of the handler's backend.
#[async_trait]
pub trait QueryEmbeddingSource: Send + Sync {
    async fn get_query_embedding(
        &self,
        query: &str,
    ) -> Result<(Vec<f32>, String), EmbeddingError>;
}

/// `db.Repos().GetLastCompletedRepoEmbeddingJob` reduced to the timestamp.
#[async_trait]
pub trait EmbeddingJobsStore: Send + Sync {
    async fn get_last_completed(&self, repo_id: i32) -> Result<OffsetDateTime, EmbeddingError>;
}

/// The production PG store.
pub struct PgEmbeddingJobsStore {
    pool: PgPool,
}

impl PgEmbeddingJobsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EmbeddingJobsStore for PgEmbeddingJobsStore {
    async fn get_last_completed(&self, repo_id: i32) -> Result<OffsetDateTime, EmbeddingError> {
        let row = sqlx::query(
            "SELECT finished_at FROM repo_embedding_jobs \
             WHERE state = 'completed' AND repo_id = $1 \
             ORDER BY finished_at DESC LIMIT 1",
        )
        .bind(repo_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| EmbeddingError::other(err.to_string()))?
        .ok_or_else(|| {
            EmbeddingError::not_found(format!("repo embedding job not found: repoID={repo_id}"))
        })?;
        let finished_at: Option<OffsetDateTime> = row
            .try_get("finished_at")
            .map_err(|err| EmbeddingError::other(err.to_string()))?;
        finished_at.ok_or_else(|| {
            EmbeddingError::other(format!(
                "repo embedding job has no finished_at: repoID={repo_id}"
            ))
        })
    }
}

/// The object get operation the downloader needs (`object.Storage.Get` plus
/// the body drain).
#[async_trait]
pub trait IndexObjectStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Vec<u8>, String>;
}

/// Production object storage (S3/blobstore/GCS through `sg-objectstorage`).
pub struct ObjectStorageIndexStore {
    config: StorageConfig,
}

impl ObjectStorageIndexStore {
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl IndexObjectStore for ObjectStorageIndexStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        use tokio::io::AsyncReadExt;
        let store = ObjectStore::new(self.config.clone())
            .await
            .map_err(|err| err.to_string())?;
        // `object.NewLazyStorage` initializes on the first operation.
        store.init().await.map_err(|err| err.to_string())?;
        let body = store.get(key).await.map_err(|err| err.to_string())?;
        let mut bytes = Vec::new();
        body.into_async_read()
            .read_to_end(&mut bytes)
            .await
            .map_err(|err| err.to_string())?;
        Ok(bytes)
    }
}

/// `DownloadRepoEmbeddingIndex`: the id key, then the deprecated repo-name
/// key. Both failures render as the baseline `CombineErrors` MultiError.
#[async_trait]
pub trait EmbeddingIndexDownloader: Send + Sync {
    async fn download(
        &self,
        repo_id: i32,
        repo_name: &str,
    ) -> Result<StoredRepoEmbeddingIndex, String>;
}

/// The production downloader over any [`IndexObjectStore`].
pub struct ObjectStorageDownloader {
    store: Arc<dyn IndexObjectStore>,
}

impl ObjectStorageDownloader {
    pub fn new(store: Arc<dyn IndexObjectStore>) -> Self {
        Self { store }
    }

    async fn download_at(
        store: &dyn IndexObjectStore,
        key: &str,
    ) -> Result<StoredRepoEmbeddingIndex, String> {
        let bytes = store.get(key).await?;
        decode_index(&bytes)
    }
}

#[async_trait]
impl EmbeddingIndexDownloader for ObjectStorageDownloader {
    async fn download(
        &self,
        repo_id: i32,
        repo_name: &str,
    ) -> Result<StoredRepoEmbeddingIndex, String> {
        let primary = repo_embedding_index_name(repo_id);
        match Self::download_at(self.store.as_ref(), &primary).await {
            Ok(index) => Ok(index),
            Err(primary_error) => {
                let deprecated = repo_embedding_index_name_deprecated(repo_name);
                Self::download_at(self.store.as_ref(), &deprecated)
                    .await
                    .map_err(|deprecated_error| {
                        format!(
                            "2 errors occurred:\n\t* {primary_error}\n\t* {deprecated_error}"
                        )
                    })
            }
        }
    }
}

struct CacheEntry {
    index: Arc<StoredRepoEmbeddingIndex>,
    finished_at: OffsetDateTime,
    size: u64,
}

/// `embeddingsIndexCache`: a byte-bounded LRU. Recency is a simple deque; the
/// cache is tiny relative to the indexes, so the linear move is fine.
struct IndexCache {
    entries: HashMap<i32, CacheEntry>,
    order: VecDeque<i32>,
    max_size_bytes: u64,
    remaining_size_bytes: u64,
}

impl IndexCache {
    fn new(max_size_bytes: u64) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_size_bytes,
            remaining_size_bytes: max_size_bytes,
        }
    }

    fn get(&mut self, repo_id: i32) -> Option<&CacheEntry> {
        if !self.entries.contains_key(&repo_id) {
            return None;
        }
        self.order.retain(|key| *key != repo_id);
        self.order.push_back(repo_id);
        self.entries.get(&repo_id)
    }

    fn add(&mut self, repo_id: i32, index: Arc<StoredRepoEmbeddingIndex>, finished_at: OffsetDateTime) {
        let size = index.estimate_size();
        if size > self.max_size_bytes {
            return;
        }
        if let Some(previous) = self.entries.remove(&repo_id) {
            self.remaining_size_bytes += previous.size;
            self.order.retain(|key| *key != repo_id);
        }
        while self.remaining_size_bytes < size {
            let Some(oldest) = self.order.pop_front() else {
                return;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.remaining_size_bytes += evicted.size;
            }
        }
        self.remaining_size_bytes -= size;
        self.order.push_back(repo_id);
        self.entries.insert(
            repo_id,
            CacheEntry {
                index,
                finished_at,
                size,
            },
        );
    }
}

/// `CachedEmbeddingIndexGetter`.
pub struct CachedEmbeddingIndexGetter {
    jobs: Arc<dyn EmbeddingJobsStore>,
    downloader: Arc<dyn EmbeddingIndexDownloader>,
    cache: Mutex<IndexCache>,
    inflight: Mutex<HashMap<i32, Arc<tokio::sync::Mutex<()>>>>,
}

impl CachedEmbeddingIndexGetter {
    pub fn new(
        jobs: Arc<dyn EmbeddingJobsStore>,
        downloader: Arc<dyn EmbeddingIndexDownloader>,
        cache_size_bytes: u64,
    ) -> Self {
        Self {
            jobs,
            downloader,
            cache: Mutex::new(IndexCache::new(cache_size_bytes)),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    fn cached_is_current(&self, repo_id: i32, finished_at: OffsetDateTime) -> Option<Arc<StoredRepoEmbeddingIndex>> {
        let mut cache = self.cache.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = cache.get(repo_id)?;
        if finished_at > entry.finished_at {
            return None;
        }
        Some(Arc::clone(&entry.index))
    }

    fn store(&self, repo_id: i32, index: Arc<StoredRepoEmbeddingIndex>, finished_at: OffsetDateTime) {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .add(repo_id, index, finished_at);
    }

    /// `CachedEmbeddingIndexGetter.Get`. The singleflight key is the repo id;
    /// context cancellation is not modelled because the handler has no
    /// per-request cancellation signal.
    pub async fn get(
        &self,
        repo_id: i32,
        repo_name: &str,
    ) -> Result<Arc<StoredRepoEmbeddingIndex>, EmbeddingError> {
        let finished_at = self.jobs.get_last_completed(repo_id).await?;
        if let Some(index) = self.cached_is_current(repo_id, finished_at) {
            return Ok(index);
        }

        let key_lock = {
            let mut inflight = self
                .inflight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(
                inflight
                    .entry(repo_id)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        let _guard = key_lock.lock().await;

        // Another request may have populated the cache while we waited.
        if let Some(index) = self.cached_is_current(repo_id, finished_at) {
            return Ok(index);
        }

        let index = self
            .downloader
            .download(repo_id, repo_name)
            .await
            .map_err(|err| {
                EmbeddingError::other(format!("downloading repo embedding index: {err}"))
            })?;
        let index = Arc::new(index);
        self.store(repo_id, Arc::clone(&index), finished_at);
        Ok(index)
    }
}

/// The production [`EmbeddingBackend`]: live query client + cached index
/// download + similarity search.
pub struct ProductionBackend {
    query: Arc<dyn QueryEmbeddingSource>,
    indexes: Arc<CachedEmbeddingIndexGetter>,
}

impl ProductionBackend {
    pub fn new(
        query: Arc<dyn QueryEmbeddingSource>,
        indexes: Arc<CachedEmbeddingIndexGetter>,
    ) -> Self {
        Self { query, indexes }
    }
}

#[async_trait]
impl EmbeddingBackend for ProductionBackend {
    async fn get_query_embedding(
        &self,
        query: &str,
    ) -> Result<(Vec<f32>, String), EmbeddingError> {
        self.query.get_query_embedding(query).await
    }

    async fn get_repo_embedding_index(
        &self,
        repo_id: i32,
        repo_name: &str,
        query_embedding: &[f32],
        code_results_count: i64,
        text_results_count: i64,
        use_document_ranks: bool,
    ) -> Result<RepoEmbeddingIndex, EmbeddingError> {
        let index = self.indexes.get(repo_id, repo_name).await?;
        let query = quantize(query_embedding);
        Ok(RepoEmbeddingIndex {
            embeddings_model: index.embeddings_model.clone(),
            repo_name: index.repo_name.clone(),
            revision: index.revision.clone(),
            code_results: index.code_index.similarity_search(
                &query,
                code_results_count,
                use_document_ranks,
                &index.repo_name,
                &index.revision,
            ),
            text_results: index.text_index.similarity_search(
                &query,
                text_results_count,
                use_document_ranks,
                &index.repo_name,
                &index.revision,
            ),
        })
    }
}

/// `embeddings.NewObjectStorage`'s normalized storage config.
pub fn object_storage_config(config: &Config) -> StorageConfig {
    StorageConfig {
        backend: config.backend.clone(),
        manage_bucket: config.manage_bucket,
        bucket: config.bucket.clone(),
        s3: S3Config {
            is_blobstore: false,
            region: config.s3_region.clone(),
            endpoint: config.s3_endpoint.clone(),
            use_path_style: config.s3_use_path_style,
            access_key_id: config.s3_access_key_id.clone(),
            secret_access_key: config.s3_secret_access_key.clone(),
            session_token: config.s3_session_token.clone().unwrap_or_default(),
        },
        gcs: GcsConfig {
            project_id: config.gcs_project_id.clone(),
            credentials_file: config.gcs_credentials_file.clone().unwrap_or_default(),
            credentials_file_contents: config
                .gcs_credentials_file_contents
                .clone()
                .unwrap_or_default(),
        },
    }
    .normalize()
}

/// Read the site configuration the process resolves against. The baseline
/// reads the live configuration service; the standalone slice supports the
/// same `SITE_CONFIG_FILE` override and an inline JSON document for tests.
pub fn site_configuration() -> SiteConfiguration {
    if let Ok(contents) = std::env::var("SITE_CONFIG_FILE") {
        if let Ok(site) = SiteConfiguration::parse(&contents) {
            return site;
        }
    }
    if let Ok(contents) = std::env::var("SITE_CONFIG") {
        if let Ok(site) = SiteConfiguration::parse(&contents) {
            return site;
        }
    }
    SiteConfiguration::parse("{}").expect("empty site config parses")
}

/// The production `getQueryEmbedding` seam with the default HTTP transport.
pub fn live_query_embedding(site: SiteConfiguration) -> Arc<dyn QueryEmbeddingSource> {
    Arc::new(LiveQueryEmbedding::new(
        site,
        force_allow_embeddings(),
        Arc::new(SgEmbeddingsClientFactory {
            transport: Arc::new(sg_embeddings::ReqwestTransport::new()),
        }),
    ))
}

/// Build the frontend PG DSN from the standard `PG*` variables, matching the
/// baseline's `new_postgres_dsn` for the default deployment.
pub fn postgres_dsn(getenv: &dyn Fn(&str) -> Option<String>) -> Result<String, String> {
    let host = getenv("PGHOST").filter(|value| !value.is_empty());
    let Some(host) = host else {
        return Err("PGHOST is not set".to_owned());
    };
    let port = getenv("PGPORT")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "5432".to_owned());
    let user = getenv("PGUSER")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "postgres".to_owned());
    let password = getenv("PGPASSWORD").unwrap_or_default();
    let database = getenv("PGDATABASE")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "postgres".to_owned());
    let sslmode = getenv("PGSSLMODE")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "disable".to_owned());
    let auth = if password.is_empty() {
        user
    } else {
        format!("{user}:{password}")
    };
    Ok(format!(
        "postgres://{auth}@{host}:{port}/{database}?sslmode={sslmode}"
    ))
}

/// Poll interval helper kept for callers that want the baseline's job-store
/// freshness semantics in tests.
pub const JOB_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeJobs {
        finished: Mutex<HashMap<i32, Result<OffsetDateTime, EmbeddingError>>>,
        calls: Mutex<Vec<i32>>,
    }

    #[async_trait]
    impl EmbeddingJobsStore for FakeJobs {
        async fn get_last_completed(
            &self,
            repo_id: i32,
        ) -> Result<OffsetDateTime, EmbeddingError> {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(repo_id);
            match self
                .finished
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&repo_id)
                .cloned()
            {
                Some(result) => result,
                None => Err(EmbeddingError::not_found(format!(
                    "repo embedding job not found: repoID={repo_id}"
                ))),
            }
        }
    }

    #[derive(Default)]
    struct FakeDownloader {
        indexes: Mutex<HashMap<i32, Result<StoredRepoEmbeddingIndex, String>>>,
        calls: Mutex<Vec<(i32, String)>>,
    }

    #[async_trait]
    impl EmbeddingIndexDownloader for FakeDownloader {
        async fn download(
            &self,
            repo_id: i32,
            repo_name: &str,
        ) -> Result<StoredRepoEmbeddingIndex, String> {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((repo_id, repo_name.to_owned()));
            self.indexes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&repo_id)
                .cloned()
                .unwrap_or_else(|| Err(format!("no fixture for {repo_id}")))
        }
    }

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
    }

    fn index(model: &str, embeddings: Vec<i8>) -> StoredRepoEmbeddingIndex {
        StoredRepoEmbeddingIndex {
            repo_name: "repo".to_owned(),
            revision: "rev".to_owned(),
            embeddings_model: model.to_owned(),
            code_index: crate::index::EmbeddingIndex {
                embeddings,
                column_dimension: 2,
                row_metadata: vec![crate::index::RowMetadata {
                    file_name: "a.go".to_owned(),
                    start_line: 0,
                    end_line: 1,
                }],
                ranks: vec![0.0],
            },
            text_index: Default::default(),
        }
    }

    async fn getter(
        jobs: Arc<FakeJobs>,
        downloader: Arc<FakeDownloader>,
        cache_size: u64,
    ) -> CachedEmbeddingIndexGetter {
        CachedEmbeddingIndexGetter::new(jobs, downloader, cache_size)
    }

    #[tokio::test]
    async fn cache_returns_the_cached_index_until_a_newer_job_finishes() {
        let jobs = Arc::new(FakeJobs::default());
        let downloader = Arc::new(FakeDownloader::default());
        jobs.finished
            .lock()
            .unwrap()
            .insert(7, Ok(at(100)));
        downloader
            .indexes
            .lock()
            .unwrap()
            .insert(7, Ok(index("m1", vec![1, 2])));

        let getter = getter(Arc::clone(&jobs), Arc::clone(&downloader), 1_000).await;
        let first = getter.get(7, "repo").await.expect("first");
        assert_eq!(first.embeddings_model, "m1");
        let second = getter.get(7, "repo").await.expect("second");
        assert_eq!(second.embeddings_model, "m1");
        assert_eq!(*downloader.calls.lock().unwrap(), vec![(7, "repo".to_owned())]);

        jobs.finished.lock().unwrap().insert(7, Ok(at(101)));
        downloader
            .indexes
            .lock()
            .unwrap()
            .insert(7, Ok(index("m2", vec![1, 2])));
        let third = getter.get(7, "repo").await.expect("third");
        assert_eq!(third.embeddings_model, "m2");
        assert_eq!(downloader.calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn cache_does_not_store_indexes_larger_than_the_budget() {
        let jobs = Arc::new(FakeJobs::default());
        let downloader = Arc::new(FakeDownloader::default());
        jobs.finished.lock().unwrap().insert(1, Ok(at(1)));
        downloader
            .indexes
            .lock()
            .unwrap()
            .insert(1, Ok(index("m", vec![1, 2])));
        let getter = getter(Arc::clone(&jobs), Arc::clone(&downloader), 1).await;
        let _ = getter.get(1, "repo").await.expect("download");
        let _ = getter.get(1, "repo").await.expect("download again");
        assert_eq!(downloader.calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn missing_job_is_not_found_and_never_downloads() {
        let jobs = Arc::new(FakeJobs::default());
        let downloader = Arc::new(FakeDownloader::default());
        let getter = getter(Arc::clone(&jobs), Arc::clone(&downloader), 1_000).await;
        let error = getter.get(3, "repo").await.expect_err("missing");
        assert!(error.not_found);
        assert_eq!(error.message, "repo embedding job not found: repoID=3");
        assert!(downloader.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn download_errors_are_wrapped_like_the_baseline() {
        let jobs = Arc::new(FakeJobs::default());
        let downloader = Arc::new(FakeDownloader::default());
        jobs.finished.lock().unwrap().insert(3, Ok(at(1)));
        downloader
            .indexes
            .lock()
            .unwrap()
            .insert(3, Err("failed to get object: NoSuchKey".to_owned()));
        let getter = getter(Arc::clone(&jobs), Arc::clone(&downloader), 1_000).await;
        let error = getter.get(3, "repo").await.expect_err("download");
        assert!(!error.not_found);
        assert_eq!(
            error.message,
            "downloading repo embedding index: failed to get object: NoSuchKey"
        );
    }

    #[test]
    fn provider_config_gate_matches_the_baseline() {
        let parse = |json: &str| SiteConfiguration::parse(json).expect("site");
        assert_eq!(
            embeddings_provider_config(&parse(r#"{"cody.enabled":true}"#), true)
                .expect("configured")
                .provider,
            sg_embeddings::Provider::Sourcegraph
        );
        assert_eq!(
            embeddings_provider_config(&parse(r#"{"cody.enabled":true}"#), false),
            None
        );
        assert_eq!(
            embeddings_provider_config(&parse(r#"{"embeddings":{"enabled":false}}"#), true),
            None
        );
        assert_eq!(
            embeddings_provider_config(
                &parse(r#"{"cody.enabled":true,"embeddings":{"provider":"openai"}}"#),
                true
            ),
            None
        );
        let azure = embeddings_provider_config(
            &parse(
                r#"{"cody.enabled":true,"embeddings":{"provider":"azure-openai","endpoint":"https://x/","model":"MY-DEPLOY"}}"#,
            ),
            true,
        )
        .expect("azure");
        assert_eq!(azure.model, "my-deploy");
        assert_eq!(azure.access_token, "");
        assert_eq!(azure.dimensions, 0);
    }
}
