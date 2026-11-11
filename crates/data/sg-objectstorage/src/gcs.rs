//! GCS object storage (`internal/object/gcs_client.go`, `gcs_api.go`,
//! `pool.go`).
//!
//! The Go baseline wraps `cloud.google.com/go/storage`; this port speaks the
//! same GCS JSON API (and the XML download path for reads) over an injectable
//! [`GcsTransport`], so tests drive a fake server without touching the
//! network.
//!
//! Request shapes reproduced from the baseline's HTTP transport:
//!
//! - upload: `multipart/related` `uploadType=multipart` for bodies smaller than
//!   the 16 MiB chunk size, a `uploadType=resumable` session followed by
//!   `POST` chunk requests for bodies at or above it (the exact boundary
//!   matches `gensupport.MediaBuffer`: a full chunk is never the last one, so
//!   an exactly-chunk-sized upload ends with a zero-byte final chunk);
//! - get: `GET /{bucket}/{object}` (the XML download path);
//! - list: `GET /storage/v1/b/{bucket}/o` with the same query surface the
//!   generated `storage.Query` emits (prefix, pagination, field selection);
//! - delete: `DELETE /storage/v1/b/{bucket}/o/{object}`;
//! - compose: `POST /storage/v1/b/{bucket}/o/{dest}/compose`, with the
//!   baseline's 32-source sharding (`composerShim.Run`);
//! - init: `GET /storage/v1/b/{bucket}` then, on 404, `POST /storage/v1/b`.
//!
//! Errors mirror the baseline's wrap prefixes and retry classification:
//! idempotent operations retry on 408/429/5xx (and transport failures) with
//! exponential backoff; writes do not.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// `Writer.ChunkSize` default in `cloud.google.com/go/storage` (16 MiB).
pub const GCS_DEFAULT_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// The GCS JSON API base URL used by production.
pub const GCS_PRODUCTION_ENDPOINT: &str = "https://storage.googleapis.com";

/// `object.GCSConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcsConfig {
    pub project_id: String,
    pub credentials_file: String,
    pub credentials_file_contents: String,
}

/// A normalized error. `not_found` encodes the baseline's
/// `storage.ErrObjectNotExist` / `ErrBucketNotExist` / `googleapi.Error` 404
/// classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsError {
    message: String,
    not_found: bool,
}

impl GcsError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: true,
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.not_found
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for GcsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GcsError {}

/// A raw HTTP request handed to a [`GcsTransport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// A raw HTTP response returned by a [`GcsTransport`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcsHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl GcsHttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn is_success(&self) -> bool {
        (200..=299).contains(&self.status)
    }
}

/// The injectable HTTP execution seam. Production uses
/// [`ReqwestGcsTransport`]; tests inject an in-memory fake.
#[async_trait]
pub trait GcsTransport: Send + Sync {
    async fn execute(&self, request: GcsHttpRequest) -> Result<GcsHttpResponse, String>;
}

/// Production transport.
pub struct ReqwestGcsTransport {
    client: reqwest::Client,
}

impl ReqwestGcsTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestGcsTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GcsTransport for ReqwestGcsTransport {
    async fn execute(&self, request: GcsHttpRequest) -> Result<GcsHttpResponse, String> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|err| err.to_string())?;
        let mut builder = self.client.request(method, &request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .body(request.body)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|err| err.to_string())?
            .to_vec();
        Ok(GcsHttpResponse {
            status,
            headers,
            body,
        })
    }
}

/// `google_cloud_auth` access-token source for the production store.
pub struct GcsTokenProvider {
    credentials: google_cloud_auth::credentials::AccessTokenCredentials,
}

impl GcsTokenProvider {
    /// `gcsClientOptions`: `WithCredentialsFile`, `WithCredentialsJSON`, or
    /// Application Default Credentials.
    pub fn from_config(config: &GcsConfig) -> Result<Self, GcsError> {
        use google_cloud_auth::credentials::service_account;

        if !config.credentials_file.is_empty() {
            let data = std::fs::read(&config.credentials_file).map_err(|err| {
                GcsError::other(format!(
                    "reading credentials file {:?}: {err}",
                    config.credentials_file
                ))
            })?;
            let key: serde_json::Value = serde_json::from_slice(&data).map_err(|err| {
                GcsError::other(format!(
                    "parsing credentials file {:?}: {err}",
                    config.credentials_file
                ))
            })?;
            let credentials = service_account::Builder::new(key)
                .build_access_token_credentials()
                .map_err(|err| GcsError::other(format!("creating credentials: {err}")))?;
            return Ok(Self { credentials });
        }

        if !config.credentials_file_contents.is_empty() {
            let key: serde_json::Value = serde_json::from_str(&config.credentials_file_contents)
                .map_err(|err| GcsError::other(format!("parsing credentials contents: {err}")))?;
            let credentials = service_account::Builder::new(key)
                .build_access_token_credentials()
                .map_err(|err| GcsError::other(format!("creating credentials: {err}")))?;
            return Ok(Self { credentials });
        }

        let credentials = google_cloud_auth::credentials::Builder::default()
            .build_access_token_credentials()
            .map_err(|err| GcsError::other(format!("creating credentials: {err}")))?;
        Ok(Self { credentials })
    }

    pub async fn bearer(&self) -> Result<String, GcsError> {
        self.credentials
            .access_token()
            .await
            .map(|token| token.token)
            .map_err(|err| GcsError::other(format!("fetching access token: {err}")))
    }
}

/// Retry/backoff policy for idempotent calls. The baseline uses gax's default
/// backoff (1s initial, doubling, 30s cap) with no attempt limit; the cap here
/// only bounds runaway retries.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
    pub max_attempts: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: 32,
        }
    }
}

impl RetryPolicy {
    /// No backoff, used by determinism tests.
    pub fn immediate() -> Self {
        Self {
            initial: Duration::ZERO,
            max: Duration::ZERO,
            multiplier: 1.0,
            max_attempts: 8,
        }
    }
}

/// A GCS-backed store (`object.gcsStore`).
#[derive(Clone)]
pub struct GcsStore {
    bucket: String,
    manage_bucket: bool,
    project_id: String,
    endpoint: String,
    transport: Arc<dyn GcsTransport>,
    token: Option<Arc<GcsTokenProvider>>,
    retry: RetryPolicy,
}

impl GcsStore {
    /// `newGCSFromConfig` equivalent: production endpoint, real transport and
    /// Application Default Credentials (or the configured service account).
    pub fn new(config: super::StorageConfig) -> Result<Self, GcsError> {
        let token = GcsTokenProvider::from_config(&config.gcs)?;
        Ok(Self {
            bucket: config.bucket,
            manage_bucket: config.manage_bucket,
            project_id: config.gcs.project_id,
            endpoint: GCS_PRODUCTION_ENDPOINT.to_owned(),
            transport: Arc::new(ReqwestGcsTransport::new()),
            token: Some(Arc::new(token)),
            retry: RetryPolicy::default(),
        })
    }

    /// Build from an injected transport (tests, alternative auth).
    pub fn with_transport(
        bucket: impl Into<String>,
        manage_bucket: bool,
        project_id: impl Into<String>,
        endpoint: impl Into<String>,
        transport: Arc<dyn GcsTransport>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            manage_bucket,
            project_id: project_id.into(),
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            transport,
            token: None,
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn manage_bucket(&self) -> bool {
        self.manage_bucket
    }

    // ------------------------------------------------------------------
    // object.Storage
    // ------------------------------------------------------------------

    /// `gcsStore.Init`.
    pub async fn init(&self) -> Result<(), GcsError> {
        if !self.manage_bucket {
            return Ok(());
        }

        match self.bucket_attrs().await {
            Ok(()) => Ok(()),
            Err(err) if err.is_not_found() => self
                .create_bucket()
                .await
                .map_err(|err| GcsError::other(format!("failed to create bucket: {err}"))),
            Err(err) => Err(GcsError::other(format!(
                "failed to get bucket attributes: {err}"
            ))),
        }
    }

    /// `gcsStore.Get`. The baseline hands back an `io.ReadCloser`; this port
    /// materializes the body because the worker seams take bytes.
    pub async fn get(&self, key: &str) -> Result<Vec<u8>, GcsError> {
        let url = format!("{}/{}/{}", self.endpoint, self.bucket, path_escape(key));
        let response = self
            // `storage.ObjectHandle.NewReader` retries idempotent reads.
            .send(true, "GET", url, Vec::new(), Vec::new())
            .await
            .map_err(|err| GcsError::other(format!("failed to get object: {err}")))?;
        if response.status == 404 {
            return Err(GcsError::not_found("failed to get object: not found"));
        }
        if !response.is_success() {
            return Err(GcsError::other(format!(
                "failed to get object: {}",
                status_error(&response)
            )));
        }
        Ok(response.body)
    }

    /// `gcsStore.Upload`; returns the number of bytes written.
    pub async fn upload(&self, key: &str, data: &[u8]) -> Result<i64, GcsError> {
        if data.len() < GCS_DEFAULT_CHUNK_SIZE {
            self.upload_multipart(key, data).await
        } else {
            self.upload_resumable(key, data).await
        }
    }

    /// `gcsStore.Delete`.
    pub async fn delete(&self, key: &str) -> Result<(), GcsError> {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=json&prettyPrint=false",
            self.endpoint,
            self.bucket,
            path_escape(key)
        );
        // `storage` retries reads (get/list/attrs) but sends deletes exactly
        // once; the oracle confirms no retry on a 503.
        let response = self
            .send(false, "DELETE", url, Vec::new(), Vec::new())
            .await
            .map_err(|err| GcsError::other(format!("failed to delete object: {err}")))?;
        if response.status == 404 {
            return Err(GcsError::not_found("failed to delete object: not found"));
        }
        if !response.is_success() {
            return Err(GcsError::other(format!(
                "failed to delete object: {}",
                status_error(&response)
            )));
        }
        Ok(())
    }

    /// `gcsStore.List` materialized: every key with the prefix. On a page
    /// error the keys read so far are dropped; [`Self::list_objects_partial`]
    /// exposes the baseline iterator's partial-results behaviour.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, GcsError> {
        match self.list_objects_partial(prefix).await {
            (_, Some(err)) => Err(err),
            (keys, None) => Ok(keys),
        }
    }

    /// The iterator semantics of `gcsStore.List`: previously returned keys
    /// survive a later page error.
    pub async fn list_objects_partial(&self, prefix: &str) -> (Vec<String>, Option<GcsError>) {
        let (items, err) = self
            .list_impl(prefix, "nextPageToken,prefixes,items(name)", false)
            .await;
        (items.into_iter().map(|item| item.key).collect(), err)
    }

    /// `storage.Query{Delimiter: ...}` support. Returns the object names and
    /// the delimiter prefixes in server order.
    pub async fn list_objects_with_delimiter(
        &self,
        prefix: &str,
        delimiter: &str,
    ) -> (Vec<String>, Vec<String>, Option<GcsError>) {
        let (items, prefixes, err) = self
            .list_impl_with_delimiter(
                prefix,
                "nextPageToken,prefixes,items(name)",
                delimiter,
                false,
            )
            .await;
        (
            items.into_iter().map(|item| item.key).collect(),
            prefixes,
            err,
        )
    }

    /// `gcsStore.Compose`: concatenates `sources` into `destination`, deleting
    /// the sources best-effort after success, exactly like the baseline
    /// (including the 32-source shard limit in `composerShim.Run`).
    pub async fn compose(&self, destination: &str, sources: &[String]) -> Result<i64, GcsError> {
        if sources.is_empty() {
            return Err(GcsError::other(
                "failed to compose objects: storage: at least one source object must be specified",
            ));
        }

        let mut working: Vec<String> = sources.to_vec();
        while working.len() > 32 {
            self.compose_one(destination, &working[..32]).await?;
            let mut next = Vec::with_capacity(working.len() - 31);
            next.push(destination.to_owned());
            next.extend_from_slice(&working[32..]);
            working = next;
        }
        let size = self.compose_one(destination, &working).await?;

        // Delete the original sources on success; failures are logged and do
        // not change the result (baseline `deleteSources`).
        let this = self.clone();
        let sources = sources.to_vec();
        let _ = for_each_string(sources, move |_, source| {
            let this = this.clone();
            async move {
                if let Err(err) = this.delete(&source).await {
                    tracing::error!(error = %err, source = %source, "Failed to delete source object");
                }
                Ok(())
            }
        })
        .await;

        Ok(size)
    }

    /// `gcsStore.ExpireObjects`: delete every object under `prefix` older than
    /// `max_age`. Pagination and deletion errors are logged, never returned
    /// (the routine retries on its next run).
    pub async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), GcsError> {
        let (items, err) = self
            .list_impl(
                prefix,
                "nextPageToken,prefixes,items(name,timeCreated)",
                true,
            )
            .await;
        if let Some(err) = err {
            tracing::error!(error = %err, bucket = %self.bucket, "Failed to iterate GCS bucket");
            return Ok(());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        for item in items {
            let Some(created) = item.last_modified_secs else {
                continue;
            };
            if now.saturating_sub(created) >= max_age.as_secs() {
                if let Err(err) = self.delete(&item.key).await {
                    tracing::error!(error = %err, bucket = %self.bucket, object = %item.key, "Failed to delete expired GCS object");
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    async fn bucket_attrs(&self) -> Result<(), GcsError> {
        let url = format!(
            "{}/storage/v1/b/{}?alt=json&prettyPrint=false&projection=full",
            self.endpoint, self.bucket
        );
        let response = self.send(true, "GET", url, Vec::new(), Vec::new()).await?;
        if response.status == 404 {
            return Err(GcsError::not_found("bucket not found"));
        }
        if !response.is_success() {
            return Err(GcsError::other(status_error(&response)));
        }
        Ok(())
    }

    async fn create_bucket(&self) -> Result<(), GcsError> {
        let url = format!(
            "{}/storage/v1/b?alt=json&prettyPrint=false&project={}",
            self.endpoint,
            query_escape(&self.project_id)
        );
        let body = serde_json::to_vec(&serde_json::json!({ "name": self.bucket }))
            .map_err(|err| GcsError::other(err.to_string()))?;
        let headers = vec![("Content-Type".to_owned(), "application/json".to_owned())];
        let response = self.send(false, "POST", url, headers, body).await?;
        if !response.is_success() {
            return Err(GcsError::other(status_error(&response)));
        }
        Ok(())
    }

    async fn upload_multipart(&self, key: &str, data: &[u8]) -> Result<i64, GcsError> {
        let boundary = next_boundary();
        let metadata = serde_json::to_vec(&serde_json::json!({
            "bucket": self.bucket,
            "name": key,
        }))
        .map_err(|err| GcsError::other(err.to_string()))?;
        let content_type = sniff_content_type(data);
        let mut body = Vec::with_capacity(data.len() + metadata.len() + 256);
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Type: application/json\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(&metadata);
        body.extend_from_slice(
            format!("\r\n--{boundary}\r\nContent-Type: {content_type}\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let url = format!(
            "{}/upload/storage/v1/b/{}/o?alt=json&name={}&prettyPrint=false&projection=full&uploadType=multipart",
            self.endpoint,
            self.bucket,
            query_escape(key)
        );
        let headers = vec![(
            "Content-Type".to_owned(),
            format!("multipart/related; boundary={boundary}"),
        )];
        let response = self
            .send(false, "POST", url, headers, body)
            .await
            .map_err(|err| GcsError::other(format!("failed to upload object: {err}")))?;
        if !response.is_success() {
            return Err(GcsError::other(format!(
                "failed to upload object: {}",
                status_error(&response)
            )));
        }
        Ok(data.len() as i64)
    }

    async fn upload_resumable(&self, key: &str, data: &[u8]) -> Result<i64, GcsError> {
        let content_type = sniff_content_type(data);
        let metadata = serde_json::to_vec(&serde_json::json!({
            "bucket": self.bucket,
            "name": key,
        }))
        .map_err(|err| GcsError::other(err.to_string()))?;
        let url = format!(
            "{}/upload/storage/v1/b/{}/o?alt=json&name={}&prettyPrint=false&projection=full&uploadType=resumable",
            self.endpoint,
            self.bucket,
            query_escape(key)
        );
        let headers = vec![
            ("Content-Type".to_owned(), "application/json".to_owned()),
            ("X-Upload-Content-Type".to_owned(), content_type.to_owned()),
        ];
        let response = self
            .send(false, "POST", url, headers, metadata)
            .await
            .map_err(|err| GcsError::other(format!("failed to upload object: {err}")))?;
        if !response.is_success() {
            return Err(GcsError::other(format!(
                "failed to upload object: {}",
                status_error(&response)
            )));
        }
        let location = response
            .header("Location")
            .ok_or_else(|| GcsError::other("failed to upload object: missing upload location"))
            .map_err(|err| GcsError::other(format!("{err}")))?;
        let location = if location.starts_with("http") {
            location.to_owned()
        } else {
            format!("{}{}", self.endpoint, location)
        };

        let total = data.len();
        let mut offset = 0usize;
        loop {
            let len = (total - offset).min(GCS_DEFAULT_CHUNK_SIZE);
            let final_chunk = len < GCS_DEFAULT_CHUNK_SIZE;
            let content_range = if final_chunk {
                if len == 0 {
                    format!("bytes */{total}")
                } else {
                    format!("bytes {}-{}/{total}", offset, offset + len - 1)
                }
            } else {
                format!("bytes {}-{}/*", offset, offset + len - 1)
            };
            let headers = vec![
                ("Content-Type".to_owned(), content_type.to_owned()),
                ("Content-Range".to_owned(), content_range),
            ];
            let chunk = data[offset..offset + len].to_vec();
            let response = self
                .send(false, "POST", location.clone(), headers, chunk)
                .await
                .map_err(|err| GcsError::other(format!("failed to upload object: {err}")))?;
            if !response.is_success() {
                return Err(GcsError::other(format!(
                    "failed to upload object: {}",
                    status_error(&response)
                )));
            }
            offset += len;
            if final_chunk {
                break;
            }
        }
        Ok(total as i64)
    }

    async fn compose_one(&self, destination: &str, sources: &[String]) -> Result<i64, GcsError> {
        #[derive(Serialize)]
        struct Destination<'a> {
            bucket: &'a str,
        }
        #[derive(Serialize)]
        struct Source<'a> {
            name: &'a str,
        }
        #[derive(Serialize)]
        struct ComposeRequest<'a> {
            destination: Destination<'a>,
            #[serde(rename = "sourceObjects")]
            source_objects: Vec<Source<'a>>,
        }

        let payload = ComposeRequest {
            destination: Destination {
                bucket: &self.bucket,
            },
            source_objects: sources
                .iter()
                .map(|source| Source { name: source })
                .collect(),
        };
        let body = serde_json::to_vec(&payload).map_err(|err| GcsError::other(err.to_string()))?;
        let url = format!(
            "{}/storage/v1/b/{}/o/{}/compose?alt=json&prettyPrint=false",
            self.endpoint,
            self.bucket,
            path_escape(destination)
        );
        let headers = vec![("Content-Type".to_owned(), "application/json".to_owned())];
        let response = self
            .send(false, "POST", url, headers, body)
            .await
            .map_err(|err| GcsError::other(format!("failed to compose objects: {err}")))?;
        if !response.is_success() {
            return Err(GcsError::other(format!(
                "failed to compose objects: {}",
                status_error(&response)
            )));
        }
        let attrs: ComposeResponse = serde_json::from_slice(&response.body).map_err(|err| {
            GcsError::other(format!(
                "failed to compose objects: decoding response: {err}"
            ))
        })?;
        Ok(attrs.size_value())
    }

    async fn list_impl(
        &self,
        prefix: &str,
        fields: &str,
        with_created: bool,
    ) -> (Vec<ListItem>, Option<GcsError>) {
        let (items, _prefixes, err) = self
            .list_impl_with_delimiter(prefix, fields, "", with_created)
            .await;
        (items, err)
    }

    async fn list_impl_with_delimiter(
        &self,
        prefix: &str,
        fields: &str,
        delimiter: &str,
        with_created: bool,
    ) -> (Vec<ListItem>, Vec<String>, Option<GcsError>) {
        let mut items: Vec<ListItem> = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut page_token = String::new();
        loop {
            let url = format!(
                "{}/storage/v1/b/{}/o?alt=json&delimiter={}&endOffset=&fields={}&includeFoldersAsPrefixes=false&includeTrailingDelimiter=false&matchGlob=&pageToken={}&prefix={}&prettyPrint=false&projection=full&startOffset=&versions=false",
                self.endpoint,
                self.bucket,
                query_escape(delimiter),
                query_escape(fields),
                query_escape(&page_token),
                query_escape(prefix)
            );
            let response = match self.send(true, "GET", url, Vec::new(), Vec::new()).await {
                Ok(response) => response,
                Err(err) => return (items, prefixes, Some(GcsError::other(err.to_string()))),
            };
            if response.status == 404 {
                return (
                    items,
                    prefixes,
                    Some(GcsError::not_found("bucket not found")),
                );
            }
            if !response.is_success() {
                return (
                    items,
                    prefixes,
                    Some(GcsError::other(status_error(&response))),
                );
            }
            let page: ListResponse = match serde_json::from_slice(&response.body) {
                Ok(page) => page,
                Err(err) => {
                    return (
                        items,
                        prefixes,
                        Some(GcsError::other(format!("decoding list response: {err}"))),
                    )
                }
            };
            for item in page.items {
                items.push(ListItem {
                    key: item.name,
                    last_modified_secs: if with_created {
                        item.time_created.as_deref().and_then(parse_rfc3339_secs)
                    } else {
                        None
                    },
                });
            }
            prefixes.extend(page.prefixes);
            match page.next_page_token {
                Some(token) if !token.is_empty() => page_token = token,
                _ => break,
            }
        }
        (items, prefixes, None)
    }

    async fn send(
        &self,
        idempotent: bool,
        method: &str,
        url: String,
        mut headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<GcsHttpResponse, GcsError> {
        if let Some(token) = &self.token {
            let bearer = token.bearer().await?;
            headers.push(("Authorization".to_owned(), format!("Bearer {bearer}")));
        }

        let mut attempt: u32 = 0;
        let mut delay = self.retry.initial;
        loop {
            let request = GcsHttpRequest {
                method: method.to_owned(),
                url: url.clone(),
                headers: headers.clone(),
                body: body.clone(),
            };
            match self.transport.execute(request).await {
                Ok(response) => {
                    if idempotent
                        && is_retryable_status(response.status)
                        && attempt + 1 < self.retry.max_attempts
                    {
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        delay = next_delay(delay, self.retry);
                        continue;
                    }
                    return Ok(response);
                }
                Err(err) => {
                    if idempotent && attempt + 1 < self.retry.max_attempts {
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        delay = next_delay(delay, self.retry);
                        continue;
                    }
                    return Err(GcsError::other(err));
                }
            }
        }
    }
}

fn next_delay(current: Duration, retry: RetryPolicy) -> Duration {
    let next = current.mul_f64(retry.multiplier.max(1.0));
    if next > retry.max {
        retry.max
    } else {
        next
    }
}

fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..=599).contains(&status)
}

fn status_error(response: &GcsHttpResponse) -> String {
    format!(
        "googleapi: Error {}: {}",
        response.status,
        String::from_utf8_lossy(&response.body)
    )
}

/// `internal/object/pool.go`: run `f` for every value with at most
/// [`pool_limit`] callbacks in flight. Errors are joined with `\n` in
/// completion order, mirroring `conc.ErrorPool.Wait` (which uses
/// `errors.Join`).
pub async fn for_each_string<F, Fut>(values: Vec<String>, f: F) -> Result<(), String>
where
    F: Fn(usize, String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
{
    use tokio::task::JoinSet;

    let limit = pool_limit();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(limit));
    let function = Arc::new(f);
    let mut tasks = JoinSet::new();
    for (index, value) in values.into_iter().enumerate() {
        let permit = match Arc::clone(&semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let function = Arc::clone(&function);
        tasks.spawn(async move {
            let _permit = permit;
            function(index, value).await
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => errors.push(err),
            Err(err) => errors.push(err.to_string()),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

/// `runtime.GOMAXPROCS(0)` equivalent for the pool limit.
pub fn pool_limit() -> usize {
    std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListItem {
    key: String,
    last_modified_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct ListResponse {
    #[serde(default)]
    items: Vec<ListObject>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ListObject {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "timeCreated")]
    time_created: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ComposeResponse {
    #[serde(default)]
    size: Option<SizeValue>,
}

impl ComposeResponse {
    fn size_value(&self) -> i64 {
        match &self.size {
            Some(SizeValue::String(value)) => value.parse().unwrap_or(0),
            Some(SizeValue::Number(value)) => *value,
            None => 0,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SizeValue {
    String(String),
    Number(i64),
}

fn next_boundary() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let value = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sg-m9jz-boundary-{value:016x}")
}

/// `gax.DetermineContentType` / `net/http.DetectContentType` for the media
/// part of a multipart upload. The corpus only needs the text/binary split.
pub fn sniff_content_type(data: &[u8]) -> &'static str {
    if is_text(data) {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

fn is_text(data: &[u8]) -> bool {
    let sample = &data[..data.len().min(512)];
    for &byte in sample {
        if byte == 0x1b {
            return false;
        }
        if byte < 0x20 && !matches!(byte, b'\t' | b'\n' | 0x0c | b'\r') {
            return false;
        }
    }
    std::str::from_utf8(sample).is_ok()
}

/// `url.PathEscape` for a single path segment (slashes are escaped).
pub fn path_escape(value: &str) -> String {
    percent_escape(value, false)
}

/// `url.QueryEscape`.
pub fn query_escape(value: &str) -> String {
    percent_escape(value, true)
}

fn percent_escape(value: &str, plus_for_space: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            b' ' if plus_for_space => out.push('+'),
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// Minimal RFC 3339 parser for `timeCreated` (`YYYY-MM-DDTHH:MM:SS[.fff]Z`),
/// returning Unix seconds.
fn parse_rfc3339_secs(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    let year: i64 = value.get(0..4)?.parse().ok()?;
    let month: i64 = value.get(5..7)?.parse().ok()?;
    let day: i64 = value.get(8..10)?.parse().ok()?;
    let hour: i64 = value.get(11..13)?.parse().ok()?;
    let minute: i64 = value.get(14..16)?.parse().ok()?;
    let second: i64 = value.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Days since epoch (Howard Hinnant's civil_from_days inverse).
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let seconds = days * 86400 + hour * 3600 + minute * 60 + second;
    if seconds < 0 {
        None
    } else {
        Some(seconds as u64)
    }
}
