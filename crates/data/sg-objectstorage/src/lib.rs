#![forbid(unsafe_code)]

//! `sg-objectstorage`: S3/blobstore and GCS client used by the code-intel
//! upload processor.
//!
//! Mirrors `internal/object` from the Go baseline:
//!
//! - [`ObjectStore::new`] is `object.create`: the normalized backend selects
//!   the S3 (`s3`/`blobstore`) or GCS (`gcs`) implementation, and every
//!   storage operation dispatches to it ([`ObjectStore`] mirrors
//!   `object.Storage`).
//! - [`StorageConfig::normalize`] is `object.normalizeConfig`: the backend name
//!   is lowercased, and `blobstore` implies `IsBlobstore`, `ManageBucket` and
//!   `UsePathStyle`.
//! - [`ObjectStore::init`] is `s3Store.Init`: only creates the bucket when
//!   `manage_bucket` is set, and treats "already exists"/"owned by you" as
//!   success.
//! - [`ObjectStore::get`] is `s3Store.Get`: returns the object body streamed by
//!   the AWS SDK (the baseline pipes a `GetObject` response through an
//!   `io.Pipe`; the retry-on-connection-reset logic is documented as a
//!   difference, see `docs/modules/07_precise-code-intel-worker.md`).
//! - [`ObjectStore::delete`] is `s3Store.Delete`.
//! - [`ObjectStore::list_objects`] and [`ObjectStore::expire_objects`] are
//!   `ListObjectsV2` pagination and `s3Store.ExpireObjects` (used by the
//!   `codeintel-uploadstore-expirer` worker job).
//!
//! Only the operations the current workers need are implemented here.

pub mod gcs;

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;

/// Re-exported so callers can build request bodies without depending on the
/// AWS SDK directly (tests upload fixtures, future upload producers).
pub use aws_sdk_s3::primitives::ByteStream as ObjectByteStream;

/// `object.S3Config`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3Config {
    pub is_blobstore: bool,
    pub region: String,
    pub endpoint: String,
    pub use_path_style: bool,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

pub use gcs::{GcsConfig, GcsError, GcsHttpRequest, GcsHttpResponse, GcsStore, GcsTransport};

/// `object.StorageConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageConfig {
    pub backend: String,
    pub manage_bucket: bool,
    pub bucket: String,
    pub s3: S3Config,
    pub gcs: GcsConfig,
}

impl StorageConfig {
    /// `object.normalizeConfig`.
    pub fn normalize(mut self) -> Self {
        self.backend = self.backend.to_lowercase();
        if self.backend == "blobstore" {
            self.s3.is_blobstore = true;
            self.manage_bucket = true;
            self.s3.use_path_style = true;
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// `failed to create bucket: ...`
    CreateBucket(String),
    /// `failed to create bucket: ...` where the request could not be sent or
    /// no response arrived (`SdkError::DispatchFailure`/`TimeoutError`). The
    /// baseline classifies this as `smithyhttp.RequestSendError` and the
    /// precise worker retries it every 250ms; the display text is identical
    /// to [`Error::CreateBucket`].
    CreateBucketRequestSend(String),
    /// `failed to get object: ...`
    GetObject(String),
    /// `failed to upload object: ...`
    UploadObject(String),
    /// `failed to delete object: ...`
    DeleteObject(String),
    /// `failed to list objects: ...`
    ListObjects(String),
    /// `failed to delete objects: ...`
    DeleteObjects(String),
    /// A GCS backend error. The GCS store already applies the baseline wrap
    /// prefixes (`failed to get object: ...`), so it is rendered as-is.
    Gcs(String),
    /// `object.create`: an unregistered backend name.
    UnknownBackend(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateBucket(message) | Self::CreateBucketRequestSend(message) => {
                write!(f, "failed to create bucket: {message}")
            }
            Self::GetObject(message) => write!(f, "failed to get object: {message}"),
            Self::UploadObject(message) => write!(f, "failed to upload object: {message}"),
            Self::DeleteObject(message) => write!(f, "failed to delete object: {message}"),
            Self::ListObjects(message) => write!(f, "failed to list objects: {message}"),
            Self::DeleteObjects(message) => write!(f, "failed to delete objects: {message}"),
            Self::Gcs(message) => f.write_str(message),
            Self::UnknownBackend(backend) => {
                write!(f, "unknown upload store backend '{backend}'")
            }
        }
    }
}

impl Error {
    /// `errors.HasType[*smithyhttp.RequestSendError]`: the S3/blobstore
    /// `Init` request could not be sent or no HTTP response arrived. This is
    /// the only retryable `Init` failure in the baseline; HTTP/service errors
    /// and GCS failures are returned immediately by the caller.
    pub fn is_request_error(&self) -> bool {
        matches!(self, Self::CreateBucketRequestSend(_))
    }
}

impl std::error::Error for Error {}

/// The response of `object.Storage.Get`. `into_async_read` is the
/// `io.ReadCloser` equivalent.
pub enum ObjectBody {
    /// S3: the body streamed by the SDK and `GetObjectOutput.ContentLength`.
    S3 {
        body: ByteStream,
        content_length: Option<i64>,
    },
    /// GCS: the baseline hands back a reader; this port materialises the body.
    Gcs(Vec<u8>),
}

impl ObjectBody {
    /// `GetObjectOutput.ContentLength`; absent when the server omits it.
    pub fn content_length(&self) -> Option<i64> {
        match self {
            Self::S3 { content_length, .. } => *content_length,
            Self::Gcs(data) => Some(data.len() as i64),
        }
    }

    pub fn into_byte_stream(self) -> ByteStream {
        match self {
            Self::S3 { body, .. } => body,
            Self::Gcs(data) => ByteStream::from(data),
        }
    }

    /// Convert into a [`tokio::io::AsyncRead`].
    pub fn into_async_read(self) -> std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> {
        match self {
            Self::S3 { body, .. } => Box::pin(body.into_async_read()),
            Self::Gcs(data) => Box::pin(std::io::Cursor::new(data)),
        }
    }
}

/// An S3-compatible store (`object.s3Store`).
pub struct S3Store {
    client: Client,
    bucket: String,
    manage_bucket: bool,
}

impl S3Store {
    /// `newS3FromConfig`.
    pub async fn new(config: StorageConfig) -> Result<Self, Error> {
        let config = config.normalize();
        let client = build_client(&config.s3).await;
        Ok(Self {
            client,
            bucket: config.bucket,
            manage_bucket: config.manage_bucket,
        })
    }

    /// Build from an already configured SDK client (tests, other callers).
    pub fn with_client(client: Client, bucket: impl Into<String>, manage_bucket: bool) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            manage_bucket,
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// `s3Store.Init`.
    pub async fn init(&self) -> Result<(), Error> {
        if !self.manage_bucket {
            return Ok(());
        }

        match self
            .client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) if is_bucket_already_exists(&err) => Ok(()),
            Err(err) if is_request_send_error(&err) => {
                Err(Error::CreateBucketRequestSend(err.to_string()))
            }
            Err(err) => Err(Error::CreateBucket(err.to_string())),
        }
    }

    /// `s3Store.Get`. The body is streamed by the SDK; callers decide whether
    /// to buffer it or read incrementally.
    pub async fn get(&self, key: &str) -> Result<ObjectBody, Error> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| Error::GetObject(err.to_string()))?;
        Ok(ObjectBody::S3 {
            body: output.body,
            content_length: output.content_length,
        })
    }

    /// `s3Store.Upload`; returns the number of bytes uploaded.
    pub async fn upload(&self, key: &str, body: ByteStream) -> Result<i64, Error> {
        // The baseline counts the bytes read from the reader; when the SDK
        // knows the exact size (in-memory/file bodies) use it.
        let uploaded = body.size_hint().1.unwrap_or(0) as i64;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|err| Error::UploadObject(err.to_string()))?;
        Ok(uploaded)
    }

    /// `s3Store.Delete`.
    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| Error::DeleteObject(err.to_string()))?;
        Ok(())
    }

    /// `ListObjectsV2` pagination: every object whose key starts with `prefix`
    /// with its `LastModified` as a Unix timestamp.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        let mut objects = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = token.as_deref() {
                request = request.continuation_token(token);
            }
            let page = request
                .send()
                .await
                .map_err(|err| Error::ListObjects(err.to_string()))?;
            for object in page.contents() {
                objects.push(ObjectInfo {
                    key: object.key().unwrap_or_default().to_owned(),
                    last_modified_secs: object.last_modified().map(|value| value.secs()),
                });
            }
            match page.next_continuation_token() {
                Some(next) if !next.is_empty() => token = Some(next.to_owned()),
                _ => break,
            }
        }
        Ok(objects)
    }

    /// `s3Store.ExpireObjects`: delete every object under `prefix` whose
    /// `LastModified` is at least `max_age` old. Deletions are flushed in
    /// batches of 1000 with `DeleteObjects`.
    ///
    /// Baseline error handling is preserved: a pagination error stops the scan
    /// and a failed flush keeps its batch for the next flush, and both are
    /// logged rather than returned (the routine retries on its next run).
    pub async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), Error> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs() as i64)
            .unwrap_or_default();
        let max_age_secs = max_age.as_secs() as i64;
        let mut to_delete: Vec<ObjectIdentifier> = Vec::new();
        let mut token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = token.as_deref() {
                request = request.continuation_token(token);
            }
            let page = match request.send().await {
                Ok(page) => page,
                Err(err) => {
                    tracing::error!(error = %err, bucket = %self.bucket, "Failed to paginate S3 bucket");
                    break; // we'll try again later
                }
            };
            for object in page.contents() {
                let Some(last_modified) = object.last_modified() else {
                    continue;
                };
                if now_secs - last_modified.secs() >= max_age_secs {
                    if let Some(key) = object.key() {
                        to_delete.push(
                            ObjectIdentifier::builder()
                                .key(key)
                                .build()
                                .expect("key is set"),
                        );
                        if to_delete.len() >= 1000 {
                            self.flush_deletes(&mut to_delete).await;
                        }
                    }
                }
            }
            match page.next_continuation_token() {
                Some(next) if !next.is_empty() => token = Some(next.to_owned()),
                _ => break,
            }
        }
        if !to_delete.is_empty() {
            self.flush_deletes(&mut to_delete).await;
        }
        Ok(())
    }

    /// The baseline's `flush` closure: on success the batch is cleared, on
    /// failure it is kept for the next flush.
    async fn flush_deletes(&self, to_delete: &mut Vec<ObjectIdentifier>) {
        let mut builder = Delete::builder();
        builder = builder.set_objects(Some(to_delete.clone()));
        let delete = match builder.build() {
            Ok(delete) => delete,
            Err(err) => {
                tracing::error!(error = %err, bucket = %self.bucket, "Failed to delete objects in S3 bucket");
                return;
            }
        };
        match self
            .client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await
        {
            Ok(_) => to_delete.clear(),
            Err(err) => {
                tracing::error!(error = %err, bucket = %self.bucket, "Failed to delete objects in S3 bucket");
            }
        }
    }
}

/// `object.Storage`: dispatches to the S3/blobstore or GCS backend. The
/// baseline's `CreateLazyStorage` picks the implementation from
/// `StorageConfig.Backend`; the same names and method surface are exposed
/// here so consumers (precise-code-intel-worker, the upload-store expirer and
/// the exhaustive-search services) do not branch on the backend themselves.
pub enum ObjectStore {
    S3(S3Store),
    Gcs(GcsStore),
}

impl ObjectStore {
    /// `newS3FromConfig` / `newGCSFromConfig` dispatch (`object.create`).
    pub async fn new(config: StorageConfig) -> Result<Self, Error> {
        let config = config.normalize();
        match config.backend.as_str() {
            "gcs" => {
                let store = GcsStore::new(config).map_err(|err| Error::Gcs(err.to_string()))?;
                Ok(Self::Gcs(store))
            }
            "s3" | "blobstore" => Ok(Self::S3(S3Store::new(config).await?)),
            other => Err(Error::UnknownBackend(other.to_owned())),
        }
    }

    /// Build an S3 store from an already configured SDK client.
    pub fn with_client(client: Client, bucket: impl Into<String>, manage_bucket: bool) -> Self {
        Self::S3(S3Store::with_client(client, bucket, manage_bucket))
    }

    /// Build from an already configured GCS store (tests inject a transport).
    pub fn from_gcs(store: GcsStore) -> Self {
        Self::Gcs(store)
    }

    pub fn bucket(&self) -> &str {
        match self {
            Self::S3(store) => store.bucket(),
            Self::Gcs(store) => store.bucket(),
        }
    }

    /// `object.Storage.Init`.
    pub async fn init(&self) -> Result<(), Error> {
        match self {
            Self::S3(store) => store.init().await,
            Self::Gcs(store) => store
                .init()
                .await
                .map_err(|err| Error::Gcs(err.to_string())),
        }
    }

    /// `object.Storage.Get`.
    pub async fn get(&self, key: &str) -> Result<ObjectBody, Error> {
        match self {
            Self::S3(store) => store.get(key).await,
            Self::Gcs(store) => store
                .get(key)
                .await
                .map(ObjectBody::Gcs)
                .map_err(|err| Error::Gcs(err.to_string())),
        }
    }

    /// `object.Storage.Upload`; returns the number of bytes uploaded. The GCS
    /// backend takes bytes, so the S3-style stream is drained first.
    pub async fn upload(&self, key: &str, body: ByteStream) -> Result<i64, Error> {
        match self {
            Self::S3(store) => store.upload(key, body).await,
            Self::Gcs(store) => {
                let data = body
                    .collect()
                    .await
                    .map_err(|err| Error::Gcs(format!("failed to upload object: {err}")))?
                    .into_bytes();
                store
                    .upload(key, &data)
                    .await
                    .map_err(|err| Error::Gcs(err.to_string()))
            }
        }
    }

    /// `object.Storage.Delete`.
    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        match self {
            Self::S3(store) => store.delete(key).await,
            Self::Gcs(store) => store
                .delete(key)
                .await
                .map_err(|err| Error::Gcs(err.to_string())),
        }
    }

    /// `object.Storage.List`: every key with the prefix. GCS listings do not
    /// expose `LastModified` here; the expirer uses `timeCreated` internally.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        match self {
            Self::S3(store) => store.list_objects(prefix).await,
            Self::Gcs(store) => store
                .list_objects(prefix)
                .await
                .map(|keys| {
                    keys.into_iter()
                        .map(|key| ObjectInfo {
                            key,
                            last_modified_secs: None,
                        })
                        .collect()
                })
                .map_err(|err| Error::Gcs(err.to_string())),
        }
    }

    /// `object.Storage.ExpireObjects`.
    pub async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), Error> {
        match self {
            Self::S3(store) => store.expire_objects(prefix, max_age).await,
            Self::Gcs(store) => store
                .expire_objects(prefix, max_age)
                .await
                .map_err(|err| Error::Gcs(err.to_string())),
        }
    }
}

/// A `ListObjectsV2` entry: the key and its `LastModified` (Unix seconds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub last_modified_secs: Option<i64>,
}

/// `smithyhttp.RequestSendError`: the request could not be sent or no HTTP
/// response arrived. Rust's AWS SDK reports these as `DispatchFailure`
/// (connection refused/reset/DNS/TLS) or `TimeoutError`; a `ResponseError`
/// (unparseable response) or `ServiceError` (HTTP error status) is *not*
/// retryable, matching the baseline's typed check.
fn is_request_send_error(
    err: &SdkError<CreateBucketError, aws_sdk_s3::config::http::HttpResponse>,
) -> bool {
    matches!(
        err,
        SdkError::DispatchFailure(_) | SdkError::TimeoutError(_)
    )
}

/// `errors.HasType[*s3types.BucketAlreadyExists] || HasType[OwnedByYou]`.
/// S3-compatible servers may surface the condition as an unhandled error code,
/// so check both the typed variants and the error code.
fn is_bucket_already_exists(
    err: &SdkError<CreateBucketError, aws_sdk_s3::config::http::HttpResponse>,
) -> bool {
    let SdkError::ServiceError(service) = err else {
        return false;
    };
    let error = service.err();
    error.is_bucket_already_exists()
        || error.is_bucket_already_owned_by_you()
        || matches!(
            aws_sdk_s3::error::ProvideErrorMetadata::code(error),
            Some("BucketAlreadyExists" | "BucketAlreadyOwnedByYou")
        )
}

/// `s3ClientConfig` + `s3ClientOptions`.
async fn build_client(s3: &S3Config) -> Client {
    let credentials = if s3.access_key_id.is_empty()
        && s3.secret_access_key.is_empty()
        && s3.session_token.is_empty()
    {
        None
    } else {
        Some(Credentials::new(
            s3.access_key_id.clone(),
            s3.secret_access_key.clone(),
            (!s3.session_token.is_empty()).then(|| s3.session_token.clone()),
            None,
            "sg-objectstorage-static",
        ))
    };

    let mut builder = if s3.is_blobstore {
        // Blobstore: no local credential files or role assumption; the config
        // only carries the region (and a static provider when configured).
        let mut builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s3.region.clone()));
        if let Some(credentials) = credentials {
            builder = builder.credentials_provider(credentials);
        }
        builder
    } else {
        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(s3.region.clone()));
        if let Some(credentials) = credentials {
            loader = loader.credentials_provider(credentials);
        }
        let shared = loader.load().await;
        aws_sdk_s3::config::Builder::from(&shared)
    };

    if !s3.endpoint.is_empty() {
        builder = builder.endpoint_url(s3.endpoint.clone());
    }

    builder = builder.force_path_style(s3.use_path_style);

    Client::from_conf(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_lowercases_backend() {
        let config = StorageConfig {
            backend: "S3".to_owned(),
            ..StorageConfig::default()
        }
        .normalize();
        assert_eq!(config.backend, "s3");
        assert!(!config.s3.is_blobstore);
        assert!(!config.manage_bucket);
        assert!(!config.s3.use_path_style);
    }

    #[test]
    fn normalize_blobstore_forces_management_and_path_style() {
        let config = StorageConfig {
            backend: "BlobStore".to_owned(),
            ..StorageConfig::default()
        }
        .normalize();
        assert_eq!(config.backend, "blobstore");
        assert!(config.s3.is_blobstore);
        assert!(config.manage_bucket);
        assert!(config.s3.use_path_style);
    }

    #[test]
    fn init_request_send_errors_are_classified_for_retry() {
        use aws_sdk_s3::error::ConnectorError;

        assert!(Error::CreateBucketRequestSend("x".to_owned()).is_request_error());
        assert!(!Error::CreateBucket("x".to_owned()).is_request_error());
        assert!(!Error::Gcs("x".to_owned()).is_request_error());

        type CreateError = SdkError<CreateBucketError, aws_sdk_s3::config::http::HttpResponse>;
        let dispatch: CreateError =
            SdkError::dispatch_failure(ConnectorError::io("connection refused".into()));
        assert!(is_request_send_error(&dispatch));
        let timeout: CreateError = SdkError::timeout_error("timed out");
        assert!(is_request_send_error(&timeout));
        let construction: CreateError = SdkError::construction_failure("bad request");
        assert!(!is_request_send_error(&construction));
    }

    #[test]
    fn error_messages_match_baseline_wraps() {
        assert_eq!(
            Error::CreateBucket("x".to_owned()).to_string(),
            "failed to create bucket: x"
        );
        assert_eq!(
            Error::CreateBucketRequestSend("x".to_owned()).to_string(),
            "failed to create bucket: x"
        );
        assert_eq!(
            Error::GetObject("x".to_owned()).to_string(),
            "failed to get object: x"
        );
        assert_eq!(
            Error::DeleteObject("x".to_owned()).to_string(),
            "failed to delete object: x"
        );
    }
}
