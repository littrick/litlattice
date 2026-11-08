#![forbid(unsafe_code)]

//! `sg-objectstorage`: S3/blobstore client used by the code-intel upload
//! processor.
//!
//! Mirrors `internal/object` from the Go baseline:
//!
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

/// `object.StorageConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageConfig {
    pub backend: String,
    pub manage_bucket: bool,
    pub bucket: String,
    pub s3: S3Config,
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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateBucket(message) => write!(f, "failed to create bucket: {message}"),
            Self::GetObject(message) => write!(f, "failed to get object: {message}"),
            Self::UploadObject(message) => write!(f, "failed to upload object: {message}"),
            Self::DeleteObject(message) => write!(f, "failed to delete object: {message}"),
            Self::ListObjects(message) => write!(f, "failed to list objects: {message}"),
            Self::DeleteObjects(message) => write!(f, "failed to delete objects: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// The response of `s3Store.Get`. `into_async_read` is the `io.ReadCloser`
/// equivalent.
pub struct ObjectBody {
    body: ByteStream,
    content_length: Option<i64>,
}

impl ObjectBody {
    /// `GetObjectOutput.ContentLength`; absent when the server omits it.
    pub fn content_length(&self) -> Option<i64> {
        self.content_length
    }

    pub fn into_byte_stream(self) -> ByteStream {
        self.body
    }

    /// Convert into a [`tokio::io::AsyncRead`].
    pub fn into_async_read(self) -> impl tokio::io::AsyncRead + Send {
        self.body.into_async_read()
    }
}

/// An S3-compatible store (`object.s3Store`).
pub struct ObjectStore {
    client: Client,
    bucket: String,
    manage_bucket: bool,
}

impl ObjectStore {
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
        Ok(ObjectBody {
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

/// A `ListObjectsV2` entry: the key and its `LastModified` (Unix seconds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub last_modified_secs: Option<i64>,
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
    fn error_messages_match_baseline_wraps() {
        assert_eq!(
            Error::CreateBucket("x".to_owned()).to_string(),
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
