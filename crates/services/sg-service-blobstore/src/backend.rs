#![forbid(unsafe_code)]

//! Storage interface shared by the filesystem backend, the multipart layer
//! and the S3 HTTP handlers.
//!
//! The shape is intentionally narrow: it is the subset the Go `cmd/blobstore`
//! implements (`internal/blobstore/blobstore.go` + `multipart.go`) and that
//! Sourcegraph's `internal/object/s3_client.go` actually calls.

use std::io;
use std::pin::Pin;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::File;
use tokio_stream::Stream;

/// Request/part payload. Errors surface as I/O errors so the HTTP layer can
/// turn them into a 500 (matching Go's `ServeHTTP` error branch).
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;

/// Metadata returned by put/head/list operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub last_modified: SystemTime,
}

/// An opened object. The caller owns the file and decides how much to stream.
#[derive(Debug)]
pub struct ObjectReader {
    pub file: File,
    pub size: u64,
    pub last_modified: SystemTime,
}

/// Errors mirror the Go sentinel errors (`ErrBucketAlreadyExists`,
/// `ErrNoSuchBucket`, `ErrNoSuchKey`, `ErrNoSuchUpload`,
/// `ErrInvalidPartOrder`) plus I/O failures.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("bucket already exists")]
    BucketAlreadyExists,
    #[error("no such bucket")]
    NoSuchBucket,
    #[error("no such key")]
    NoSuchKey,
    #[error("no such upload")]
    NoSuchUpload,
    #[error("invalid part order")]
    InvalidPartOrder,
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Other(String),
}

impl BackendError {
    /// S3 error code (`s3_types.go`).
    pub fn code(&self) -> &'static str {
        match self {
            Self::BucketAlreadyExists => "BucketAlreadyOwnedByYou",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::Io(_) | Self::Other(_) => "InternalError",
        }
    }

    /// HTTP status used by the Go handlers for each sentinel.
    pub fn status(&self) -> u16 {
        match self {
            Self::BucketAlreadyExists => 409,
            Self::NoSuchBucket => 409,
            Self::NoSuchKey => 404,
            Self::NoSuchUpload => 404,
            Self::InvalidPartOrder => 404,
            Self::Io(_) | Self::Other(_) => 500,
        }
    }

    /// `true` for the sentinels whose HTTP representation is an S3 XML error.
    pub fn is_s3_error(&self) -> bool {
        !matches!(self, Self::Io(_) | Self::Other(_))
    }
}

/// Object/bucket operations (`blobstore.go`).
#[async_trait]
pub trait ObjectBackend: Send + Sync {
    async fn create_bucket(&self, bucket: &str) -> Result<(), BackendError>;

    /// Stream `body` into `bucket/key`, atomically replacing any existing
    /// object.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
    ) -> Result<ObjectMeta, BackendError>;

    async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectReader, BackendError>;

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta, BackendError>;

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), BackendError>;

    /// Objects whose keys start with `prefix`, in directory order.
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<ObjectMeta>, BackendError>;

    /// `DELETE /<bucket>?delete`: Go ignores per-key errors and missing keys.
    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> Result<(), BackendError> {
        for key in keys {
            match self.delete_object(bucket, key).await {
                Ok(()) | Err(BackendError::NoSuchKey) => {}
                Err(error) => {
                    tracing::warn!(bucket, key = %key, error = %error, "error deleting object");
                }
            }
        }
        Ok(())
    }
}

/// Multipart operations (`multipart.go`).
#[async_trait]
pub trait MultipartBackend: ObjectBackend {
    async fn create_upload(&self, bucket: &str, key: &str) -> Result<String, BackendError>;

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: ByteStream,
    ) -> Result<ObjectMeta, BackendError>;

    async fn upload_part_copy(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        source_bucket: &str,
        source_key: &str,
    ) -> Result<ObjectMeta, BackendError>;

    async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), BackendError>;

    async fn abort_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), BackendError>;
}
