#![forbid(unsafe_code)]

//! Multipart uploads (`multipart.go`).
//!
//! The multipart slice implements the descriptor object, part uploads/copies
//! and complete/abort using the object backend. Pending uploads live in the
//! internal `<bucket>---uploads` bucket: the descriptor is keyed by the upload
//! id and each part is keyed `<upload_id>---<part_number>`.

use std::io;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::backend::{
    BackendError, ByteStream, MultipartBackend, ObjectBackend, ObjectMeta, ObjectReader,
};
use crate::fs_backend::{FsBlobStore, MULTIPART_UPLOADS_BUCKET_SUFFIX};

/// Upload descriptor object (`pendingUpload` in Go).
///
/// Field names are serialized exactly as Go's `encoding/json` would emit them
/// (the Go struct uses its default field names).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingUpload {
    #[serde(rename = "BucketName")]
    bucket_name: String,
    #[serde(rename = "ObjectName")]
    object_name: String,
    #[serde(rename = "Parts", default)]
    parts: Vec<i32>,
}

impl PendingUpload {
    /// Part numbers must be consecutively ordered, but can start/end at any
    /// number. Returns the min/max found in `parts` (`partNumberRange`).
    fn part_number_range(&self) -> (i32, i32) {
        let mut min = -1;
        let mut max = -1;
        for &part_number in &self.parts {
            if max == -1 || part_number > max {
                max = part_number;
            }
            if min == -1 || part_number < min {
                min = part_number;
            }
        }
        (min, max)
    }
}

/// Name of the bucket holding multipart upload state (`bucketName + suffix`).
fn uploads_bucket_name(bucket: &str) -> String {
    format!("{bucket}{MULTIPART_UPLOADS_BUCKET_SUFFIX}")
}

/// Object name of an uploaded part (`fmt.Sprintf("%v---%v", uploadID, part)`).
fn part_object_name(upload_id: &str, part_number: i32) -> String {
    format!("{upload_id}---{part_number}")
}

/// A `ByteStream` that yields a single in-memory chunk.
fn byte_stream(data: Vec<u8>) -> ByteStream {
    Box::pin(tokio_stream::once(Ok(Bytes::from(data))))
}

impl FsBlobStore {
    /// Fetch and decode a pending upload descriptor. A missing descriptor
    /// object maps to `NoSuchUpload` (`getPendingUpload`).
    async fn get_pending_upload(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<PendingUpload, BackendError> {
        let uploads_bucket = uploads_bucket_name(bucket);
        let mut reader = match self.get_object(&uploads_bucket, upload_id).await {
            Ok(reader) => reader,
            Err(BackendError::NoSuchKey) => return Err(BackendError::NoSuchUpload),
            Err(error) => return Err(error),
        };

        let mut buffer = Vec::new();
        reader.file.read_to_end(&mut buffer).await?;
        serde_json::from_slice(&buffer)
            .map_err(|error| BackendError::Other(format!("decodePendingUpload: {error}")))
    }

    /// Write a pending upload descriptor (`upsertPendingUpload`).
    async fn upsert_pending_upload(
        &self,
        bucket: &str,
        upload_id: &str,
        upload: &PendingUpload,
    ) -> Result<(), BackendError> {
        let uploads_bucket = uploads_bucket_name(bucket);
        let data = serde_json::to_vec(upload)
            .map_err(|error| BackendError::Other(format!("encodingPendingUpload: {error}")))?;
        self.put_object(&uploads_bucket, upload_id, byte_stream(data))
            .await?;
        Ok(())
    }

    /// Read, mutate and rewrite a pending upload descriptor while holding
    /// `mutate_pending_upload`.
    ///
    /// Go holds `mutatePendingUploadMu` across read / mutate / write; keep the
    /// same critical section so concurrent part uploads cannot lose an append.
    async fn mutate_pending_upload_atomic<F>(
        &self,
        bucket: &str,
        upload_id: &str,
        mutate: F,
    ) -> Result<(), BackendError>
    where
        F: FnOnce(&mut PendingUpload) + Send,
    {
        let _guard = self.mutate_pending_upload.lock().await;
        let mut upload = self.get_pending_upload(bucket, upload_id).await?;
        mutate(&mut upload);
        self.upsert_pending_upload(bucket, upload_id, &upload).await
    }

    /// Delete the descriptor and every part in `[min, max]` (`deletePendingUpload`).
    ///
    /// This is best effort: individual failures are logged and otherwise
    /// ignored, matching the Go caller which logs and continues.
    async fn delete_pending_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        min_part_number: i32,
        max_part_number: i32,
    ) {
        let uploads_bucket = uploads_bucket_name(bucket);
        if let Err(error) = self.delete_object(&uploads_bucket, upload_id).await {
            tracing::error!(
                bucket,
                key,
                upload_id,
                %error,
                "deleting pending multi-part upload failed"
            );
        }
        for part_number in min_part_number..=max_part_number {
            let part_name = part_object_name(upload_id, part_number);
            if let Err(error) = self.delete_object(&uploads_bucket, &part_name).await {
                tracing::error!(
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    %error,
                    "deleting pending multi-part upload failed"
                );
            }
        }
    }

    /// Open parts `min..=max` and stream them, in order, into `bucket/key`.
    async fn complete_upload_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        min_part_number: i32,
        max_part_number: i32,
    ) -> Result<(), BackendError> {
        let uploads_bucket = uploads_bucket_name(bucket);

        let mut readers = Vec::new();
        for part_number in min_part_number..=max_part_number {
            let part_name = part_object_name(upload_id, part_number);
            match self.get_object(&uploads_bucket, &part_name).await {
                Ok(reader) => readers.push(reader),
                Err(BackendError::NoSuchKey) => return Err(BackendError::InvalidPartOrder),
                Err(error) => return Err(error),
            }
        }

        self.put_object(bucket, key, concatenated_stream(readers))
            .await?;
        Ok(())
    }
}

/// Chain `readers` into one stream without buffering whole parts.
///
/// A background task copies each file into a bounded channel in order; the
/// returned stream is backed by the channel's receive half.
fn concatenated_stream(readers: Vec<ObjectReader>) -> ByteStream {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(4);
    tokio::spawn(async move {
        for reader in readers {
            let mut stream = ReaderStream::new(reader.file);
            while let Some(item) = stream.next().await {
                if sender.send(item).await.is_err() {
                    // The consumer went away; stop copying.
                    return;
                }
            }
        }
    });
    Box::pin(ReceiverStream::new(receiver))
}

#[async_trait]
impl MultipartBackend for FsBlobStore {
    async fn create_upload(&self, bucket: &str, key: &str) -> Result<String, BackendError> {
        // Create the bucket which will hold multipart uploads for the named
        // bucket. An existing bucket is fine.
        let uploads_bucket = uploads_bucket_name(bucket);
        match self.create_bucket(&uploads_bucket).await {
            Ok(()) | Err(BackendError::BucketAlreadyExists) => {}
            Err(error) => return Err(error),
        }

        // Create the upload descriptor object.
        let upload_id = Uuid::new_v4().to_string();
        let upload = PendingUpload {
            bucket_name: bucket.to_owned(),
            object_name: key.to_owned(),
            parts: Vec::new(),
        };
        self.upsert_pending_upload(bucket, &upload_id, &upload)
            .await?;

        tracing::debug!(key = %format!("{bucket}/{key}"), %upload_id, "createUpload");
        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        bucket: &str,
        _key: &str,
        upload_id: &str,
        part_number: i32,
        body: ByteStream,
    ) -> Result<ObjectMeta, BackendError> {
        // Add the new part number to the upload descriptor first; Go appends
        // the part before writing the part object.
        self.mutate_pending_upload_atomic(bucket, upload_id, |upload| {
            upload.parts.push(part_number);
        })
        .await?;

        let part_name = part_object_name(upload_id, part_number);
        let metadata = self
            .put_object(&uploads_bucket_name(bucket), &part_name, body)
            .await?;

        tracing::debug!(
            key = %format!("{bucket}/{_key}"),
            %upload_id,
            part_number,
            "uploadPart"
        );
        Ok(metadata)
    }

    async fn upload_part_copy(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        source_bucket: &str,
        source_key: &str,
    ) -> Result<ObjectMeta, BackendError> {
        // A missing source propagates as `NoSuchKey`; Go wraps the error from
        // `getObject` rather than translating it to `NoSuchUpload`.
        let source = self.get_object(source_bucket, source_key).await?;
        let body: ByteStream = Box::pin(ReaderStream::new(source.file));
        self.upload_part(bucket, key, upload_id, part_number, body)
            .await
    }

    async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), BackendError> {
        let upload = self.get_pending_upload(bucket, upload_id).await?;
        let (min_part_number, max_part_number) = upload.part_number_range();

        let result = self
            .complete_upload_parts(bucket, key, upload_id, min_part_number, max_part_number)
            .await;

        // The upload cannot be recovered once completion has been attempted,
        // so always delete the descriptor and parts (best effort).
        self.delete_pending_upload(bucket, key, upload_id, min_part_number, max_part_number)
            .await;

        result
    }

    async fn abort_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), BackendError> {
        let upload = self.get_pending_upload(bucket, upload_id).await?;
        let (min_part_number, max_part_number) = upload.part_number_range();

        self.delete_pending_upload(bucket, key, upload_id, min_part_number, max_part_number)
            .await;

        tracing::debug!(key = %format!("{bucket}/{key}"), %upload_id, "abortUpload");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ObjectBackend;

    fn stream(data: &[u8]) -> ByteStream {
        byte_stream(data.to_vec())
    }

    async fn read_object(store: &FsBlobStore, bucket: &str, key: &str) -> Vec<u8> {
        let mut reader = store.get_object(bucket, key).await.unwrap();
        let mut buffer = Vec::new();
        reader.file.read_to_end(&mut buffer).await.unwrap();
        buffer
    }

    #[tokio::test]
    async fn create_upload_upload_complete_concatenates_parts() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path()).unwrap();
        store.create_bucket("bucket").await.unwrap();

        let upload_id = store.create_upload("bucket", "object").await.unwrap();
        store
            .upload_part("bucket", "object", &upload_id, 1, stream(b"Hello "))
            .await
            .unwrap();
        store
            .upload_part("bucket", "object", &upload_id, 2, stream(b"world!"))
            .await
            .unwrap();
        store
            .complete_upload("bucket", "object", &upload_id)
            .await
            .unwrap();

        assert_eq!(
            read_object(&store, "bucket", "object").await,
            b"Hello world!"
        );

        // The descriptor and parts are always removed after completion.
        assert!(matches!(
            store.get_object("bucket---uploads", &upload_id).await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store
                .get_object("bucket---uploads", &format!("{upload_id}---1"))
                .await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store
                .get_object("bucket---uploads", &format!("{upload_id}---2"))
                .await,
            Err(BackendError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn abort_upload_removes_descriptor_and_parts() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path()).unwrap();
        store.create_bucket("bucket").await.unwrap();

        let upload_id = store.create_upload("bucket", "object").await.unwrap();
        store
            .upload_part("bucket", "object", &upload_id, 1, stream(b"one"))
            .await
            .unwrap();
        store
            .upload_part("bucket", "object", &upload_id, 2, stream(b"two"))
            .await
            .unwrap();

        store
            .abort_upload("bucket", "object", &upload_id)
            .await
            .unwrap();

        assert!(matches!(
            store.get_object("bucket---uploads", &upload_id).await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store
                .get_object("bucket---uploads", &format!("{upload_id}---1"))
                .await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store
                .get_object("bucket---uploads", &format!("{upload_id}---2"))
                .await,
            Err(BackendError::NoSuchKey)
        ));

        // A second abort sees no descriptor.
        assert!(matches!(
            store.abort_upload("bucket", "object", &upload_id).await,
            Err(BackendError::NoSuchUpload)
        ));
    }

    #[tokio::test]
    async fn missing_upload_is_no_such_upload() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path()).unwrap();
        store.create_bucket("bucket").await.unwrap();

        assert!(matches!(
            store.complete_upload("bucket", "object", "missing").await,
            Err(BackendError::NoSuchUpload)
        ));
        assert!(matches!(
            store.abort_upload("bucket", "object", "missing").await,
            Err(BackendError::NoSuchUpload)
        ));
        assert!(matches!(
            store
                .upload_part("bucket", "object", "missing", 1, stream(b"data"))
                .await,
            Err(BackendError::NoSuchUpload)
        ));
    }

    #[tokio::test]
    async fn upload_part_copy_streams_source_object() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path()).unwrap();
        store.create_bucket("source").await.unwrap();
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("source", "src", stream(b"copied data"))
            .await
            .unwrap();

        let upload_id = store.create_upload("bucket", "object").await.unwrap();
        store
            .upload_part_copy("bucket", "object", &upload_id, 1, "source", "src")
            .await
            .unwrap();
        store
            .upload_part("bucket", "object", &upload_id, 2, stream(b"!"))
            .await
            .unwrap();
        store
            .complete_upload("bucket", "object", &upload_id)
            .await
            .unwrap();

        assert_eq!(
            read_object(&store, "bucket", "object").await,
            b"copied data!"
        );

        // A missing copy source propagates `NoSuchKey` (Go wraps the
        // `getObject` error rather than mapping it to `NoSuchUpload`).
        let upload_id = store.create_upload("bucket", "other").await.unwrap();
        assert!(matches!(
            store
                .upload_part_copy("bucket", "other", &upload_id, 1, "source", "missing")
                .await,
            Err(BackendError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn gap_in_parts_is_invalid_part_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path()).unwrap();
        store.create_bucket("bucket").await.unwrap();

        let upload_id = store.create_upload("bucket", "object").await.unwrap();
        store
            .upload_part("bucket", "object", &upload_id, 1, stream(b"one"))
            .await
            .unwrap();
        // Part 2 is deliberately missing.
        store
            .upload_part("bucket", "object", &upload_id, 3, stream(b"three"))
            .await
            .unwrap();

        assert!(matches!(
            store.complete_upload("bucket", "object", &upload_id).await,
            Err(BackendError::InvalidPartOrder)
        ));

        // Failed completion still cleans up the pending upload.
        assert!(matches!(
            store.get_object("bucket---uploads", &upload_id).await,
            Err(BackendError::NoSuchKey)
        ));
    }
}
