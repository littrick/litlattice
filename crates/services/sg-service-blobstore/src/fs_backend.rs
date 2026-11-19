#![forbid(unsafe_code)]

//! Filesystem object backend (`blobstore.go` + `blobstore_posix.go`).
//!
//! Objects live as flat files under `<data_dir>/buckets/<bucket>/`, named with
//! `url.QueryEscape(key)` exactly like the Go implementation, so an existing
//! data directory keeps working. Bucket-level `tokio::sync::RwLock`s mirror
//! Go's `bucketLock`: creators take the write lock, all object operations take
//! the read lock.
//!
//! Multipart methods live in `multipart.rs`.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use crate::backend::{BackendError, ByteStream, ObjectBackend, ObjectMeta, ObjectReader};

pub const BUCKETS_DIR: &str = "buckets";
pub const MULTIPART_UPLOADS_BUCKET_SUFFIX: &str = "---uploads";

#[derive(Debug)]
pub struct FsBlobStore {
    pub(crate) data_dir: PathBuf,
    /// `bucketLock`: the outer std `Mutex` only guards the map, never held
    /// across an await. The inner lock is a tokio `RwLock` so its guards are
    /// `Send` and may be held across awaits.
    pub(crate) bucket_locks: Mutex<HashMap<String, Arc<RwLock<()>>>>,
    /// Serializes descriptor read-modify-write across awaits
    /// (`mutatePendingUploadAtomic`); unlike the bucket map this must be a
    /// tokio mutex.
    pub(crate) mutate_pending_upload: tokio::sync::Mutex<()>,
}

impl FsBlobStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> io::Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(data_dir.join(BUCKETS_DIR))?;
        Ok(Self {
            data_dir,
            bucket_locks: Mutex::new(HashMap::new()),
            mutate_pending_upload: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) fn bucket_dir(&self, bucket: &str) -> PathBuf {
        self.data_dir.join(BUCKETS_DIR).join(bucket)
    }

    pub(crate) fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.bucket_dir(bucket)
            .join(crate::paths::object_file_name(key))
    }

    /// Bucket-level lock (`bucketLock`). Writers exclude every other access.
    pub(crate) fn bucket_lock(&self, bucket: &str) -> Arc<RwLock<()>> {
        let mut locks = self.bucket_locks.lock().expect("bucket locks poisoned");
        Arc::clone(
            locks
                .entry(bucket.to_owned())
                .or_insert_with(|| Arc::new(RwLock::new(()))),
        )
    }

    /// Best-effort directory fsync, so a completed rename is durable
    /// (`fsync` in `blobstore_posix.go`). Directories can only be opened for
    /// fsync on Unix.
    async fn fsync_dir(path: &std::path::Path) -> Result<(), BackendError> {
        #[cfg(unix)]
        {
            let dir = tokio::fs::File::open(path).await?;
            dir.sync_all().await?;
        }
        #[cfg(not(unix))]
        {
            let _ = path;
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectBackend for FsBlobStore {
    async fn create_bucket(&self, bucket: &str) -> Result<(), BackendError> {
        // Exclusive bucket lock: nobody may read/write the bucket while it is
        // being created.
        let lock = self.bucket_lock(bucket);
        let _guard = lock.write().await;

        let bucket_dir = self.bucket_dir(bucket);
        // Go proceeds to Mkdir on any stat error; Mkdir then surfaces it.
        if tokio::fs::metadata(&bucket_dir).await.is_ok() {
            return Err(BackendError::BucketAlreadyExists);
        }

        match tokio::fs::create_dir(&bucket_dir).await {
            Ok(()) => {
                tracing::info!(bucket, dir = %bucket_dir.display(), "created bucket");
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Err(BackendError::BucketAlreadyExists)
            }
            Err(error) => Err(BackendError::Io(error)),
        }
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        mut body: ByteStream,
    ) -> Result<ObjectMeta, BackendError> {
        // Ensure the bucket cannot be created/deleted while we look at it.
        let lock = self.bucket_lock(bucket);
        let _guard = lock.read().await;

        let bucket_dir = self.bucket_dir(bucket);
        if tokio::fs::metadata(&bucket_dir).await.is_err() {
            return Err(BackendError::NoSuchBucket);
        }

        // Write to a temp file in the bucket dir and atomically rename over the
        // final path, exactly like Go (`os.CreateTemp` + `os.Rename`).
        let escaped = crate::paths::object_file_name(key);
        let tmp_path = bucket_dir.join(format!("{}-{escaped}.tmp", uuid::Uuid::new_v4()));
        let mut tmp = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .await?;

        let object_path = self.object_path(bucket, key);
        let result: Result<u64, BackendError> = async {
            let mut hasher = md5::Context::new();
            let mut size: u64 = 0;
            while let Some(chunk) = body.next().await {
                let chunk: Bytes = chunk?;
                hasher.consume(&chunk);
                tmp.write_all(&chunk).await?;
                size += chunk.len() as u64;
            }
            // Ensure bytes are on disk before the rename (Go `tmpFile.Sync`).
            tmp.flush().await?;
            tmp.sync_all().await?;
            drop(tmp);
            tokio::fs::rename(&tmp_path, &object_path).await?;
            // Ensure the rename itself is recorded (Go `fsync(bucketDir)`).
            Self::fsync_dir(&bucket_dir).await?;
            let etag = hasher.compute();
            tracing::debug!(bucket, key, etag = %format!("{etag:x}"), "put object");
            Ok(size)
        }
        .await;

        let size = match result {
            Ok(size) => size,
            Err(error) => {
                // Never leave a stray temp file behind on failure.
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return Err(error);
            }
        };

        Ok(ObjectMeta {
            key: key.to_owned(),
            size,
            // Go deliberately reports "logically now", not the file mtime.
            last_modified: SystemTime::now(),
        })
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectReader, BackendError> {
        let lock = self.bucket_lock(bucket);
        let _guard = lock.read().await;

        let object_path = self.object_path(bucket, key);
        let file = match tokio::fs::File::open(&object_path).await {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(BackendError::NoSuchKey)
            }
            Err(error) => return Err(BackendError::Io(error)),
        };
        let metadata = file.metadata().await?;
        let last_modified = metadata.modified().unwrap_or_else(|_| SystemTime::now());
        Ok(ObjectReader {
            file,
            size: metadata.len(),
            last_modified,
        })
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta, BackendError> {
        let lock = self.bucket_lock(bucket);
        let _guard = lock.read().await;

        let object_path = self.object_path(bucket, key);
        let metadata = match tokio::fs::metadata(&object_path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(BackendError::NoSuchKey)
            }
            Err(error) => return Err(BackendError::Io(error)),
        };
        Ok(ObjectMeta {
            key: key.to_owned(),
            size: metadata.len(),
            // Head/list report the file mtime (Go `entry.Info().ModTime()`).
            last_modified: metadata.modified().unwrap_or_else(|_| SystemTime::now()),
        })
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), BackendError> {
        let lock = self.bucket_lock(bucket);
        let _guard = lock.read().await;

        let object_path = self.object_path(bucket, key);
        match tokio::fs::remove_file(&object_path).await {
            Ok(()) => {
                tracing::debug!(bucket, key, "delete object");
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(BackendError::NoSuchKey),
            Err(error) => Err(BackendError::Io(error)),
        }
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<ObjectMeta>, BackendError> {
        let lock = self.bucket_lock(bucket);
        let _guard = lock.read().await;

        let bucket_dir = self.bucket_dir(bucket);
        let mut entries = match tokio::fs::read_dir(&bucket_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(BackendError::NoSuchBucket)
            }
            Err(error) => return Err(BackendError::Io(error)),
        };

        let mut objects = Vec::new();
        // OS order, unsorted, matching Go's `os.ReadDir`.
        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let object_name = crate::paths::file_name_to_object_name(&file_name);
            if !object_name.starts_with(prefix) {
                continue;
            }
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                Err(error) => {
                    tracing::warn!(
                        bucket,
                        key = %object_name,
                        error = %error,
                        "error listing objects in bucket (ignoring)"
                    );
                    continue;
                }
            };
            let last_modified = match metadata.modified() {
                Ok(modified) => modified,
                Err(error) => {
                    tracing::warn!(
                        bucket,
                        key = %object_name,
                        error = %error,
                        "error listing objects in bucket (ignoring)"
                    );
                    continue;
                }
            };
            objects.push(ObjectMeta {
                key: object_name,
                size: metadata.len(),
                last_modified,
            });
        }
        Ok(objects)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use tokio::io::AsyncReadExt;

    use super::*;

    fn stream_from(data: impl Into<Vec<u8>>) -> ByteStream {
        let bytes = Bytes::from(data.into());
        Box::pin(tokio_stream::iter(vec![Ok::<Bytes, io::Error>(bytes)]))
    }

    /// A stream that emits one chunk and then fails, exercising cleanup.
    fn failing_stream() -> ByteStream {
        Box::pin(tokio_stream::iter(vec![Err::<Bytes, io::Error>(
            io::Error::other("boom"),
        )]))
    }

    async fn read_all(reader: ObjectReader) -> Vec<u8> {
        let mut file = reader.file;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.expect("read object");
        buf
    }

    async fn store() -> (tempfile::TempDir, FsBlobStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsBlobStore::new(dir.path()).expect("store");
        (dir, store)
    }

    #[tokio::test]
    async fn create_bucket_creates_directory_and_rejects_duplicate() {
        let (_dir, store) = store().await;
        store.create_bucket("bucket").await.expect("create");
        assert!(store.bucket_dir("bucket").is_dir());
        assert!(matches!(
            store.create_bucket("bucket").await,
            Err(BackendError::BucketAlreadyExists)
        ));
    }

    #[tokio::test]
    async fn put_get_head_delete_roundtrip_with_encoded_key() {
        let (_dir, store) = store().await;
        store.create_bucket("bucket").await.expect("create");

        let key = "ümlaut/dir/file name+plus";
        let meta = store
            .put_object("bucket", key, stream_from(b"hello world".to_vec()))
            .await
            .expect("put");
        assert_eq!(meta.key, key);
        assert_eq!(meta.size, 11);

        // Flat file, URL-escaped exactly like Go's `url.QueryEscape`.
        let expected_file = store
            .bucket_dir("bucket")
            .join(crate::paths::object_file_name(key));
        assert!(
            expected_file.is_file(),
            "missing {}",
            expected_file.display()
        );
        assert!(!expected_file.parent().unwrap().join("ümlaut").exists());

        let reader = store.get_object("bucket", key).await.expect("get");
        assert_eq!(reader.size, 11);
        assert_eq!(read_all(reader).await, b"hello world");

        let head = store.head_object("bucket", key).await.expect("head");
        assert_eq!(head.key, key);
        assert_eq!(head.size, 11);

        store.delete_object("bucket", key).await.expect("delete");
        assert!(!expected_file.exists());
        assert!(matches!(
            store.get_object("bucket", key).await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store.head_object("bucket", key).await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store.delete_object("bucket", key).await,
            Err(BackendError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn missing_bucket_and_missing_key_errors() {
        let (_dir, store) = store().await;

        assert!(matches!(
            store
                .put_object("missing", "key", stream_from(b"x".to_vec()))
                .await,
            Err(BackendError::NoSuchBucket)
        ));
        assert!(matches!(
            store.list_objects("missing", "").await,
            Err(BackendError::NoSuchBucket)
        ));

        store.create_bucket("bucket").await.expect("create");
        assert!(matches!(
            store.get_object("bucket", "nope").await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store.head_object("bucket", "nope").await,
            Err(BackendError::NoSuchKey)
        ));
        assert!(matches!(
            store.delete_object("bucket", "nope").await,
            Err(BackendError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn put_overwrites_atomically_and_leaves_no_temp_files() {
        let (_dir, store) = store().await;
        store.create_bucket("bucket").await.expect("create");

        store
            .put_object("bucket", "key", stream_from(b"first".to_vec()))
            .await
            .expect("put first");
        store
            .put_object("bucket", "key", stream_from(b"second".to_vec()))
            .await
            .expect("put second");

        let reader = store.get_object("bucket", "key").await.expect("get");
        assert_eq!(read_all(reader).await, b"second");

        let entries: Vec<_> = std::fs::read_dir(store.bucket_dir("bucket"))
            .expect("read dir")
            .collect::<Result<_, _>>()
            .expect("entries");
        assert_eq!(entries.len(), 1, "temp files must be renamed or removed");
        assert_eq!(
            entries[0].file_name().to_string_lossy(),
            crate::paths::object_file_name("key")
        );
    }

    #[tokio::test]
    async fn failed_stream_cleans_up_temp_file() {
        let (_dir, store) = store().await;
        store.create_bucket("bucket").await.expect("create");

        let error = store
            .put_object("bucket", "key", failing_stream())
            .await
            .expect_err("stream error must propagate");
        assert!(matches!(error, BackendError::Io(_)));

        let entries: Vec<_> = std::fs::read_dir(store.bucket_dir("bucket"))
            .expect("read dir")
            .collect::<Result<_, _>>()
            .expect("entries");
        assert!(entries.is_empty(), "no temp file should remain");
        assert!(matches!(
            store.get_object("bucket", "key").await,
            Err(BackendError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn list_objects_filters_prefix_and_decodes_names() {
        let (_dir, store) = store().await;
        store.create_bucket("bucket").await.expect("create");

        for key in ["a/one", "a/two", "ab/three", "ümlaut/four", "b/five"] {
            store
                .put_object("bucket", key, stream_from(key.as_bytes().to_vec()))
                .await
                .expect("put");
        }

        let mut keys: Vec<String> = store
            .list_objects("bucket", "a/")
            .await
            .expect("list prefix")
            .into_iter()
            .map(|meta| meta.key)
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["a/one".to_owned(), "a/two".to_owned()]);

        let mut all: Vec<String> = store
            .list_objects("bucket", "")
            .await
            .expect("list all")
            .into_iter()
            .map(|meta| meta.key)
            .collect();
        all.sort();
        assert_eq!(
            all,
            vec![
                "a/one".to_owned(),
                "a/two".to_owned(),
                "ab/three".to_owned(),
                "b/five".to_owned(),
                "ümlaut/four".to_owned(),
            ]
        );

        // The flat file names are escaped, but listing returns decoded keys.
        assert!(store.bucket_dir("bucket").join("a%2Fone").is_file());
    }

    #[tokio::test]
    async fn head_and_list_report_file_mtime() {
        let (_dir, store) = store().await;
        store.create_bucket("bucket").await.expect("create");
        store
            .put_object("bucket", "key", stream_from(b"data".to_vec()))
            .await
            .expect("put");

        let head = store.head_object("bucket", "key").await.expect("head");
        let listed = store.list_objects("bucket", "key").await.expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(head.size, 4);
        assert_eq!(listed[0].size, 4);
        // Both consult the same filesystem mtime.
        assert_eq!(
            head.last_modified
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap(),
            listed[0]
                .last_modified
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
        );
    }
}
