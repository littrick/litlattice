#![forbid(unsafe_code)]

//! The first half of `HandleRawUpload` (`job_worker_handler.go:195-340`):
//!
//! 1. `repoStore.Get` — the repository must exist (`Repos.Get`).
//! 2. `requeueIfCloningOrCommitUnknown` — an unresolvable commit requeues the
//!    upload one minute out with reason `commit not found`; a cloning
//!    repository uses `repository still cloning`; any other gitserver error
//!    aborts.
//! 3. `defaultBranchContains` — `GetDefaultBranch` + `ListRefs(HeadsOnly,
//!    Contains)`.
//! 4. `withUploadData` — fetch `upload-<id>.lsif.gz` from the object store,
//!    read it into memory (size hint = `upload_size`) or a temporary file when
//!    the uncompressed size (or a 5x estimate) exceeds 512 MiB, and validate
//!    the gzip header.
//! 5. Content-type validation (`application/x-ndjson+lsif` is deprecated,
//!    `application/x-protobuf+scip` is the only accepted value).
//! 6. `GetCommit` and `UpdateCommittedAt` (`codeintel_commit_dates`).
//!
//! This function stops before `prepareSCIPDataStream`; it returns the fetched
//! payload so the handler can run the SCIP stage. The delete-on-success side
//! effect of `withUploadData` belongs to the handler, which deletes the
//! object only after the code graph and the uploads-database transaction have
//! committed.

use std::fmt;
use std::io::{BufReader, Cursor, Read};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use flate2::read::GzDecoder;
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use sg_git::{Error as GitError, GitserverClient};
use sg_objectstorage::ObjectStore;

use crate::queue::{Upload, UploadQueueStore};

pub const LSIF_CONTENT_TYPE: &str = "application/x-ndjson+lsif";
pub const SCIP_CONTENT_TYPE: &str = "application/x-protobuf+scip";

/// `uncompressedSizeLimitBytes`: keep processing memory bounded.
pub const UNCOMPRESSED_SIZE_LIMIT_BYTES: i64 = 512 * 1024 * 1024;

/// `requeueDelay`.
pub const REQUEUE_DELAY: Duration = Duration::minutes(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreludeError {
    /// `Repos.Get: ...`
    RepoLookup(String),
    /// `repos.ResolveRev: ...`
    ResolveRevision(String),
    /// `store.Requeue: ...`
    Requeue(String),
    /// `gitserver.DefaultBranchContains: ...`
    DefaultBranchContains(String),
    /// `uploadStore.Get: ...`
    UploadGet(String),
    /// `failed to read upload file: ...`
    ReadUpload(String),
    /// `failed to create temporary file...`, `failed to copy buffer...`
    TempFile(String),
    /// `failed to decompress file "...": ...` (or the bare gzip error for the
    /// in-memory path, matching the baseline).
    Decompress(String),
    /// `LSIF support is deprecated`
    DeprecatedLsif,
    /// `unsupported content type "..."`
    UnsupportedContentType(String),
    /// `commit does not exist`
    CommitDoesNotExist,
    /// `failed to determine commit date: ...`
    CommitDate(String),
    /// `store.CommitDate: ...`
    StoreCommitDate(String),
}

impl fmt::Display for PreludeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RepoLookup(message) => write!(f, "Repos.Get: {message}"),
            Self::ResolveRevision(message) => write!(f, "repos.ResolveRev: {message}"),
            Self::Requeue(message) => write!(f, "store.Requeue: {message}"),
            Self::DefaultBranchContains(message) => {
                write!(f, "gitserver.DefaultBranchContains: {message}")
            }
            Self::UploadGet(message) => write!(f, "uploadStore.Get: {message}"),
            Self::ReadUpload(message) => write!(f, "failed to read upload file: {message}"),
            Self::TempFile(message) | Self::Decompress(message) => f.write_str(message),
            Self::DeprecatedLsif => f.write_str("LSIF support is deprecated"),
            Self::UnsupportedContentType(content_type) => {
                write!(f, "unsupported content type {content_type:?}")
            }
            Self::CommitDoesNotExist => f.write_str("commit does not exist"),
            Self::CommitDate(message) => write!(f, "failed to determine commit date: {message}"),
            Self::StoreCommitDate(message) => write!(f, "store.CommitDate: {message}"),
        }
    }
}

impl std::error::Error for PreludeError {}

#[derive(Debug)]
pub enum PreludeOutcome {
    /// `requeueIfCloningOrCommitUnknown` returned true.
    Requeued {
        reason: String,
        after: OffsetDateTime,
    },
    /// Everything up to `UpdateCommittedAt` succeeded. The SCIP writer is the
    /// next stage and needs the repository name (gitserver calls), the
    /// payload, and the upload's root.
    Ready {
        commit_date: OffsetDateTime,
        is_default_branch: bool,
        used_temp_file: bool,
        repo_name: String,
        data: UploadData,
    },
}

/// Handle to the stores and clients used by the prelude. The worker loop will
/// own one of these once the SCIP writer exists.
pub struct PreludeDeps {
    pub pool: PgPool,
    pub queue: Arc<UploadQueueStore>,
    pub git: Arc<GitserverClient>,
    pub upload_store: Arc<ObjectStore>,
}

impl PreludeDeps {
    pub fn new(
        pool: PgPool,
        queue: Arc<UploadQueueStore>,
        git: Arc<GitserverClient>,
        upload_store: Arc<ObjectStore>,
    ) -> Self {
        Self {
            pool,
            queue,
            git,
            upload_store,
        }
    }
}

/// `HandleRawUpload` through `UpdateCommittedAt`. See the module docs for the
/// stopping point.
pub async fn handle_raw_upload_prelude(
    deps: &PreludeDeps,
    upload: &Upload,
) -> Result<PreludeOutcome, PreludeError> {
    // 1. `repoStore.Get`.
    let repo_name: Option<String> =
        sqlx::query_scalar("SELECT name FROM repo WHERE id = $1 AND deleted_at IS NULL")
            .bind(upload.repository_id)
            .fetch_optional(&deps.pool)
            .await
            .map_err(|err| PreludeError::RepoLookup(err.to_string()))?;
    let repo_name = repo_name.ok_or_else(|| {
        PreludeError::RepoLookup(format!("repository not found: id={}", upload.repository_id))
    })?;

    // 2. `requeueIfCloningOrCommitUnknown`.
    match deps
        .git
        .resolve_revision(&repo_name, &upload.commit, true)
        .await
    {
        Ok(_) => {}
        Err(GitError::RevisionNotFound { .. }) => {
            return requeue(deps, upload, "commit not found").await;
        }
        Err(err) if err.is_clone_in_progress() => {
            return requeue(deps, upload, "repository still cloning").await;
        }
        Err(err) => return Err(PreludeError::ResolveRevision(err.to_string())),
    }

    // 3. `defaultBranchContains`.
    let is_default_branch = default_branch_contains(deps, &repo_name, &upload.commit).await?;

    // 4. `withUploadData` (read + gzip header validation). The payload is
    // returned for the SCIP stage; this slice drops it after validation.
    let data = read_upload_data(deps, upload).await?;
    let used_temp_file = data.used_temp_file;

    // 5. Content-type validation (inside the baseline's callback).
    if upload.content_type == LSIF_CONTENT_TYPE {
        return Err(PreludeError::DeprecatedLsif);
    }
    if upload.content_type != SCIP_CONTENT_TYPE {
        return Err(PreludeError::UnsupportedContentType(
            upload.content_type.clone(),
        ));
    }

    // 6. `GetCommit`, then `UpdateCommittedAt` outside the transaction.
    let commit = deps
        .git
        .get_commit(&repo_name, &upload.commit)
        .await
        .map_err(|err| {
            if err.is_revision_not_found() {
                PreludeError::CommitDoesNotExist
            } else {
                PreludeError::CommitDate(err.to_string())
            }
        })?;
    let Some(timestamp) = commit.committer_date else {
        return Err(PreludeError::CommitDate(
            "commit has no committer date".to_owned(),
        ));
    };
    let commit_date = OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .map_err(|err| PreludeError::CommitDate(err.to_string()))?;

    // `commitDate.Format(time.RFC3339)`; the text cast mirrors the baseline's
    // string binding into a timestamptz column.
    let formatted = commit_date
        .format(&Rfc3339)
        .map_err(|err| PreludeError::StoreCommitDate(err.to_string()))?;
    sqlx::query(
        "INSERT INTO codeintel_commit_dates(repository_id, commit_bytea, committed_at) \
         VALUES ($1, decode($2, 'hex'), $3::text::timestamptz) ON CONFLICT DO NOTHING",
    )
    .bind(upload.repository_id)
    .bind(&upload.commit)
    .bind(&formatted)
    .execute(&deps.pool)
    .await
    .map_err(|err| PreludeError::StoreCommitDate(err.to_string()))?;

    Ok(PreludeOutcome::Ready {
        commit_date,
        is_default_branch,
        used_temp_file,
        repo_name,
        data,
    })
}

async fn requeue(
    deps: &PreludeDeps,
    upload: &Upload,
    reason: &str,
) -> Result<PreludeOutcome, PreludeError> {
    let after = OffsetDateTime::now_utc() + REQUEUE_DELAY;
    deps.queue
        .requeue(upload.id, after)
        .await
        .map_err(|err| PreludeError::Requeue(err.to_string()))?;
    Ok(PreludeOutcome::Requeued {
        reason: reason.to_owned(),
        after,
    })
}

/// `defaultBranchContains`: the default branch must appear among the head refs
/// whose history contains the commit.
async fn default_branch_contains(
    deps: &PreludeDeps,
    repo_name: &str,
    commit: &str,
) -> Result<bool, PreludeError> {
    let default_branch = deps
        .git
        .default_branch(repo_name)
        .await
        .map_err(|err| PreludeError::DefaultBranchContains(err.to_string()))?;
    let refs = deps
        .git
        .list_refs(repo_name, true, false, &[], Some(commit))
        .await
        .map_err(|err| PreludeError::DefaultBranchContains(err.to_string()))?;
    Ok(refs.iter().any(|git_ref| git_ref.name == default_branch))
}

/// The raw upload payload, kept so the SCIP stage can decompress it in
/// multiple passes.
///
/// This replaces the baseline's `gzipReadSeeker` + `seekToStart`: instead of
/// seeking one gzip decoder to zero, every pass calls [`UploadData::open`] for
/// a fresh decoder over the same bytes. Small uploads stay in memory; when the
/// uncompressed size is (or may be) larger than
/// [`UNCOMPRESSED_SIZE_LIMIT_BYTES`] they are spooled to a temporary file.
#[derive(Debug)]
pub struct UploadData {
    source: UploadDataSource,
    /// Whether the temporary-file branch was taken (`shouldWriteToDisk`).
    pub used_temp_file: bool,
    /// `upload-<id>.lsif.gz`: the object-store key, used by delete-on-success.
    pub filename: String,
}

#[derive(Debug)]
enum UploadDataSource {
    Memory(Arc<Vec<u8>>),
    /// Kept alive for as long as the data is needed; `reopen` gives each pass
    /// an independent handle positioned at zero.
    Temp(tempfile::NamedTempFile),
}

/// A re-openable gzip payload: the Rust equivalent of the baseline's
/// `gzipReadSeeker` whose `seekToStart` re-creates the gzip reader over the
/// same bytes.
pub trait GzipSource: Send + Sync {
    fn open_gzip(&self) -> std::io::Result<GzReader>;
}

impl UploadData {
    /// An in-memory payload, for callers (and tests) that already hold the
    /// gzip bytes.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self {
            source: UploadDataSource::Memory(Arc::new(data)),
            used_temp_file: false,
            filename: String::new(),
        }
    }

    /// `newGzipReadSeeker`: a gzip decoder positioned at the start of the
    /// upload. flate2 parses the gzip header lazily on the first read.
    pub fn open(&self) -> std::io::Result<GzReader> {
        let inner: Box<dyn Read + Send> = match &self.source {
            UploadDataSource::Memory(data) => Box::new(SharedCursor {
                data: Arc::clone(data),
                position: 0,
            }),
            UploadDataSource::Temp(file) => Box::new(BufReader::new(file.reopen()?)),
        };
        Ok(GzDecoder::new(inner))
    }

    /// The temporary file path, when the payload was spooled to disk. Used for
    /// the baseline's `failed to decompress file %q` error message.
    pub fn temp_path(&self) -> Option<PathBuf> {
        match &self.source {
            UploadDataSource::Temp(file) => Some(file.path().to_path_buf()),
            UploadDataSource::Memory(_) => None,
        }
    }
}

impl GzipSource for UploadData {
    fn open_gzip(&self) -> std::io::Result<GzReader> {
        self.open()
    }
}

/// The decoder type returned by [`UploadData::open`].
pub type GzReader = GzDecoder<Box<dyn Read + Send>>;

/// A `Read` over a shared in-memory buffer with its own position, so each
/// gzip pass gets an independent cursor without copying the payload.
struct SharedCursor {
    data: Arc<Vec<u8>>,
    position: usize,
}

impl Read for SharedCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.data.len() {
            return Ok(0);
        }
        let remaining = &self.data[self.position..];
        let count = remaining.len().min(buf.len());
        buf[..count].copy_from_slice(&remaining[..count]);
        self.position += count;
        Ok(count)
    }
}

/// `withUploadData`'s read + size-decision logic, including gzip header
/// validation (`newGzipReadSeeker`), but without the callback: the payload is
/// returned so the SCIP stage can read it again.
async fn read_upload_data(deps: &PreludeDeps, upload: &Upload) -> Result<UploadData, PreludeError> {
    let filename = format!("upload-{}.lsif.gz", upload.id);
    let body = deps
        .upload_store
        .get(&filename)
        .await
        .map_err(|err| PreludeError::UploadGet(err.to_string()))?;

    // If the uncompressed size is known and too large, stream straight to a
    // temporary file (`shouldWriteToDisk`).
    let mut used_temp_file = upload
        .uncompressed_size
        .is_some_and(|size| size > UNCOMPRESSED_SIZE_LIMIT_BYTES);

    let mut body = body.into_async_read();
    let mut buffer: Option<Vec<u8>> = None;
    if !used_temp_file {
        let hint = upload.upload_size.unwrap_or(0).max(0) as usize;
        let mut data = Vec::with_capacity(hint);
        body.read_to_end(&mut data)
            .await
            .map_err(|err| PreludeError::ReadUpload(err.to_string()))?;

        if upload.uncompressed_size.is_none()
            && (data.len() as i64).saturating_mul(5) > UNCOMPRESSED_SIZE_LIMIT_BYTES
        {
            used_temp_file = true;
        }
        buffer = Some(data);
    }

    let source = if used_temp_file {
        let named = tempfile::Builder::new()
            .prefix(&format!("upload-{}-tmp.gz", upload.id))
            .tempfile()
            .map_err(|err| {
                PreludeError::TempFile(format!(
                    "failed to create temporary file to save upload for streaming: {err}"
                ))
            })?;
        {
            let std_file = named.reopen().map_err(|err| {
                PreludeError::TempFile(format!(
                    "failed to create temporary file to save upload for streaming: {err}"
                ))
            })?;
            let mut file = tokio::fs::File::from_std(std_file);
            match &buffer {
                Some(data) => file.write_all(data).await.map_err(|err| {
                    PreludeError::TempFile(format!(
                        "failed to copy buffer to temporary file: {err}"
                    ))
                })?,
                None => {
                    tokio::io::copy(&mut body, &mut file).await.map_err(|err| {
                        PreludeError::TempFile(format!(
                            "failed to copy buffer to temporary file: {err}"
                        ))
                    })?;
                }
            }
            file.flush().await.map_err(|err| {
                PreludeError::TempFile(format!("failed to copy buffer to temporary file: {err}"))
            })?;
        }
        // The handle written above starts at EOF; `UploadData::open` reopens
        // the file, which positions the reader at zero (the baseline's
        // `Seek(0, io.SeekStart)`).
        UploadDataSource::Temp(named)
    } else {
        UploadDataSource::Memory(Arc::new(buffer.unwrap_or_default()))
    };

    let data = UploadData {
        source,
        used_temp_file,
        filename,
    };

    // `newGzipReadSeeker`: reading the header is where a corrupt payload fails.
    // flate2 parses the gzip header on the first read, so read one byte.
    let reader = data.open().map_err(|err| match data.temp_path() {
        Some(path) => PreludeError::Decompress(format!(
            "failed to decompress file {:?}: {err}",
            path.display().to_string()
        )),
        None => PreludeError::Decompress(err.to_string()),
    })?;
    let path = data.temp_path().map(|path| path.display().to_string());
    if let Some(err) = gzip_header_error(reader, path.as_deref()) {
        return Err(err);
    }

    Ok(data)
}

/// `uncompressedSizeLimitBytes` is a `var` in the baseline for tests.
pub fn uncompressed_size_limit_bytes() -> i64 {
    UNCOMPRESSED_SIZE_LIMIT_BYTES
}

/// Force the gzip header parse: flate2 reads it on the first `read`.
fn gzip_header_error(mut decoder: impl Read, path: Option<&str>) -> Option<PreludeError> {
    let mut scratch = [0u8; 1];
    decoder.read(&mut scratch).err().map(|err| match path {
        Some(path) => {
            PreludeError::Decompress(format!("failed to decompress file {path:?}: {err}"))
        }
        None => PreludeError::Decompress(err.to_string()),
    })
}

/// `gzip.NewReader` over an in-memory buffer, kept for the SCIP writer.
pub fn gzip_reader(data: Vec<u8>) -> GzDecoder<Cursor<Vec<u8>>> {
    GzDecoder::new(Cursor::new(data))
}

/// Read a whole gzip stream into memory (used by tests and the SCIP writer).
pub fn read_gzip(mut reader: impl Read) -> std::io::Result<Vec<u8>> {
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    Ok(data)
}

/// Convenience for log messages: Go durations (`1m0s`).
pub fn format_go_duration(duration: StdDuration) -> String {
    let total = duration.as_secs();
    let minutes = total / 60;
    let seconds = total % 60;
    if minutes > 0 {
        format!("{minutes}m{seconds}s")
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_messages_match_baseline_wraps() {
        assert_eq!(
            PreludeError::RepoLookup("x".to_owned()).to_string(),
            "Repos.Get: x"
        );
        assert_eq!(
            PreludeError::UploadGet("failed to get object".to_owned()).to_string(),
            "uploadStore.Get: failed to get object"
        );
        assert_eq!(
            PreludeError::DeprecatedLsif.to_string(),
            "LSIF support is deprecated"
        );
        assert_eq!(
            PreludeError::UnsupportedContentType("text/plain".to_owned()).to_string(),
            "unsupported content type \"text/plain\""
        );
        assert_eq!(
            PreludeError::CommitDoesNotExist.to_string(),
            "commit does not exist"
        );
    }

    #[test]
    fn requeue_delay_is_one_minute() {
        assert_eq!(REQUEUE_DELAY, Duration::minutes(1));
        assert_eq!(
            format_go_duration(StdDuration::from_secs(60)),
            "1m0s".to_owned()
        );
    }

    #[test]
    fn gzip_header_validation() {
        let mut encoded = Vec::new();
        {
            let mut encoder =
                flate2::write::GzEncoder::new(&mut encoded, flate2::Compression::default());
            std::io::Write::write_all(&mut encoder, b"hello").unwrap();
            encoder.finish().unwrap();
        }
        assert!(gzip_header_error(gzip_reader(encoded), None).is_none());
        assert!(gzip_header_error(gzip_reader(b"not gzip".to_vec()), None).is_some());
    }
}
