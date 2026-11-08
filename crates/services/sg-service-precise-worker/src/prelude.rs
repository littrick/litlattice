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
//! The slice deliberately stops before `prepareSCIPDataStream`. The
//! delete-on-success side effect of `withUploadData` therefore also waits for
//! the SCIP writer (9.7e): this function must not delete the object it has
//! not finished processing.

use std::fmt;
use std::io::{BufReader, Cursor, Read};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use flate2::bufread::GzDecoder;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreludeOutcome {
    /// `requeueIfCloningOrCommitUnknown` returned true.
    Requeued {
        reason: String,
        after: OffsetDateTime,
    },
    /// Everything up to `UpdateCommittedAt` succeeded. The SCIP writer is the
    /// next stage.
    Ready {
        commit_date: OffsetDateTime,
        is_default_branch: bool,
        used_temp_file: bool,
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

    // 4. `withUploadData` (read + gzip header validation).
    let (gzip_error, used_temp_file) = read_upload_data(deps, upload).await?;
    if let Some(err) = gzip_error {
        return Err(err);
    }

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

/// The reader chosen by `withUploadData` for the payload.
enum GzipSource {
    Memory(Cursor<Vec<u8>>),
    File(std::fs::File, String),
}

/// Mirrors `withUploadData`'s read + size-decision logic and returns the gzip
/// header validation error (if any) plus whether the temporary-file branch was
/// taken. The payload itself is dropped: this slice only validates it.
async fn read_upload_data(
    deps: &PreludeDeps,
    upload: &Upload,
) -> Result<(Option<PreludeError>, bool), PreludeError> {
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
        // Reopen to read from the start (the baseline seeks to 0).
        let reopened = std::fs::File::open(named.path())
            .map_err(|err| PreludeError::TempFile(format!("failed to seek to start: {err}")))?;
        GzipSource::File(reopened, named.path().display().to_string())
    } else {
        GzipSource::Memory(Cursor::new(buffer.unwrap_or_default()))
    };

    // `newGzipReadSeeker`: reading the header is where a corrupt payload fails.
    // flate2 parses the gzip header on the first read, so read one byte.
    let gzip_error = match source {
        GzipSource::Memory(cursor) => gzip_header_error(GzDecoder::new(cursor), None),
        GzipSource::File(file, path) => {
            gzip_header_error(GzDecoder::new(BufReader::new(file)), Some(&path))
        }
    };

    Ok((gzip_error, used_temp_file))
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
