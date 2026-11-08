#![forbid(unsafe_code)]

//! `HandleRawUpload`'s second half and the `workerutil.Handler` shell
//! (`internal/codeintel/uploads/internal/background/processor/job_worker_handler.go`):
//!
//! 1. `getChildren` — gitserver `ReadDir` for every requested directory,
//!    de-duplicated by name, then [`parse_directory_children`].
//! 2. `prepareSCIPDataStream` with the upload's root.
//! 3. `writeSCIPDocuments` in its own code-intel transaction; a unique
//!    constraint violation means a previous attempt already wrote the index,
//!    so the error is logged and processing continues.
//! 4. The uploads-database transaction: `DeleteOverlappingCompletedUploads`,
//!    `UpdatePackages`, `UpdatePackageReferences`,
//!    `InsertDependencySyncingJob`, `SetRepositoryAsDirty`.
//! 5. Delete-on-success: `upload-<id>.lsif.gz` is removed from object
//!    storage; a failure only logs a warning.
//!
//! The handler owns the upload-processor budget: `PreDequeue` refuses work
//! when the budget is exhausted and restricts candidates to
//! `upload_size <= remaining`, while `PreHandle`/`PostHandle` subtract/add
//! the gzip size. The baseline's upload-size gauge metric is not ported
//! (module 07 §19.3).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use sg_git::{Error as GitError, FileInfo, GitserverClient};

use crate::codegraph::{write_scip_documents, CodeGraphError};
use crate::pathexistence::DirectoryChildren;
use crate::prelude::{handle_raw_upload_prelude, PreludeOutcome};
use crate::queue::Upload;
use crate::scip_stream::prepare_scip_data_stream;
use crate::uploads_store;
use crate::worker::{HandlerError, PreDequeue, UploadHandler};

/// The stores and clients `HandleRawUpload` needs.
pub struct HandlerDeps {
    /// Frontend pool, queue store, gitserver client and upload store.
    pub prelude: crate::prelude::PreludeDeps,
    /// Code-intel pool; the SCIP tables live here.
    pub codeintel: PgPool,
}

/// `handler` from `job_worker_handler.go`.
pub struct UploadProcessorHandler {
    deps: HandlerDeps,
    enable_budget: bool,
    budget_remaining: AtomicI64,
}

impl UploadProcessorHandler {
    pub fn new(deps: HandlerDeps, budget_max: i64) -> Self {
        Self {
            deps,
            enable_budget: budget_max > 0,
            budget_remaining: AtomicI64::new(budget_max),
        }
    }

    /// `Handler.Handle` → `HandleRawUpload`.
    async fn handle_raw_upload(
        &self,
        upload: &Upload,
        cancel: &CancellationToken,
    ) -> Result<(), String> {
        let prelude = &self.deps.prelude;

        let outcome = handle_raw_upload_prelude(prelude, upload)
            .await
            .map_err(|err| err.to_string())?;
        let PreludeOutcome::Ready {
            repo_name, data, ..
        } = outcome
        else {
            // Requeued: the record is back in `queued`, so the worker's
            // `MarkComplete` (guarded on `state = 'processing'`) is a no-op.
            return Ok(());
        };
        check_cancel(cancel)?;

        let get_children = {
            let git = Arc::clone(&prelude.git);
            let repo_name = repo_name.clone();
            let commit = upload.commit.clone();
            move |dirnames: Vec<String>| {
                let git = Arc::clone(&git);
                let repo_name = repo_name.clone();
                let commit = commit.clone();
                async move { read_directory_children(&git, &repo_name, &commit, &dirnames).await }
            }
        };

        let stream = prepare_scip_data_stream(data, &upload.root, get_children)
            .await
            .map_err(format_prepare_error)?;
        check_cancel(cancel)?;

        // `writeSCIPDocuments` opens its own transaction on the code-intel
        // database (different database from the uploads block below).
        let packages = {
            let mut tx = self
                .deps
                .codeintel
                .begin()
                .await
                .map_err(|err| err.to_string())?;
            match write_scip_documents(&mut tx, upload.id, &upload.indexer, &stream).await {
                Ok(packages) => {
                    tx.commit()
                        .await
                        .map_err(|err| format!("committing SCIP data: {err}"))?;
                    packages
                }
                Err(err) if is_unique_constraint_violation(&err) => {
                    // A previous attempt wrote this index but failed before
                    // the uploads transaction; the index data is complete.
                    // The baseline continues with the zero-value package
                    // data, so the package rows are skipped but the
                    // dependency job and dirty marker are still written.
                    tracing::warn!(id = upload.id, "SCIP data already exists for upload record");
                    tx.rollback().await.map_err(|err| err.to_string())?;
                    crate::scip_stream::ProcessedPackageData::default()
                }
                Err(err) => {
                    tx.rollback().await.map_err(|err| err.to_string())?;
                    return Err(err.to_string());
                }
            }
        };
        check_cancel(cancel)?;

        let key = format!("upload-{}.lsif.gz", upload.id);
        let mut tx = prelude.pool.begin().await.map_err(|err| err.to_string())?;

        uploads_store::delete_overlapping_completed_uploads(
            &mut tx,
            upload.repository_id,
            &upload.commit,
            &upload.root,
            &upload.indexer,
        )
        .await
        .map_err(|err| format!("store.DeleteOverlappingCompletedUploads: {err}"))?;

        uploads_store::update_packages(&mut tx, upload.id, &packages.packages)
            .await
            .map_err(|err| format!("store.UpdatePackages: {err}"))?;

        uploads_store::update_package_references(&mut tx, upload.id, &packages.package_references)
            .await
            .map_err(|err| format!("store.UpdatePackageReferences: {err}"))?;

        uploads_store::insert_dependency_syncing_job(&mut tx, upload.id)
            .await
            .map_err(|err| format!("store.InsertDependencyIndexingJob: {err}"))?;

        uploads_store::set_repository_as_dirty(&mut tx, upload.repository_id)
            .await
            .map_err(|err| format!("store.MarkRepositoryAsDirty: {err}"))?;

        tx.commit().await.map_err(|err| err.to_string())?;
        check_cancel(cancel)?;

        // `withUploadData`: the object is deleted only after the callback
        // (everything above) succeeded; a delete failure is a warning.
        if let Err(err) = prelude.upload_store.delete(&key).await {
            tracing::warn!(filename = %key, error = %err, "Failed to delete upload file");
        }

        Ok(())
    }
}

#[async_trait]
impl UploadHandler for UploadProcessorHandler {
    async fn handle(&self, upload: Upload, cancel: CancellationToken) -> Result<(), HandlerError> {
        let future = self.handle_raw_upload(&upload, &cancel);
        tokio::pin!(future);
        // `ctx.Err()` checks in the baseline: cancellation wins between and
        // during the handler's awaits.
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(HandlerError::canceled()),
            result = &mut future => result,
        };
        match result {
            Ok(()) => Ok(()),
            Err(_) if cancel.is_cancelled() => Err(HandlerError::canceled()),
            Err(message) => Err(HandlerError::retryable(message)),
        }
    }

    /// `PreDequeue`: `(upload_size IS NULL OR upload_size <= <remaining>)`,
    /// or no dequeue at all once the budget is exhausted.
    fn pre_dequeue(&self) -> PreDequeue {
        if !self.enable_budget {
            return PreDequeue::Dequeue {
                max_upload_size: None,
            };
        }
        let remaining = self.budget_remaining.load(Ordering::SeqCst);
        if remaining <= 0 {
            PreDequeue::Decline
        } else {
            PreDequeue::Dequeue {
                max_upload_size: Some(remaining),
            }
        }
    }

    /// `PreHandle`: reserve the upload's gzip size.
    fn pre_handle(&self, upload: &Upload) {
        let gzip_size = upload.upload_size.unwrap_or(0);
        self.budget_remaining.fetch_sub(gzip_size, Ordering::SeqCst);
    }

    /// `PostHandle`: return the reservation.
    fn post_handle(&self, upload: &Upload) {
        let gzip_size = upload.upload_size.unwrap_or(0);
        self.budget_remaining.fetch_add(gzip_size, Ordering::SeqCst);
    }
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), String> {
    if cancel.is_cancelled() {
        Err("context canceled".to_owned())
    } else {
        Ok(())
    }
}

fn format_prepare_error(err: crate::scip_stream::PrepareScipError<String>) -> String {
    use crate::scip_stream::PrepareScipError;
    match err {
        PrepareScipError::Open(err) => format!("prepareSCIPDataStream: {err}"),
        PrepareScipError::Parse(err) => format!("prepareSCIPDataStream: {err}"),
        PrepareScipError::MissingMetadata => {
            "prepareSCIPDataStream: index metadata is missing".to_owned()
        }
        PrepareScipError::GetChildren(err) => format!("prepareSCIPDataStream: {err}"),
    }
}

/// `isUniqueConstraintViolation`: PostgreSQL SQLSTATE 23505.
fn is_unique_constraint_violation(err: &CodeGraphError) -> bool {
    match err {
        CodeGraphError::Database(err) => err
            .as_database_error()
            .and_then(|err| err.code())
            .is_some_and(|code| code == "23505"),
        _ => false,
    }
}

/// `getChildren`'s gitserver leg: `ReadDir` every requested directory,
/// ignoring directories that do not exist in the repository (the upload may
/// mention `node_modules`-style paths that were never checked in), and
/// de-duplicating file names across directories.
async fn read_directory_children(
    git: &GitserverClient,
    repo_name: &str,
    commit: &str,
    dirnames: &[String],
) -> Result<DirectoryChildren, String> {
    let mut all_file_infos: Vec<FileInfo> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for dirname in dirnames {
        match git.read_dir(repo_name, commit, dirname).await {
            Ok(file_infos) => {
                for file_info in file_infos {
                    if seen.insert(file_info.name.clone()) {
                        all_file_infos.push(file_info);
                    }
                }
            }
            // `os.IsNotExist(err)`.
            Err(GitError::NotFound(_)) => continue,
            Err(err) => return Err(format!("gitserverClient.ReadDir: {err}")),
        }
    }

    Ok(parse_directory_children(dirnames, &all_file_infos))
}

/// `parseDirectoryChildren`: group the flat gitserver listing under the
/// requested directories.
///
/// Every requested directory gets an entry (empty when nothing matched).
/// Directory names are visited longest-first so a path lands under its most
/// specific enclosing directory; paths without a `/` go to the root entry
/// when `""` was requested.
pub fn parse_directory_children(
    dirnames: &[String],
    file_infos: &[FileInfo],
) -> HashMap<String, Vec<String>> {
    let mut children: HashMap<String, Vec<String>> = HashMap::with_capacity(dirnames.len());
    for dirname in dirnames {
        children.insert(dirname.clone(), Vec::new());
    }

    let mut sorted_dirnames: Vec<&String> = dirnames.iter().collect();
    sorted_dirnames.sort_by_key(|dirname| std::cmp::Reverse(dirname.len()));

    for file_info in file_infos {
        let path = &file_info.name;
        if path.contains('/') {
            for dirname in &sorted_dirnames {
                if path.starts_with(dirname.as_str()) {
                    if let Some(entries) = children.get_mut(dirname.as_str()) {
                        entries.push(path.clone());
                    }
                    break;
                }
            }
        } else if sorted_dirnames
            .last()
            .is_some_and(|dirname| dirname.is_empty())
        {
            // The root input directory is necessarily last after sorting.
            if let Some(entries) = children.get_mut("") {
                entries.push(path.clone());
            }
        }
    }

    children
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_infos(names: &[&str]) -> Vec<FileInfo> {
        names
            .iter()
            .map(|name| FileInfo {
                name: (*name).to_owned(),
            })
            .collect()
    }

    #[test]
    fn every_requested_directory_gets_an_entry() {
        let children =
            parse_directory_children(&["".to_owned(), "a".to_owned()], &file_infos(&["a/b.txt"]));
        assert_eq!(children.get(""), Some(&Vec::<String>::new()));
        assert_eq!(children.get("a"), Some(&vec!["a/b.txt".to_owned()]));
    }

    #[test]
    fn longest_prefix_wins() {
        let dirnames = vec!["a".to_owned(), "a/b".to_owned()];
        let children = parse_directory_children(&dirnames, &file_infos(&["a/b/c.txt", "a/d.txt"]));
        assert_eq!(children.get("a/b"), Some(&vec!["a/b/c.txt".to_owned()]));
        assert_eq!(children.get("a"), Some(&vec!["a/d.txt".to_owned()]));
    }

    #[test]
    fn root_also_catches_paths_at_the_empty_prefix() {
        // Baseline quirk: `strings.HasPrefix(path, "")` is always true, so a
        // nested path requested against the `""` directory lands in the root
        // bucket even though it contains a slash. Nested directories normally
        // outrank `""` because they are longer.
        let dirnames = vec!["".to_owned()];
        let children =
            parse_directory_children(&dirnames, &file_infos(&["README.md", "a/b.txt", "empty"]));
        assert_eq!(
            children.get(""),
            Some(&vec![
                "README.md".to_owned(),
                "a/b.txt".to_owned(),
                "empty".to_owned()
            ])
        );
    }

    #[test]
    fn unmatched_directories_stay_empty() {
        let dirnames = vec!["x".to_owned(), "y".to_owned()];
        let children = parse_directory_children(&dirnames, &file_infos(&["z/file.txt"]));
        assert!(children["x"].is_empty());
        assert!(children["y"].is_empty());
    }
}
