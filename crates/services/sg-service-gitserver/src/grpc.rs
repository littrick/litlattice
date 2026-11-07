//! `gitserver.v1.GitserverService`: read path plus `UNIMPLEMENTED` stubs.
//!
//! Implemented so far: `Archive`, `ReadFile`, `ChangedFiles`, `ResolveRevision`,
//! `GetObject`, `DefaultBranch`, `MergeBase`, `Stat`, `ReadDir`, `ListRefs` and
//! `GetCommit` (M9.2). Every other RPC returns `UNIMPLEMENTED`.

use std::pin::Pin;
use std::sync::Arc;

use prost::Message;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::TaskTracker;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};

use sg_proto::gitserver as gpb;

use crate::git::{
    pump_stdout, GitCommit, GitError, GitRef, GitRepo, RefType, TreeEntry, CHUNK_MESSAGE_SIZE,
};
use crate::repo::repo_dir;

type Stream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

pub struct Grpc {
    pub repos_dir: Arc<std::path::PathBuf>,
    pub tasks: TaskTracker,
}

impl Grpc {
    /// Baseline `checkRepoExists`: a repository is cloned when `<dir>/HEAD`
    /// exists.
    #[allow(clippy::result_large_err)] // `tonic::Status` is the RPC error type.
    fn repo(&self, name: &str) -> Result<GitRepo, Status> {
        let dir =
            repo_dir(&self.repos_dir, name).ok_or_else(|| Status::not_found("repo not found"))?;
        match std::fs::metadata(dir.join("HEAD")) {
            Ok(_) => Ok(GitRepo::new(dir)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(Status::not_found("repo not found"))
            }
            Err(err) => Err(Status::internal(format!(
                "failed to check if repo is cloned: {err}"
            ))),
        }
    }
}

fn unimplemented(rpc: &str) -> Status {
    Status::unimplemented(format!(
        "{rpc} is not implemented by the Rust gitserver yet"
    ))
}

fn git_status(err: GitError) -> Status {
    match err {
        GitError::RevisionNotFound(_) => Status::not_found("revision not found"),
        GitError::FileNotFound => Status::not_found("file not found"),
        GitError::Message(message) => Status::unknown(message),
    }
}

fn file_info(entry: TreeEntry) -> gpb::FileInfo {
    gpb::FileInfo {
        name: entry.name,
        size: entry.size,
        mode: entry.mode,
        blob_oid: entry.blob_oid.unwrap_or_default(),
        submodule: entry.submodule.map(|submodule| gpb::GitSubmodule {
            url: submodule.url,
            path: submodule.path,
            commit_sha: submodule.commit_sha,
        }),
    }
}

fn timestamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

fn git_ref_proto(git_ref: GitRef) -> gpb::GitRef {
    let ref_type = match git_ref.ref_type {
        RefType::Unspecified => gpb::git_ref::RefType::Unspecified,
        RefType::Branch => gpb::git_ref::RefType::Branch,
        RefType::Tag => gpb::git_ref::RefType::Tag,
    };
    gpb::GitRef {
        ref_name: git_ref.ref_name,
        short_ref_name: git_ref.short_ref_name,
        target_commit: git_ref.target_commit,
        ref_oid: git_ref.ref_oid,
        created_at: Some(timestamp(git_ref.created_at_seconds)),
        ref_type: ref_type as i32,
        is_head: git_ref.is_head,
    }
}

fn git_commit_proto(commit: GitCommit) -> (gpb::GitCommit, Vec<Vec<u8>>) {
    let signature = |signature: crate::git::Signature| gpb::GitSignature {
        name: signature.name,
        email: signature.email,
        date: Some(timestamp(signature.date_seconds)),
    };
    (
        gpb::GitCommit {
            oid: commit.oid,
            author: Some(signature(commit.author)),
            committer: Some(signature(commit.committer)),
            message: commit.message,
            parents: commit.parents,
        },
        commit.modified_files,
    )
}

#[tonic::async_trait]
impl gpb::gitserver_service_server::GitserverService for Grpc {
    // ------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------

    type ArchiveStream = Stream<gpb::ArchiveResponse>;

    async fn archive(
        &self,
        request: GrpcRequest<gpb::ArchiveRequest>,
    ) -> Result<GrpcResponse<Self::ArchiveStream>, Status> {
        let request = request.into_inner();
        if request.repo.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.treeish.is_empty() {
            return Err(Status::invalid_argument("treeish must be specified"));
        }
        let format = match gpb::ArchiveFormat::try_from(request.format) {
            Ok(gpb::ArchiveFormat::Zip) => "zip",
            Ok(gpb::ArchiveFormat::Tar) => "tar",
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unknown archive format {:?}",
                    request.format
                )))
            }
        };
        let repo = self.repo(&request.repo)?;
        let child = repo
            .archive(format, &request.treeish, &request.paths)
            .await
            .map_err(git_status)?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<gpb::ArchiveResponse, Status>>(64);
        self.tasks.spawn(async move {
            let sender = tx.clone();
            let result = pump_stdout(child, |data| {
                // A failed send means the client disconnected; stop git.
                let sender = sender.clone();
                async move { sender.send(Ok(gpb::ArchiveResponse { data })).await.is_ok() }
            })
            .await;
            if let Err(message) = result {
                let _ = tx.send(Err(Status::unknown(message))).await;
            }
        });
        Ok(GrpcResponse::new(Box::pin(ReceiverStream::new(rx))))
    }

    type ReadFileStream = Stream<gpb::ReadFileResponse>;

    async fn read_file(
        &self,
        request: GrpcRequest<gpb::ReadFileRequest>,
    ) -> Result<GrpcResponse<Self::ReadFileStream>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.path.is_empty() {
            return Err(Status::invalid_argument("path must be specified"));
        }
        if request.commit.is_empty() {
            return Err(Status::invalid_argument("commit must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let child = match repo.read_file(&request.commit, &request.path).await {
            Ok(Some(child)) => child,
            // Submodule: baseline streams an empty file.
            Ok(None) => {
                return Ok(GrpcResponse::new(Box::pin(tokio_stream::empty())));
            }
            Err(err) => return Err(git_status(err)),
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<gpb::ReadFileResponse, Status>>(64);
        self.tasks.spawn(async move {
            let sender = tx.clone();
            let result = pump_stdout(child, |data| {
                let sender = sender.clone();
                async move {
                    sender
                        .send(Ok(gpb::ReadFileResponse { data }))
                        .await
                        .is_ok()
                }
            })
            .await;
            if let Err(message) = result {
                let _ = tx.send(Err(Status::unknown(message))).await;
            }
        });
        Ok(GrpcResponse::new(Box::pin(ReceiverStream::new(rx))))
    }

    type ChangedFilesStream = Stream<gpb::ChangedFilesResponse>;

    async fn changed_files(
        &self,
        request: GrpcRequest<gpb::ChangedFilesRequest>,
    ) -> Result<GrpcResponse<Self::ChangedFilesStream>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.head.is_empty() {
            return Err(Status::invalid_argument(
                "head (<tree-ish>) must be specified",
            ));
        }
        let repo = self.repo(&request.repo_name)?;
        let files = repo
            .changed_files(request.base.as_deref(), &request.head)
            .await
            .map_err(git_status)?;

        // Chunk at 1 MiB like the baseline `internal/grpc/chunk`.
        let mut responses = Vec::new();
        let mut chunk = Vec::new();
        let mut size = 0usize;
        for (code, path) in files {
            let status = match code {
                b'A' => gpb::changed_file::Status::Added,
                b'M' => gpb::changed_file::Status::Modified,
                b'D' => gpb::changed_file::Status::Deleted,
                _ => gpb::changed_file::Status::TypeChanged,
            };
            let file = gpb::ChangedFile {
                path,
                status: status as i32,
            };
            let file_size = file.encoded_len();
            if size + file_size >= CHUNK_MESSAGE_SIZE {
                responses.push(Ok(gpb::ChangedFilesResponse {
                    files: std::mem::take(&mut chunk),
                }));
                size = 0;
            }
            size += file_size;
            chunk.push(file);
        }
        if !chunk.is_empty() {
            responses.push(Ok(gpb::ChangedFilesResponse { files: chunk }));
        }
        Ok(GrpcResponse::new(Box::pin(tokio_stream::iter(responses))))
    }

    // ------------------------------------------------------------------
    // Stubs
    // ------------------------------------------------------------------

    async fn create_commit_from_patch_binary(
        &self,
        _request: GrpcRequest<tonic::Streaming<gpb::CreateCommitFromPatchBinaryRequest>>,
    ) -> Result<GrpcResponse<gpb::CreateCommitFromPatchBinaryResponse>, Status> {
        Err(unimplemented("create_commit_from_patch_binary"))
    }

    async fn disk_info(
        &self,
        _request: GrpcRequest<gpb::DiskInfoRequest>,
    ) -> Result<GrpcResponse<gpb::DiskInfoResponse>, Status> {
        Err(unimplemented("disk_info"))
    }

    async fn get_object(
        &self,
        request: GrpcRequest<gpb::GetObjectRequest>,
    ) -> Result<GrpcResponse<gpb::GetObjectResponse>, Status> {
        let request = request.into_inner();
        if request.repo.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.object_name.is_empty() {
            return Err(Status::invalid_argument("object name must be specified"));
        }
        let repo = self.repo(&request.repo)?;
        let (id, object_type) = repo
            .get_object(&request.object_name)
            .await
            .map_err(git_status)?;
        let r#type = match object_type.as_str() {
            "commit" => gpb::git_object::ObjectType::Commit,
            "tag" => gpb::git_object::ObjectType::Tag,
            "tree" => gpb::git_object::ObjectType::Tree,
            "blob" => gpb::git_object::ObjectType::Blob,
            _ => gpb::git_object::ObjectType::Unspecified,
        };
        Ok(GrpcResponse::new(gpb::GetObjectResponse {
            object: Some(gpb::GitObject {
                id,
                r#type: r#type as i32,
            }),
        }))
    }

    async fn is_repo_cloneable(
        &self,
        _request: GrpcRequest<gpb::IsRepoCloneableRequest>,
    ) -> Result<GrpcResponse<gpb::IsRepoCloneableResponse>, Status> {
        Err(unimplemented("is_repo_cloneable"))
    }

    async fn list_gitolite(
        &self,
        _request: GrpcRequest<gpb::ListGitoliteRequest>,
    ) -> Result<GrpcResponse<gpb::ListGitoliteResponse>, Status> {
        Err(unimplemented("list_gitolite"))
    }

    type SearchStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<gpb::SearchResponse, Status>> + Send>>;

    async fn search(
        &self,
        _request: GrpcRequest<gpb::SearchRequest>,
    ) -> Result<GrpcResponse<Self::SearchStream>, Status> {
        Err(unimplemented("search"))
    }

    async fn repo_clone_progress(
        &self,
        _request: GrpcRequest<gpb::RepoCloneProgressRequest>,
    ) -> Result<GrpcResponse<gpb::RepoCloneProgressResponse>, Status> {
        Err(unimplemented("repo_clone_progress"))
    }

    async fn is_perforce_path_cloneable(
        &self,
        _request: GrpcRequest<gpb::IsPerforcePathCloneableRequest>,
    ) -> Result<GrpcResponse<gpb::IsPerforcePathCloneableResponse>, Status> {
        Err(unimplemented("is_perforce_path_cloneable"))
    }

    async fn check_perforce_credentials(
        &self,
        _request: GrpcRequest<gpb::CheckPerforceCredentialsRequest>,
    ) -> Result<GrpcResponse<gpb::CheckPerforceCredentialsResponse>, Status> {
        Err(unimplemented("check_perforce_credentials"))
    }

    async fn perforce_users(
        &self,
        _request: GrpcRequest<gpb::PerforceUsersRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceUsersResponse>, Status> {
        Err(unimplemented("perforce_users"))
    }

    async fn perforce_protects_for_user(
        &self,
        _request: GrpcRequest<gpb::PerforceProtectsForUserRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceProtectsForUserResponse>, Status> {
        Err(unimplemented("perforce_protects_for_user"))
    }

    async fn perforce_protects_for_depot(
        &self,
        _request: GrpcRequest<gpb::PerforceProtectsForDepotRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceProtectsForDepotResponse>, Status> {
        Err(unimplemented("perforce_protects_for_depot"))
    }

    async fn perforce_group_members(
        &self,
        _request: GrpcRequest<gpb::PerforceGroupMembersRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceGroupMembersResponse>, Status> {
        Err(unimplemented("perforce_group_members"))
    }

    async fn is_perforce_super_user(
        &self,
        _request: GrpcRequest<gpb::IsPerforceSuperUserRequest>,
    ) -> Result<GrpcResponse<gpb::IsPerforceSuperUserResponse>, Status> {
        Err(unimplemented("is_perforce_super_user"))
    }

    async fn perforce_get_changelist(
        &self,
        _request: GrpcRequest<gpb::PerforceGetChangelistRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceGetChangelistResponse>, Status> {
        Err(unimplemented("perforce_get_changelist"))
    }

    async fn merge_base(
        &self,
        request: GrpcRequest<gpb::MergeBaseRequest>,
    ) -> Result<GrpcResponse<gpb::MergeBaseResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.base.is_empty() {
            return Err(Status::invalid_argument("base must be specified"));
        }
        if request.head.is_empty() {
            return Err(Status::invalid_argument("head must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let base = String::from_utf8_lossy(&request.base).into_owned();
        let head = String::from_utf8_lossy(&request.head).into_owned();
        let merge_base_commit_sha = repo.merge_base(&base, &head).await.map_err(git_status)?;
        Ok(GrpcResponse::new(gpb::MergeBaseResponse {
            merge_base_commit_sha,
        }))
    }

    type BlameStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<gpb::BlameResponse, Status>> + Send>>;

    async fn blame(
        &self,
        _request: GrpcRequest<gpb::BlameRequest>,
    ) -> Result<GrpcResponse<Self::BlameStream>, Status> {
        Err(unimplemented("blame"))
    }

    async fn default_branch(
        &self,
        request: GrpcRequest<gpb::DefaultBranchRequest>,
    ) -> Result<GrpcResponse<gpb::DefaultBranchResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let ref_name = repo
            .symbolic_ref_head(request.short_ref)
            .await
            .map_err(git_status)?;
        let commit = repo.rev_parse_head().await.map_err(git_status)?;
        Ok(GrpcResponse::new(gpb::DefaultBranchResponse {
            ref_name,
            commit,
        }))
    }

    async fn get_commit(
        &self,
        request: GrpcRequest<gpb::GetCommitRequest>,
    ) -> Result<GrpcResponse<gpb::GetCommitResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.commit.is_empty() {
            return Err(Status::invalid_argument("commit must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let commit = repo
            .get_commit(&request.commit, request.include_modified_files)
            .await
            .map_err(git_status)?;
        let (commit, modified_files) = git_commit_proto(commit);
        Ok(GrpcResponse::new(gpb::GetCommitResponse {
            commit: Some(commit),
            modified_files,
        }))
    }

    async fn resolve_revision(
        &self,
        request: GrpcRequest<gpb::ResolveRevisionRequest>,
    ) -> Result<GrpcResponse<gpb::ResolveRevisionResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let spec = String::from_utf8_lossy(&request.rev_spec).into_owned();
        // `ensure_revision` would fetch missing revisions in the baseline;
        // the Rust gitserver has no write path yet, so it behaves like a
        // failed ensure and reports `NotFound`.
        let commit_sha = repo.resolve_revision(&spec).await.map_err(git_status)?;
        Ok(GrpcResponse::new(gpb::ResolveRevisionResponse {
            commit_sha,
        }))
    }

    type ListRefsStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<gpb::ListRefsResponse, Status>> + Send>>;

    async fn list_refs(
        &self,
        request: GrpcRequest<gpb::ListRefsRequest>,
    ) -> Result<GrpcResponse<Self::ListRefsStream>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let refs = repo
            .list_refs(
                request.heads_only,
                request.tags_only,
                &request.points_at_commit,
                request.contains_sha.as_deref(),
            )
            .await
            .map_err(git_status)?;

        // Chunk at 1 MiB like the baseline `internal/grpc/chunk`.
        let mut responses = Vec::new();
        let mut chunk = Vec::new();
        let mut size = 0usize;
        for git_ref in refs {
            let git_ref = git_ref_proto(git_ref);
            let ref_size = git_ref.encoded_len();
            if size + ref_size >= CHUNK_MESSAGE_SIZE {
                responses.push(Ok(gpb::ListRefsResponse {
                    refs: std::mem::take(&mut chunk),
                }));
                size = 0;
            }
            size += ref_size;
            chunk.push(git_ref);
        }
        if !chunk.is_empty() {
            responses.push(Ok(gpb::ListRefsResponse { refs: chunk }));
        }
        Ok(GrpcResponse::new(Box::pin(tokio_stream::iter(responses))))
    }

    async fn rev_at_time(
        &self,
        _request: GrpcRequest<gpb::RevAtTimeRequest>,
    ) -> Result<GrpcResponse<gpb::RevAtTimeResponse>, Status> {
        Err(unimplemented("rev_at_time"))
    }

    type RawDiffStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<gpb::RawDiffResponse, Status>> + Send>>;

    async fn raw_diff(
        &self,
        _request: GrpcRequest<gpb::RawDiffRequest>,
    ) -> Result<GrpcResponse<Self::RawDiffStream>, Status> {
        Err(unimplemented("raw_diff"))
    }

    async fn contributor_counts(
        &self,
        _request: GrpcRequest<gpb::ContributorCountsRequest>,
    ) -> Result<GrpcResponse<gpb::ContributorCountsResponse>, Status> {
        Err(unimplemented("contributor_counts"))
    }

    async fn first_ever_commit(
        &self,
        _request: GrpcRequest<gpb::FirstEverCommitRequest>,
    ) -> Result<GrpcResponse<gpb::FirstEverCommitResponse>, Status> {
        Err(unimplemented("first_ever_commit"))
    }

    async fn behind_ahead(
        &self,
        _request: GrpcRequest<gpb::BehindAheadRequest>,
    ) -> Result<GrpcResponse<gpb::BehindAheadResponse>, Status> {
        Err(unimplemented("behind_ahead"))
    }

    async fn stat(
        &self,
        request: GrpcRequest<gpb::StatRequest>,
    ) -> Result<GrpcResponse<gpb::StatResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.commit_sha.is_empty() {
            return Err(Status::invalid_argument("commit_sha must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let entry = repo
            .stat(&request.commit_sha, &request.path)
            .await
            .map_err(git_status)?;
        Ok(GrpcResponse::new(gpb::StatResponse {
            file_info: Some(file_info(entry)),
        }))
    }

    type ReadDirStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<gpb::ReadDirResponse, Status>> + Send>>;

    async fn read_dir(
        &self,
        request: GrpcRequest<gpb::ReadDirRequest>,
    ) -> Result<GrpcResponse<Self::ReadDirStream>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.commit_sha.is_empty() {
            return Err(Status::invalid_argument("commit_sha must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let entries = repo
            .read_dir(
                &request.commit_sha,
                request.path.as_deref().unwrap_or_default(),
                request.recursive,
            )
            .await
            .map_err(git_status)?;

        // Chunk at 1 MiB like the baseline `internal/grpc/chunk`.
        let mut responses = Vec::new();
        let mut chunk = Vec::new();
        let mut size = 0usize;
        for entry in entries {
            let file = file_info(entry);
            let file_size = file.encoded_len();
            if size + file_size >= CHUNK_MESSAGE_SIZE {
                responses.push(Ok(gpb::ReadDirResponse {
                    file_info: std::mem::take(&mut chunk),
                }));
                size = 0;
            }
            size += file_size;
            chunk.push(file);
        }
        if !chunk.is_empty() {
            responses.push(Ok(gpb::ReadDirResponse { file_info: chunk }));
        }
        Ok(GrpcResponse::new(Box::pin(tokio_stream::iter(responses))))
    }

    type CommitLogStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<gpb::CommitLogResponse, Status>> + Send>>;

    async fn commit_log(
        &self,
        _request: GrpcRequest<gpb::CommitLogRequest>,
    ) -> Result<GrpcResponse<Self::CommitLogStream>, Status> {
        Err(unimplemented("commit_log"))
    }

    async fn merge_base_octopus(
        &self,
        _request: GrpcRequest<gpb::MergeBaseOctopusRequest>,
    ) -> Result<GrpcResponse<gpb::MergeBaseOctopusResponse>, Status> {
        Err(unimplemented("merge_base_octopus"))
    }
}
