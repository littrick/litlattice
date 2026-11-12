//! `gitserver.v1.GitserverService`: read path plus `UNIMPLEMENTED` stubs.
//!
//! Implemented so far: `Archive`, `ReadFile`, `ChangedFiles`, `ResolveRevision`,
//! `GetObject`, `DefaultBranch`, `MergeBase`, `MergeBaseOctopus`, `Stat`,
//! `ReadDir`, `ListRefs`, `GetCommit`, `RevAtTime`, `RawDiff`, `CommitLog`,
//! `ContributorCounts`, `FirstEverCommit`, `BehindAhead`, `Blame`, `DiskInfo`,
//! `RepoCloneProgress` and non-diff `Search` (M9.2). Every other RPC returns
//! `UNIMPLEMENTED`.

use std::pin::Pin;
use std::sync::Arc;

use prost::Message;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::TaskTracker;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};

use sg_proto::gitserver as gpb;

use crate::details::{
    create_commit_from_patch_error, file_not_found, repo_not_found, repo_not_found_with_progress,
    revision_not_found,
};
use crate::disk::disk_usage;
use crate::git::{
    pump_blame, pump_commit_log, pump_stdout, stat_path, BlameHunk, BlameRange, CommitLogOptions,
    CommitLogOrder, GitCommit, GitError, GitRef, GitRepo, RawDiffComparison, RefType, TreeEntry,
    CHUNK_MESSAGE_SIZE,
};
use crate::repo::{normalize_repo, repo_dir};
use crate::search::{self, SearchError};

type Stream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

pub struct Grpc {
    pub repos_dir: Arc<std::path::PathBuf>,
    pub tasks: TaskTracker,
    /// `RepositoryLocker`: status text for clones/fetches in flight.
    pub locker: Arc<crate::locker::RepositoryLocker>,
    /// `gs.svc`: `ResolveRevision` asks it to fetch on a missing revision.
    /// `None` only in unit tests that do not exercise fetching.
    pub fetch: Option<Arc<crate::fetchservice::FetchService>>,
    /// `Server.IsRepoCloneable` needs the repository store and the remote URL
    /// source; `None` only in unit tests.
    pub cloneable: Option<Arc<crate::cloneable::CloneableService>>,
    /// `Server.CreateCommitFromPatch` (the write path); `None` only in unit
    /// tests.
    pub patch: Option<Arc<crate::patch::PatchService>>,
    /// `Server`'s Perforce management RPCs; `None` only in unit tests.
    pub perforce: Option<Arc<crate::perforce::PerforceService>>,
    /// `defaultGitolite`: the `ListGitolite` repository source. Production is
    /// a [`crate::gitolite::GitoliteFetcher`]; tests inject a fixed lister.
    pub gitolite: Arc<dyn crate::gitolite::GitoliteRepoLister>,
}

impl Grpc {
    /// Baseline `checkRepoExists`: a repository is cloned when `<dir>/HEAD`
    /// exists.
    #[allow(clippy::result_large_err)] // `tonic::Status` is the RPC error type.
    fn repo(&self, name: &str) -> Result<GitRepo, Status> {
        let dir = repo_dir(&self.repos_dir, name).ok_or_else(|| repo_not_found(name))?;
        match std::fs::metadata(dir.join("HEAD")) {
            Ok(_) => Ok(GitRepo::new(dir)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(repo_not_found(name)),
            Err(err) => Err(Status::internal(format!(
                "failed to check if repo is cloned: {err}"
            ))),
        }
    }

    /// Baseline `checkRepoExists`: report `NotFound` with the locker's state
    /// when the repository is not cloned.
    #[allow(clippy::result_large_err)] // `tonic::Status` is the RPC error type.
    fn check_repo_exists(&self, name: &str) -> Result<(), Status> {
        match crate::fs::repo_cloned(&self.repos_dir, name) {
            Ok(true) => Ok(()),
            Ok(false) => {
                // An uncloned repository can only be locked while it is being
                // cloned.
                let status = self.locker.status(name);
                Err(repo_not_found_with_progress(
                    name,
                    status.is_some(),
                    status.as_deref().unwrap_or_default(),
                ))
            }
            Err(err) => Err(Status::internal(format!(
                "failed to check if repo is cloned: {err}"
            ))),
        }
    }
}

/// `isAlwaysCloningTest`: `NormalizeRepo(name).Equal("github.com/sourcegraphtest/alwayscloningtest")`.
pub(crate) fn is_always_cloning_test(name: &str) -> bool {
    normalize_repo(name).eq_ignore_ascii_case("github.com/sourcegraphtest/alwayscloningtest")
}

/// `req.GetConnectionDetails()`: a missing message reads as empty fields.
fn connection_details(
    details: Option<&gpb::PerforceConnectionDetails>,
) -> crate::perforce::ConnectionDetails {
    match details {
        Some(details) => crate::perforce::ConnectionDetails {
            p4port: details.p4port.clone(),
            p4user: details.p4user.clone(),
            p4passwd: details.p4passwd.clone(),
        },
        None => crate::perforce::ConnectionDetails::default(),
    }
}

/// `(*perforce.Protect).ToProto`.
fn protect_to_proto(protect: crate::perforce::PerforceProtect) -> gpb::PerforceProtect {
    gpb::PerforceProtect {
        level: protect.level,
        entity_type: protect.entity_type,
        entity_name: protect.entity_name,
        r#match: protect.match_,
        is_exclusion: protect.is_exclusion,
        host: protect.host,
    }
}

fn unimplemented(rpc: &str) -> Status {
    Status::unimplemented(format!(
        "{rpc} is not implemented by the Rust gitserver yet"
    ))
}

/// Translate a backend error into the status the baseline would send.
///
/// `repo` is the raw request repo name (the baseline passes it to
/// `RevisionNotFoundPayload`). RPCs that report file errors attach their own
/// [`file_not_found`] payload instead of using this helper.
fn git_status(err: GitError, repo: &str) -> Status {
    match err {
        GitError::RevisionNotFound(spec) => revision_not_found(repo, &spec),
        GitError::FileNotFound => Status::not_found("file not found"),
        GitError::Message(message) => Status::unknown(message),
    }
}

/// Approximates Go's `%q` (`strconv.Quote`) for revision specs, which the
/// baseline embeds in `ChangedFiles` error messages.
fn go_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for character in text.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\x07' => quoted.push_str("\\a"),
            '\x08' => quoted.push_str("\\b"),
            '\x0c' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\x0b' => quoted.push_str("\\v"),
            character if character.is_ascii_control() || character == '\x7f' => {
                quoted.push_str(&format!("\\x{:02x}", character as u32));
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
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

fn blame_hunk_proto(hunk: BlameHunk) -> gpb::BlameHunk {
    gpb::BlameHunk {
        start_line: hunk.start_line,
        end_line: hunk.end_line,
        start_byte: hunk.start_byte,
        end_byte: hunk.end_byte,
        commit: hunk.commit,
        author: Some(gpb::BlameAuthor {
            // git does not require author names to be valid UTF-8.
            name: hunk.author_name,
            email: hunk.author_email,
            date: Some(timestamp(hunk.author_date_seconds)),
        }),
        message: hunk.message,
        filename: hunk.filename,
        previous_commit: hunk.previous.map(|previous| gpb::PreviousCommit {
            commit: previous.commit,
            filename: previous.filename,
        }),
    }
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
            .map_err(|err| git_status(err, &request.repo))?;

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
            Err(GitError::FileNotFound) => {
                return Err(file_not_found(
                    &request.repo_name,
                    &request.commit,
                    &request.path,
                ));
            }
            Err(err) => return Err(git_status(err, &request.repo_name)),
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
        let files = match repo
            .changed_files(request.base.as_deref(), &request.head)
            .await
        {
            Ok(files) => files,
            Err(GitError::RevisionNotFound(spec)) => {
                // The baseline wraps the resolution error and copies
                // `err.Error()` into `Spec`. The backend resolves the base
                // first, so a failure with the base's spec came from there.
                let base = request.base.as_deref().map(String::from_utf8_lossy);
                let message = if base.as_deref() == Some(spec.as_str()) {
                    format!(
                        "failed to resolve base commit {}: revision not found: {}@{}",
                        go_quote(&spec),
                        request.repo_name,
                        spec
                    )
                } else {
                    format!(
                        "failed to resolve head commit {}: revision not found: {}@{}",
                        go_quote(&String::from_utf8_lossy(&request.head)),
                        request.repo_name,
                        spec
                    )
                };
                return Err(revision_not_found(&request.repo_name, &message));
            }
            Err(err) => return Err(git_status(err, &request.repo_name)),
        };

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

    /// Client-streaming: one `Metadata` message, then `Patch` messages. The
    /// baseline pipes the patch chunks into `git apply`'s stdin as they
    /// arrive; here they are buffered first (documented deviation) while
    /// preserving the baseline's error precedence: a reader error wins over
    /// the child's exit status because `os/exec.Cmd.Wait` returns it first.
    async fn create_commit_from_patch_binary(
        &self,
        request: GrpcRequest<tonic::Streaming<gpb::CreateCommitFromPatchBinaryRequest>>,
    ) -> Result<GrpcResponse<gpb::CreateCommitFromPatchBinaryResponse>, Status> {
        let mut stream = request.into_inner();

        // `s.Recv()`: an error (including `io.EOF` when the client sent
        // nothing) is returned unchanged; gRPC maps `io.EOF` to Unknown.
        let Some(first) = stream.message().await? else {
            return Err(Status::unknown("EOF"));
        };
        let metadata = match first.payload {
            Some(gpb::create_commit_from_patch_binary_request::Payload::Metadata(metadata)) => {
                metadata
            }
            _ => return Err(Status::invalid_argument("must send metadata event first")),
        };
        if metadata.repo.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        self.check_repo_exists(&metadata.repo)?;

        let mut patch = Vec::new();
        let mut patch_stream_error = None;
        loop {
            match stream.message().await {
                Ok(Some(message)) => match message.payload {
                    Some(gpb::create_commit_from_patch_binary_request::Payload::Patch(chunk)) => {
                        patch.extend_from_slice(&chunk.data);
                    }
                    _ => {
                        // `streamio`'s reader returns the status error to
                        // `exec`, which surfaces it as the command failure.
                        patch_stream_error = Some(
                            "rpc error: code = InvalidArgument desc = must only send patch events after metadata"
                                .to_owned(),
                        );
                        break;
                    }
                },
                Ok(None) => break,
                Err(status) => {
                    patch_stream_error = Some(format!(
                        "rpc error: code = {:?} desc = {}",
                        status.code(),
                        status.message()
                    ));
                    break;
                }
            }
        }

        let Some(patch_service) = &self.patch else {
            return Err(unimplemented("create_commit_from_patch_binary"));
        };
        let request = crate::patch::CreateCommitFromPatchRequest::from_metadata(metadata);
        let response = patch_service
            .create_commit_from_patch(&request, &patch, patch_stream_error)
            .await;
        if let Some(error) = &response.error {
            // `resp.ToProto()` reports the error and the handler returns its
            // status; the partially filled response is not sent.
            return Err(create_commit_from_patch_error(error));
        }
        Ok(GrpcResponse::new(
            gpb::CreateCommitFromPatchBinaryResponse {
                rev: response.rev,
                changelist_id: response.changelist_id,
            },
        ))
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
            .map_err(|err| git_status(err, &request.repo))?;
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
        request: GrpcRequest<gpb::IsRepoCloneableRequest>,
    ) -> Result<GrpcResponse<gpb::IsRepoCloneableResponse>, Status> {
        let repo = request.into_inner().repo;
        if repo.is_empty() {
            return Err(Status::invalid_argument("no Repo given"));
        }
        let Some(cloneable) = self.cloneable.as_ref() else {
            return Err(Status::unknown(
                "GetVCSSyncer: the database is not configured",
            ));
        };
        Ok(GrpcResponse::new(cloneable.is_repo_cloneable(&repo).await?))
    }

    async fn list_gitolite(
        &self,
        request: GrpcRequest<gpb::ListGitoliteRequest>,
    ) -> Result<GrpcResponse<gpb::ListGitoliteResponse>, Status> {
        let host = request.into_inner().gitolite_host;
        let repos = self
            .gitolite
            .list_repos(&host)
            .await
            .map_err(Status::internal)?;
        Ok(GrpcResponse::new(gpb::ListGitoliteResponse {
            repos: repos
                .into_iter()
                .map(|repo| gpb::GitoliteRepo {
                    name: repo.name,
                    url: repo.url,
                })
                .collect(),
        }))
    }

    type SearchStream = Stream<gpb::SearchResponse>;

    async fn search(
        &self,
        request: GrpcRequest<gpb::SearchRequest>,
    ) -> Result<GrpcResponse<Self::SearchStream>, Status> {
        let request = request.into_inner();
        // `SearchRequestFromProto` runs before the `repo` check, so a broken
        // query tree is reported even when the repository is missing.
        let args = search::SearchRequest::from_proto(&request)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if request.repo.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo)?;

        let tasks = self.tasks.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<gpb::SearchResponse, Status>>(64);
        self.tasks.spawn(async move {
            let sender = tx.clone();
            let result = search::search(&repo, &args, &tasks, |commit_match| {
                let sender = sender.clone();
                async move {
                    sender
                        .send(Ok(gpb::SearchResponse {
                            message: Some(gpb::search_response::Message::Match(commit_match)),
                        }))
                        .await
                        .is_ok()
                }
            })
            .await;
            match result {
                // The baseline always terminates a successful stream with
                // `limit_hit`, even when nothing matched.
                Ok(limit_hit) => {
                    let _ = tx
                        .send(Ok(gpb::SearchResponse {
                            message: Some(gpb::search_response::Message::LimitHit(limit_hit)),
                        }))
                        .await;
                }
                Err(SearchError::Unsupported(message)) => {
                    let _ = tx.send(Err(Status::unimplemented(message))).await;
                }
                Err(SearchError::InvalidArgument(message)) => {
                    let _ = tx.send(Err(Status::invalid_argument(message))).await;
                }
                Err(SearchError::Message(message)) => {
                    let _ = tx.send(Err(Status::unknown(message))).await;
                }
            }
        });
        Ok(GrpcResponse::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn repo_clone_progress(
        &self,
        request: GrpcRequest<gpb::RepoCloneProgressRequest>,
    ) -> Result<GrpcResponse<gpb::RepoCloneProgressResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }

        let always_cloning = is_always_cloning_test(&request.repo_name);

        // `repoCloned`: `<gitdir>/HEAD` exists. A name that escapes
        // `repos_dir` cannot have been cloned; the baseline panics there.
        let cloned = match repo_dir(&self.repos_dir, &request.repo_name) {
            Some(dir) => match std::fs::metadata(dir.join("HEAD")) {
                Ok(_) => true,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
                Err(err) => return Err(Status::unknown(format!("determine clone status: {err}"))),
            },
            None => false,
        };

        let mut response = gpb::RepoCloneProgressResponse {
            cloned,
            clone_in_progress: always_cloning,
            clone_progress: String::new(),
        };
        if always_cloning {
            response.clone_progress = "This will never finish cloning".to_owned();
        }
        // `repoCloneProgress`: the locker status only overrides the response
        // while the repository is not cloned (`!cloned && locked`).
        if let Some(clone_progress) = self.locker.status(&request.repo_name) {
            if !cloned {
                response.clone_in_progress = true;
                response.clone_progress = clone_progress;
            }
        }
        Ok(GrpcResponse::new(response))
    }

    async fn is_perforce_path_cloneable(
        &self,
        request: GrpcRequest<gpb::IsPerforcePathCloneableRequest>,
    ) -> Result<GrpcResponse<gpb::IsPerforcePathCloneableResponse>, Status> {
        let request = request.into_inner();
        if request.depot_path.is_empty() {
            return Err(Status::invalid_argument("no DepotPath given"));
        }
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("is_perforce_path_cloneable"));
        };
        perforce
            .is_depot_path_cloneable(
                &connection_details(request.connection_details.as_ref()),
                &request.depot_path,
            )
            .await
            .map_err(|err| Status::not_found(err.message()))?;
        Ok(GrpcResponse::new(gpb::IsPerforcePathCloneableResponse {}))
    }

    async fn check_perforce_credentials(
        &self,
        request: GrpcRequest<gpb::CheckPerforceCredentialsRequest>,
    ) -> Result<GrpcResponse<gpb::CheckPerforceCredentialsResponse>, Status> {
        let request = request.into_inner();
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("check_perforce_credentials"));
        };
        perforce
            .test_with_trust(&connection_details(request.connection_details.as_ref()))
            .await
            .map_err(|err| Status::invalid_argument(err.message()))?;
        Ok(GrpcResponse::new(gpb::CheckPerforceCredentialsResponse {}))
    }

    async fn perforce_users(
        &self,
        request: GrpcRequest<gpb::PerforceUsersRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceUsersResponse>, Status> {
        let request = request.into_inner();
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("perforce_users"));
        };
        let conn = connection_details(request.connection_details.as_ref());
        perforce
            .test_with_trust(&conn)
            .await
            .map_err(|err| Status::invalid_argument(err.message()))?;
        let users = perforce
            .users(&conn)
            .await
            .map_err(|err| Status::internal(err.message()))?;
        Ok(GrpcResponse::new(gpb::PerforceUsersResponse {
            users: users
                .into_iter()
                .map(|user| gpb::PerforceUser {
                    username: user.username,
                    email: user.email,
                })
                .collect(),
        }))
    }

    async fn perforce_protects_for_user(
        &self,
        request: GrpcRequest<gpb::PerforceProtectsForUserRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceProtectsForUserResponse>, Status> {
        let request = request.into_inner();
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("perforce_protects_for_user"));
        };
        let conn = connection_details(request.connection_details.as_ref());
        perforce
            .test_with_trust(&conn)
            .await
            .map_err(|err| Status::invalid_argument(err.message()))?;
        let protects = perforce
            .protects_for_user(&conn, &request.username)
            .await
            .map_err(|err| Status::unknown(err.message()))?;
        Ok(GrpcResponse::new(gpb::PerforceProtectsForUserResponse {
            protects: protects.into_iter().map(protect_to_proto).collect(),
        }))
    }

    async fn perforce_protects_for_depot(
        &self,
        request: GrpcRequest<gpb::PerforceProtectsForDepotRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceProtectsForDepotResponse>, Status> {
        let request = request.into_inner();
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("perforce_protects_for_depot"));
        };
        let conn = connection_details(request.connection_details.as_ref());
        perforce
            .test_with_trust(&conn)
            .await
            .map_err(|err| Status::invalid_argument(err.message()))?;
        let protects = perforce
            .protects_for_depot(&conn, &request.depot)
            .await
            .map_err(|err| Status::unknown(err.message()))?;
        Ok(GrpcResponse::new(gpb::PerforceProtectsForDepotResponse {
            protects: protects.into_iter().map(protect_to_proto).collect(),
        }))
    }

    async fn perforce_group_members(
        &self,
        request: GrpcRequest<gpb::PerforceGroupMembersRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceGroupMembersResponse>, Status> {
        let request = request.into_inner();
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("perforce_group_members"));
        };
        let conn = connection_details(request.connection_details.as_ref());
        perforce
            .test_with_trust(&conn)
            .await
            .map_err(|err| Status::invalid_argument(err.message()))?;
        let usernames = perforce
            .group_members(&conn, &request.group)
            .await
            .map_err(|err| Status::unknown(err.message()))?;
        Ok(GrpcResponse::new(gpb::PerforceGroupMembersResponse {
            usernames,
        }))
    }

    async fn is_perforce_super_user(
        &self,
        request: GrpcRequest<gpb::IsPerforceSuperUserRequest>,
    ) -> Result<GrpcResponse<gpb::IsPerforceSuperUserResponse>, Status> {
        let request = request.into_inner();
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("is_perforce_super_user"));
        };
        let conn = connection_details(request.connection_details.as_ref());
        perforce
            .test_with_trust(&conn)
            .await
            .map_err(|err| Status::invalid_argument(err.message()))?;
        perforce.user_is_super_user(&conn).await.map_err(|err| {
            if err.is_not_super_user() {
                Status::invalid_argument(err.message())
            } else {
                Status::internal(err.message())
            }
        })?;
        Ok(GrpcResponse::new(gpb::IsPerforceSuperUserResponse {}))
    }

    async fn perforce_get_changelist(
        &self,
        request: GrpcRequest<gpb::PerforceGetChangelistRequest>,
    ) -> Result<GrpcResponse<gpb::PerforceGetChangelistResponse>, Status> {
        let request = request.into_inner();
        let Some(perforce) = self.perforce.as_deref() else {
            return Err(unimplemented("perforce_get_changelist"));
        };
        let conn = connection_details(request.connection_details.as_ref());
        perforce
            .test_with_trust(&conn)
            .await
            .map_err(|err| Status::invalid_argument(err.message()))?;
        let changelist = perforce
            .changelist_by_id(&conn, &request.changelist_id)
            .await
            .map_err(|err| Status::unknown(err.message()))?;
        Ok(GrpcResponse::new(gpb::PerforceGetChangelistResponse {
            changelist: Some(gpb::PerforceChangelist {
                id: changelist.id,
                creation_date: Some(prost_types::Timestamp {
                    seconds: changelist.creation_date_seconds,
                    nanos: 0,
                }),
                state: changelist.state.as_proto(),
                author: changelist.author,
                title: changelist.title,
                message: changelist.message,
            }),
        }))
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
        let merge_base_commit_sha = repo
            .merge_base(&base, &head)
            .await
            .map_err(|err| git_status(err, &request.repo_name))?;
        Ok(GrpcResponse::new(gpb::MergeBaseResponse {
            merge_base_commit_sha,
        }))
    }

    type BlameStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<gpb::BlameResponse, Status>> + Send>>;

    async fn blame(
        &self,
        request: GrpcRequest<gpb::BlameRequest>,
    ) -> Result<GrpcResponse<Self::BlameStream>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.commit.is_empty() {
            return Err(Status::invalid_argument("commit must be specified"));
        }
        if request.path.is_empty() {
            return Err(Status::invalid_argument("path must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let range = request.range.map(|range| BlameRange {
            start_line: range.start_line,
            end_line: range.end_line,
        });
        let child = match repo
            .blame(
                &request.commit,
                &request.path,
                request.ignore_whitespace,
                range,
            )
            .await
        {
            Ok(child) => child,
            Err(GitError::FileNotFound) => {
                return Err(file_not_found(
                    &request.repo_name,
                    &request.commit,
                    &request.path,
                ));
            }
            Err(err) => return Err(git_status(err, &request.repo_name)),
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<gpb::BlameResponse, Status>>(64);
        let repo_name = request.repo_name.clone();
        self.tasks.spawn(async move {
            let sender = tx.clone();
            let result = pump_blame(child, |hunk| {
                let sender = sender.clone();
                async move {
                    sender
                        .send(Ok(gpb::BlameResponse {
                            hunk: Some(blame_hunk_proto(hunk)),
                        }))
                        .await
                        .is_ok()
                }
            })
            .await;
            if let Err(err) = result {
                let _ = tx.send(Err(git_status(err, &repo_name))).await;
            }
        });
        Ok(GrpcResponse::new(Box::pin(ReceiverStream::new(rx))))
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
            .map_err(|err| git_status(err, &request.repo_name))?;
        let commit = repo
            .rev_parse_head()
            .await
            .map_err(|err| git_status(err, &request.repo_name))?;
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
            .map_err(|err| git_status(err, &request.repo_name))?;
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

        // First, try to resolve the revspec.
        let mut resolved = repo.resolve_revision(&spec).await;
        if let Err(GitError::RevisionNotFound(_)) = resolved {
            // `ensure_revision` asks us to fetch before retrying.
            if request.ensure_revision.unwrap_or(false) {
                let did_update = match &self.fetch {
                    Some(fetch) => fetch.ensure_revision(&request.repo_name, &spec).await,
                    // Without a fetch service the baseline's "ensure failed"
                    // path applies: do not retry.
                    None => false,
                };
                if did_update {
                    resolved = repo.resolve_revision(&spec).await;
                }
            }
        }
        let commit_sha = resolved.map_err(|err| git_status(err, &request.repo_name))?;
        Ok(GrpcResponse::new(gpb::ResolveRevisionResponse {
            commit_sha,
        }))
    }

    async fn rev_at_time(
        &self,
        request: GrpcRequest<gpb::RevAtTimeRequest>,
    ) -> Result<GrpcResponse<gpb::RevAtTimeResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let spec = String::from_utf8_lossy(&request.rev_spec).into_owned();
        // A missing timestamp arrives as `None`; the baseline calls `AsTime()`
        // on the nil pointer, which is the Unix epoch.
        let before_seconds = request.time.as_ref().map_or(0, |time| time.seconds);
        let commit_sha = match repo.rev_at_time(&spec, before_seconds).await {
            Ok(commit_sha) => commit_sha,
            Err(GitError::RevisionNotFound(spec)) => {
                return Err(revision_not_found(&request.repo_name, &spec))
            }
            // The baseline wraps every other fault as `Internal`.
            Err(err) => return Err(Status::internal(err.message())),
        };
        Ok(GrpcResponse::new(gpb::RevAtTimeResponse { commit_sha }))
    }

    type RawDiffStream = Stream<gpb::RawDiffResponse>;

    async fn raw_diff(
        &self,
        request: GrpcRequest<gpb::RawDiffRequest>,
    ) -> Result<GrpcResponse<Self::RawDiffStream>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.base_rev_spec.is_empty() {
            return Err(Status::invalid_argument("base_rev_spec must be specified"));
        }
        if request.head_rev_spec.is_empty() {
            return Err(Status::invalid_argument("head_rev_spec must be specified"));
        }
        let comparison =
            match gpb::raw_diff_request::ComparisonType::try_from(request.comparison_type) {
                Ok(gpb::raw_diff_request::ComparisonType::OnlyInHead) => {
                    RawDiffComparison::OnlyInHead
                }
                Ok(gpb::raw_diff_request::ComparisonType::Intersection) => {
                    RawDiffComparison::Intersection
                }
                // The baseline rejects the unspecified value despite the proto
                // comment mentioning a fallback.
                Ok(gpb::raw_diff_request::ComparisonType::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument(
                        "comparison_type must be specified",
                    ))
                }
            };
        let repo = self.repo(&request.repo_name)?;
        let base = String::from_utf8_lossy(&request.base_rev_spec).into_owned();
        let head = String::from_utf8_lossy(&request.head_rev_spec).into_owned();
        let child = repo
            .raw_diff(
                &base,
                &head,
                comparison,
                request.inter_hunk_context.unwrap_or(3),
                request.context_lines.unwrap_or(3),
                &request.paths,
            )
            .await
            .map_err(|err| git_status(err, &request.repo_name))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<gpb::RawDiffResponse, Status>>(64);
        self.tasks.spawn(async move {
            let sender = tx.clone();
            let result = pump_stdout(child, |data| {
                let sender = sender.clone();
                async move {
                    sender
                        .send(Ok(gpb::RawDiffResponse { chunk: data }))
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

    type CommitLogStream = Stream<gpb::CommitLogResponse>;

    async fn commit_log(
        &self,
        request: GrpcRequest<gpb::CommitLogRequest>,
    ) -> Result<GrpcResponse<Self::CommitLogStream>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        if request.ranges.is_empty() && !request.all_refs {
            return Err(Status::invalid_argument("must specify ranges or all_refs"));
        }
        if !request.ranges.is_empty() && request.all_refs {
            return Err(Status::invalid_argument(
                "cannot specify both ranges and all_refs",
            ));
        }
        let order = match gpb::commit_log_request::CommitLogOrder::try_from(request.order) {
            Ok(gpb::commit_log_request::CommitLogOrder::Unspecified) => CommitLogOrder::Default,
            Ok(gpb::commit_log_request::CommitLogOrder::CommitDate) => CommitLogOrder::CommitDate,
            Ok(gpb::commit_log_request::CommitLogOrder::TopoDate) => CommitLogOrder::TopoDate,
            Err(_) => return Err(Status::invalid_argument("unknown order")),
        };
        let repo = self.repo(&request.repo_name)?;
        let spec = request
            .ranges
            .iter()
            .map(|range| String::from_utf8_lossy(range).into_owned())
            .collect::<Vec<_>>()
            .join(" ");
        let options = CommitLogOptions {
            ranges: request.ranges,
            all_refs: request.all_refs,
            after_seconds: request.after.as_ref().map_or(0, |time| time.seconds),
            before_seconds: request.before.as_ref().map_or(0, |time| time.seconds),
            max_commits: request.max_commits,
            skip: request.skip,
            follow_only_first_parent: request.follow_only_first_parent,
            include_modified_files: request.include_modified_files,
            order,
            message_query: request.message_query,
            author_query: request.author_query,
            path: request.path,
            follow_path_renames: request.follow_path_renames,
        };
        let child = repo
            .commit_log(&options)
            .await
            .map_err(|err| git_status(err, &request.repo_name))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<gpb::CommitLogResponse, Status>>(64);
        let repo_name = request.repo_name.clone();
        self.tasks.spawn(async move {
            let sender = tx.clone();
            let result = pump_commit_log(child, spec, |commit| {
                let sender = sender.clone();
                async move {
                    let (commit, modified_files) = git_commit_proto(commit);
                    sender
                        .send(Ok(gpb::CommitLogResponse {
                            commits: vec![gpb::GetCommitResponse {
                                commit: Some(commit),
                                modified_files,
                            }],
                        }))
                        .await
                        .is_ok()
                }
            })
            .await;
            match result {
                Ok(()) => {}
                Err(GitError::RevisionNotFound(spec)) => {
                    let _ = tx.send(Err(revision_not_found(&repo_name, &spec))).await;
                }
                Err(err) => {
                    let _ = tx.send(Err(Status::unknown(err.message()))).await;
                }
            }
        });
        Ok(GrpcResponse::new(Box::pin(ReceiverStream::new(rx))))
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
            .map_err(|err| git_status(err, &request.repo_name))?;

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

    async fn contributor_counts(
        &self,
        request: GrpcRequest<gpb::ContributorCountsRequest>,
    ) -> Result<GrpcResponse<gpb::ContributorCountsResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let range = String::from_utf8_lossy(&request.range).into_owned();
        // A missing timestamp arrives as `None`; the baseline calls `AsTime()`
        // on the nil pointer, which is the Unix epoch.
        let after_seconds = request.after.as_ref().map_or(0, |time| time.seconds);
        let counts = match repo
            .contributor_counts(&range, after_seconds, &request.path)
            .await
        {
            Ok(counts) => counts,
            Err(GitError::RevisionNotFound(spec)) => {
                return Err(revision_not_found(&request.repo_name, &spec))
            }
            // The baseline returns every other fault unchanged (`Unknown`).
            Err(err) => return Err(Status::unknown(err.message())),
        };
        Ok(GrpcResponse::new(gpb::ContributorCountsResponse {
            counts: counts
                .into_iter()
                .map(|count| gpb::ContributorCount {
                    author: Some(gpb::GitSignature {
                        name: count.name.into_bytes(),
                        email: count.email.into_bytes(),
                        date: None,
                    }),
                    count: count.count,
                })
                .collect(),
        }))
    }

    async fn first_ever_commit(
        &self,
        request: GrpcRequest<gpb::FirstEverCommitRequest>,
    ) -> Result<GrpcResponse<gpb::FirstEverCommitResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let id = match repo.first_ever_commit().await {
            Ok(id) => id,
            Err(GitError::RevisionNotFound(spec)) => {
                return Err(revision_not_found(&request.repo_name, &spec))
            }
            Err(err) => return Err(Status::unknown(err.message())),
        };
        let commit = match repo.get_commit(&id, false).await {
            Ok(commit) => commit,
            Err(GitError::RevisionNotFound(spec)) => {
                return Err(revision_not_found(&request.repo_name, &spec))
            }
            Err(err) => return Err(Status::unknown(err.message())),
        };
        let (commit, _modified_files) = git_commit_proto(commit);
        Ok(GrpcResponse::new(gpb::FirstEverCommitResponse {
            commit: Some(commit),
        }))
    }

    async fn behind_ahead(
        &self,
        request: GrpcRequest<gpb::BehindAheadRequest>,
    ) -> Result<GrpcResponse<gpb::BehindAheadResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        let repo = self.repo(&request.repo_name)?;
        let left = String::from_utf8_lossy(&request.left).into_owned();
        let right = String::from_utf8_lossy(&request.right).into_owned();
        let behind_ahead = match repo.behind_ahead(&left, &right).await {
            Ok(behind_ahead) => behind_ahead,
            Err(GitError::RevisionNotFound(spec)) => {
                // The baseline copies `err.Error()` into `Spec` here, which
                // is why the payload contains the full repo-qualified text.
                return Err(revision_not_found(
                    &request.repo_name,
                    &format!("revision not found: {}@{spec}", request.repo_name),
                ));
            }
            Err(err) => return Err(git_status(err, &request.repo_name)),
        };
        Ok(GrpcResponse::new(gpb::BehindAheadResponse {
            behind: behind_ahead.behind,
            ahead: behind_ahead.ahead,
        }))
    }

    async fn disk_info(
        &self,
        _request: GrpcRequest<gpb::DiskInfoRequest>,
    ) -> Result<GrpcResponse<gpb::DiskInfoResponse>, Status> {
        let usage = disk_usage(&self.repos_dir).map_err(|err| {
            Status::unknown(format!("statfs {}: {err}", self.repos_dir.display()))
        })?;
        Ok(GrpcResponse::new(gpb::DiskInfoResponse {
            total_space: usage.total_space,
            free_space: usage.free_space,
            percent_used: usage.percent_used,
        }))
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
        let entry = match repo.stat(&request.commit_sha, &request.path).await {
            Ok(entry) => entry,
            Err(GitError::FileNotFound) => {
                // The baseline reports the cleaned path it handed to
                // `ls-tree`, not the raw request path.
                return Err(file_not_found(
                    &request.repo_name,
                    &request.commit_sha,
                    &stat_path(&request.path),
                ));
            }
            Err(err) => return Err(git_status(err, &request.repo_name)),
        };
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
        let entries = match repo
            .read_dir(
                &request.commit_sha,
                request.path.as_deref().unwrap_or_default(),
                request.recursive,
            )
            .await
        {
            Ok(entries) => entries,
            Err(GitError::FileNotFound) => {
                // `ReadDir` reports the raw request path.
                return Err(file_not_found(
                    &request.repo_name,
                    &request.commit_sha,
                    request.path.as_deref().unwrap_or_default(),
                ));
            }
            Err(err) => return Err(git_status(err, &request.repo_name)),
        };

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

    async fn merge_base_octopus(
        &self,
        request: GrpcRequest<gpb::MergeBaseOctopusRequest>,
    ) -> Result<GrpcResponse<gpb::MergeBaseOctopusResponse>, Status> {
        let request = request.into_inner();
        if request.repo_name.is_empty() {
            return Err(Status::invalid_argument("repo must be specified"));
        }
        // The baseline checks the count before touching the repository.
        if request.revspecs.len() < 2 {
            return Err(Status::invalid_argument(
                "at least 2 revspecs must be specified",
            ));
        }
        let repo = self.repo(&request.repo_name)?;
        let revspecs = request
            .revspecs
            .iter()
            .map(|revspec| String::from_utf8_lossy(revspec).into_owned())
            .collect::<Vec<_>>();
        let merge_base_commit_sha = repo
            .merge_base_octopus(&revspecs)
            .await
            .map_err(|err| git_status(err, &request.repo_name))?;
        Ok(GrpcResponse::new(gpb::MergeBaseOctopusResponse {
            merge_base_commit_sha,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::is_always_cloning_test;
    use super::*;
    use gpb::gitserver_service_server::GitserverService as _;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn matches_the_always_cloning_test_repo() {
        assert!(is_always_cloning_test(
            "github.com/sourcegraphtest/alwayscloningtest"
        ));
        // `NormalizeRepo` lowercases GitHub paths and `Equal` folds case.
        assert!(is_always_cloning_test(
            "GitHub.com/SourcegraphTest/AlwaysCloningTest"
        ));
        assert!(is_always_cloning_test(
            "/github.com//sourcegraphtest/alwayscloningtest/"
        ));
        assert!(!is_always_cloning_test("github.com/sourcegraphtest/other"));
        assert!(!is_always_cloning_test(
            "gitlab.com/sourcegraphtest/alwayscloningtest"
        ));
    }

    /// A backend that must never run in these tests (`ensure_revision` bails
    /// out before fetching when auto-updates are disabled).
    struct NeverCalledBackend;

    #[async_trait::async_trait]
    impl crate::update::UpdateBackend for NeverCalledBackend {
        async fn repo_cloned(&self, _repo: &str) -> Result<bool, GitError> {
            panic!("repo_cloned must not be called")
        }

        async fn clone(
            &self,
            _repo: &str,
            _lock: &crate::locker::RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), GitError> {
            panic!("clone must not be called")
        }

        async fn fetch(
            &self,
            _repo: &str,
            _lock: &crate::locker::RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), GitError> {
            panic!("fetch must not be called")
        }

        async fn set_last_error(&self, _repo: &str, _error: &str) {}
    }

    fn repo_with_a_commit(root: &std::path::Path) -> std::path::PathBuf {
        let repos_dir = root.join("repos");
        let work = repos_dir.join("example.com/o/r");
        std::fs::create_dir_all(&work).expect("mkdir");
        let git = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .args(args)
                .current_dir(&work)
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@example.com")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@example.com")
                .status()
                .expect("run git");
            assert!(status.success(), "git {args:?} failed");
        };
        git(&["init", "-q"]);
        std::fs::write(work.join("a.txt"), b"hello\n").expect("write");
        git(&["add", "a.txt"]);
        git(&["commit", "-q", "-m", "first"]);
        repos_dir
    }

    fn service(
        repos_dir: &std::path::Path,
        fetch: Option<Arc<crate::fetchservice::FetchService>>,
    ) -> Grpc {
        Grpc {
            repos_dir: Arc::new(repos_dir.to_path_buf()),
            tasks: TaskTracker::new(),
            locker: crate::locker::RepositoryLocker::new(),
            fetch,
            cloneable: None,
            patch: None,
            perforce: None,
            gitolite: Arc::new(crate::gitolite::GitoliteFetcher::with_default_command()),
        }
    }

    fn fetch_service(
        repos_dir: &std::path::Path,
        disable: bool,
    ) -> Arc<crate::fetchservice::FetchService> {
        Arc::new(crate::fetchservice::FetchService {
            repos_dir: Arc::new(repos_dir.to_path_buf()),
            updater: Arc::new(crate::update::UpdateServer {
                locker: crate::locker::RepositoryLocker::new(),
                limiter: crate::limiter::MutableLimiter::new(5),
                backend: Arc::new(NeverCalledBackend),
                cancelled: CancellationToken::new(),
            }),
            tasks: TaskTracker::new(),
            store: None,
            live: live_config(disable),
        })
    }

    fn live_config(disable: bool) -> Arc<sg_config::LiveConfig> {
        let raw = sg_config::RawUnified {
            site: serde_json::json!({ "disableAutoGitUpdates": disable }).to_string(),
            ..Default::default()
        };
        Arc::new(sg_config::LiveConfig::new(
            sg_config::Unified::parse(&raw).expect("site config"),
        ))
    }

    #[allow(clippy::result_large_err)] // `tonic::Status` is the RPC error type.
    async fn resolve(
        service: &Grpc,
        ensure_revision: bool,
        rev: &str,
    ) -> Result<GrpcResponse<gpb::ResolveRevisionResponse>, Status> {
        service
            .resolve_revision(GrpcRequest::new(gpb::ResolveRevisionRequest {
                repo_name: "example.com/o/r".to_owned(),
                rev_spec: rev.as_bytes().to_vec(),
                ensure_revision: Some(ensure_revision),
            }))
            .await
    }

    #[tokio::test]
    async fn resolves_revisions_without_fetching() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = repo_with_a_commit(tmp.path());
        let service = service(&repos_dir, None);

        assert_eq!(
            resolve(&service, true, "HEAD")
                .await
                .expect("main")
                .into_inner()
                .commit_sha
                .len(),
            40
        );
        // No fetch service: the failed ensure must not turn into a retry.
        let err = resolve(&service, true, "deadbeef")
            .await
            .expect_err("missing");
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(err.message(), "revision not found");
        // `ensure_revision=false` behaves the same without a revision.
        let err = resolve(&service, false, "deadbeef")
            .await
            .expect_err("missing");
        assert_eq!(err.code(), tonic::Code::NotFound);

        service.tasks.close();
        service.tasks.wait().await;
    }

    /// With `DisableAutoGitUpdates` the ensure step reports "did not update",
    /// so the second resolution attempt is skipped and the RPC still fails.
    #[tokio::test]
    async fn skips_the_retry_when_auto_updates_are_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = repo_with_a_commit(tmp.path());
        let fetch = fetch_service(&repos_dir, true);
        let service = service(&repos_dir, Some(Arc::clone(&fetch)));

        let err = resolve(&service, true, "deadbeef")
            .await
            .expect_err("missing");
        assert_eq!(err.code(), tonic::Code::NotFound);

        fetch.tasks.close();
        fetch.tasks.wait().await;
        service.tasks.close();
        service.tasks.wait().await;
    }
}
