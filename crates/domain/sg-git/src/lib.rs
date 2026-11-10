#![forbid(unsafe_code)]

//! `sg-git`: gitserver gRPC client shared by the Rust services.
//!
//! Callers pass the addresses they learned from site configuration
//! (`SRC_GIT_SERVERS` in the pilot binaries, `ServiceConnections.GitServers`
//! in repo-updater). Repository-to-shard selection is
//! [`addresses::GitserverAddresses::addr_for_repo`], the same MD5 mapping the
//! baseline uses, so every process reaches the same shard for a repository.

pub mod addresses;

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::pin::Pin;

use bytes::Bytes;
use prost::Message;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};

use sg_proto::gitserver as pb;

type Client = pb::gitserver_service_client::GitserverServiceClient<tonic::transport::Channel>;

/// A stream of tar archive bytes (`gitserver.Archive`).
pub type TarByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

/// Error returned by every gitserver operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The requested file or revision does not exist (gRPC `NotFound`).
    NotFound(String),
    /// `gitdomain.RepoNotExistError`; `clone_in_progress` mirrors the
    /// `RepoNotFoundPayload` detail.
    RepoNotFound {
        repo: String,
        clone_in_progress: bool,
    },
    /// `gitdomain.RevisionNotFoundError` (`RevisionNotFoundPayload`).
    RevisionNotFound { repo: String, spec: String },
    /// `*os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}` from a
    /// `FileNotFoundPayload`; only `gitserver.Client.StreamBlameFile` maps the
    /// payload this way (the handler checks `os.IsNotExist`).
    FileNotExist { path: String },
    /// Any other failure.
    Message(String),
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::NotFound(_) | Self::RepoNotFound { .. } | Self::RevisionNotFound { .. }
        )
    }

    /// `gitdomain.IsCloneInProgress`.
    pub fn is_clone_in_progress(&self) -> bool {
        matches!(
            self,
            Self::RepoNotFound {
                clone_in_progress: true,
                ..
            }
        )
    }

    pub fn is_revision_not_found(&self) -> bool {
        matches!(self, Self::RevisionNotFound { .. })
    }

    /// `os.IsNotExist` for the `*os.PathError` that `StreamBlameFile` returns
    /// for a `FileNotFoundPayload` (and for nothing else).
    pub fn is_file_not_exist(&self) -> bool {
        matches!(self, Self::FileNotExist { .. })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message) | Self::Message(message) => f.write_str(message),
            // `os.PathError.Error()`.
            Self::FileNotExist { path } => write!(f, "open {path}: file does not exist"),
            Self::RepoNotFound { repo, .. } => write!(f, "repo not found: {repo}"),
            Self::RevisionNotFound { repo, spec } => {
                write!(f, "revision not found: {repo}@{spec}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for String {
    fn from(err: Error) -> Self {
        err.to_string()
    }
}

/// `gitdomain.RefType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefType {
    Unknown,
    Branch,
    Tag,
}

/// `gitdomain.Ref`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRef {
    pub name: String,
    pub short_name: String,
    pub target_commit: String,
    pub ref_oid: String,
    /// `gitdomain.Ref.CreatedDate`; absent (treated as the Unix epoch, like
    /// `(*timestamppb.Timestamp)(nil).AsTime()`) when the server sent none.
    pub created_at: Option<prost_types::Timestamp>,
    pub ref_type: RefType,
    pub is_head: bool,
}

/// `gitdomain.ProtoFileInfoToFS` keeps the full file info; only the name is
/// used by the code-intel upload handler (`fs.FileInfo.Name`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInfo {
    pub name: String,
}

/// One entry of a `ReadDir` listing with the directory flag preserved.
///
/// The proto carries no explicit `is_directory` field: the baseline's
/// `fs.FileInfo.IsDir` reads `os.ModeDir` out of `FileInfo.mode`, where
/// gitserver encodes `uint32(fi.Mode())` (`gitdomain.FSFileInfoToProto`).
/// The Rust gitserver port encodes the same flag (`MODE_DIR`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// `os.ModeDir`: the bit gitserver sets in `FileInfo.mode` for tree entries.
const MODE_DIR: u32 = 1 << 31;

/// `gitdomain.ProtoFileInfoToFS`'s directory flag (`fs.FileInfo.IsDir` is
/// `Mode().IsDir()`, i.e. `mode & os.ModeDir != 0`).
fn dir_entry(file_info: &pb::FileInfo) -> DirEntry {
    DirEntry {
        name: String::from_utf8_lossy(&file_info.name).into_owned(),
        is_dir: file_info.mode & MODE_DIR != 0,
    }
}

/// The subset of `gitdomain.Commit` the upload processor, the code-intel
/// commit graph updater and the Perforce changelist mapper read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub oid: String,
    /// `commit.Parents`, in the order gitserver reported them.
    pub parents: Vec<String>,
    /// `commit.Author.Name` (`gitdomain.Commit.Author`).
    pub author_name: String,
    /// `commit.Author.Email`.
    pub author_email: String,
    /// `commit.Author.Date`; absent when the signature has no date.
    pub author_date: Option<prost_types::Timestamp>,
    /// `commit.Committer.Date`; absent when the signature has no date.
    pub committer_date: Option<prost_types::Timestamp>,
    /// `commit.Message`. gitserver sends it as bytes; Go converts it to a
    /// string without validating UTF-8, so invalid sequences are replaced
    /// here (`String::from_utf8_lossy`).
    pub message: String,
}

/// `gitserver.CommitsOrder`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitsOrder {
    Default,
    CommitDate,
    TopoDate,
}

/// `gitserver.CommitsOptions`; only the fields the commit graph updater sets
/// are represented, plus the validation `ToProto` performs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitsOptions {
    pub all_refs: bool,
    pub ranges: Vec<String>,
    pub after: Option<prost_types::Timestamp>,
    pub before: Option<prost_types::Timestamp>,
    pub max_commits: u32,
    pub skip: u32,
    pub order: Option<CommitsOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeStatus {
    Added,
    Modified,
    Deleted,
    TypeChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedFile {
    pub path: String,
    pub status: ChangeStatus,
}

/// `gitdomain.PreviousCommit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviousCommit {
    pub commit_id: String,
    pub filename: String,
}

/// `gitdomain.Signature` (`json:"Name,omitempty"`, `"Email,omitempty"`,
/// `"Date"`). An absent timestamp is the Unix epoch, like
/// `(*timestamppb.Timestamp)(nil).AsTime()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub date: Option<prost_types::Timestamp>,
}

/// `gitdomain.Hunk` as returned by `gitserver.Client.StreamBlameFile`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameHunk {
    pub start_line: u32,
    pub end_line: u32,
    pub start_byte: u32,
    pub end_byte: u32,
    pub commit_id: String,
    pub previous_commit: Option<PreviousCommit>,
    pub author: Signature,
    pub message: String,
    pub filename: String,
}

/// The streaming result of `gitserver.Client.StreamBlameFile`
/// (`gitserver.HunkReader`). The first error `Recv` returns is surfaced as the
/// first item, matching `grpcBlameHunkReader.Read`.
pub type BlameHunkStream = Pin<Box<dyn Stream<Item = Result<BlameHunk, Error>> + Send>>;

#[derive(Debug, Default)]
pub struct GitserverClient {
    addrs: Vec<String>,
    clients: Mutex<HashMap<String, Client>>,
}

impl GitserverClient {
    pub fn new(addrs: Vec<String>) -> Self {
        Self {
            addrs,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn addr_for(&self, repo: &str) -> Option<&str> {
        // `connection.AddrForRepo` without the site-config pins: the callers
        // of this client receive the shard list, not the unified config.
        // Pins only affect repos an operator explicitly moved; repo-updater
        // (which does have the config) uses `addresses::GitserverAddresses`.
        let name = addresses::undeleted_repo_name(repo);
        let normalized = addresses::normalize_repo(&name);
        let index = addresses::addr_index(&normalized, self.addrs.len())?;
        self.addrs.get(index).map(String::as_str)
    }

    async fn client(&self, addr: &str) -> Result<Client, Error> {
        if let Some(client) = self.clients.lock().await.get(addr) {
            return Ok(client.clone());
        }
        let client = Client::connect(format!("http://{addr}"))
            .await
            .map_err(|err| Error::Message(format!("connect to gitserver {addr}: {err}")))?;
        self.clients
            .lock()
            .await
            .insert(addr.to_owned(), client.clone());
        Ok(client)
    }

    async fn client_for(&self, repo: &str) -> Result<Client, Error> {
        let addr = self
            .addr_for(repo)
            .ok_or_else(|| Error::Message("no gitserver address configured".to_owned()))?;
        self.client(addr).await
    }

    /// Stream a tar archive for `repo@commit`. An empty `paths` means the full
    /// archive.
    pub async fn archive(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<TarByteStream, Error> {
        let mut client = self.client_for(repo).await?;

        let request = pb::ArchiveRequest {
            repo: repo.to_owned(),
            treeish: commit.to_owned(),
            format: pb::ArchiveFormat::Tar as i32,
            paths: paths.iter().map(|path| path.as_bytes().to_vec()).collect(),
        };

        let response = client
            .archive(request)
            .await
            .map_err(|status| Error::Message(format!("archive {repo}@{commit}: {status}")))?
            .into_inner();

        Ok(Box::pin(response.map(|chunk| {
            chunk
                .map(|response| Bytes::from(response.data))
                .map_err(|status| io::Error::other(status.to_string()))
        })))
    }

    /// Paths that changed between two commits (`gitserver.ChangedFiles`).
    pub async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, Error> {
        let mut client = self.client_for(repo).await?;

        let request = pb::ChangedFilesRequest {
            repo_name: repo.to_owned(),
            base: Some(base.as_bytes().to_vec()),
            head: head.as_bytes().to_vec(),
        };

        let error = |status: tonic::Status| {
            let message = format!("changed files {repo} {base}..{head}: {status}");
            if status.code() == tonic::Code::NotFound {
                Error::NotFound(message)
            } else {
                Error::Message(message)
            }
        };

        let mut response = client
            .changed_files(request)
            .await
            .map_err(error)?
            .into_inner();

        let mut files = Vec::new();
        while let Some(chunk) = response.message().await.map_err(error)? {
            for file in chunk.files {
                let status = match pb::changed_file::Status::try_from(file.status) {
                    Ok(pb::changed_file::Status::Added) => ChangeStatus::Added,
                    Ok(pb::changed_file::Status::Modified) => ChangeStatus::Modified,
                    Ok(pb::changed_file::Status::Deleted) => ChangeStatus::Deleted,
                    Ok(pb::changed_file::Status::TypeChanged) => ChangeStatus::TypeChanged,
                    // Unknown/unset statuses are ignored like the baseline's
                    // switch over gitdomain statuses.
                    _ => continue,
                };
                files.push(ChangedFile {
                    path: String::from_utf8_lossy(&file.path).into_owned(),
                    status,
                });
            }
        }
        Ok(files)
    }

    /// Read one file at `repo@commit:path`.
    pub async fn read_file(&self, repo: &str, commit: &str, path: &str) -> Result<Vec<u8>, Error> {
        let mut client = self.client_for(repo).await?;

        let request = pb::ReadFileRequest {
            repo_name: repo.to_owned(),
            commit: commit.to_owned(),
            path: path.as_bytes().to_vec(),
        };

        let error = |status: tonic::Status| {
            let message = format!("read file {repo}@{commit}:{path}: {status}");
            if status.code() == tonic::Code::NotFound {
                Error::NotFound(message)
            } else {
                Error::Message(message)
            }
        };

        let mut response = client.read_file(request).await.map_err(error)?.into_inner();

        // gitserver reports a missing file or commit as the first stream
        // message, not as the RPC status (the baseline's `NewFileReader` reads
        // the first message for exactly this reason).
        let mut data = Vec::new();
        while let Some(chunk) = response.message().await.map_err(error)? {
            data.extend_from_slice(&chunk.data);
        }
        Ok(data)
    }

    /// Read `path`, returning `Ok(None)` when gitserver reports that the file
    /// (or its commit) does not exist. The baseline's `NewFileReader` maps
    /// both cases to `os.ErrNotExist`.
    pub async fn read_file_optional(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        match self.read_file(repo, commit, path).await {
            Ok(data) => Ok(Some(data)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// `gitserver.Client.StreamBlameFile` with `BlameOptions{NewestCommit}`:
    /// the first `Recv` is awaited so a missing file (`FileNotFoundPayload`)
    /// surfaces as [`Error::FileNotExist`] before any response is written,
    /// exactly like `grpcBlameHunkReader`'s deferred first-hunk error.
    pub async fn stream_blame_file(
        &self,
        repo: &str,
        path: &str,
        newest_commit: &str,
    ) -> Result<BlameHunkStream, Error> {
        let addr = self
            .addr_for(repo)
            .ok_or_else(|| Error::Message("no gitserver address configured".to_owned()))?
            .to_owned();
        self.stream_blame_file_at(&addr, repo, path, newest_commit)
            .await
    }

    /// `StreamBlameFile` against an explicitly resolved shard. Callers that
    /// have the site configuration (pins) resolve the address through
    /// [`addresses::GitserverAddresses::addr_for_repo`]; this client only
    /// knows the plain shard list.
    pub async fn stream_blame_file_at(
        &self,
        addr: &str,
        repo: &str,
        path: &str,
        newest_commit: &str,
    ) -> Result<BlameHunkStream, Error> {
        let mut client = self.client(addr).await?;
        // Own the request fields: the returned stream is `'static`.
        let path = path.to_owned();
        let context = format!("blame file {repo}:{path}");
        let mut response = client
            .blame(pb::BlameRequest {
                repo_name: repo.to_owned(),
                commit: newest_commit.to_owned(),
                path: path.as_bytes().to_vec(),
                ignore_whitespace: false,
                range: None,
            })
            .await
            .map_err(|status| blame_status_error(status, &path, &context))?
            .into_inner();

        let first = response
            .message()
            .await
            .map_err(|status| blame_status_error(status, &path, &context))?
            .map(|response| hunk_from_proto(response.hunk, &context));

        let first = tokio_stream::iter(first);
        let rest = response.map(move |item| {
            item.map_err(|status| blame_status_error(status, &path, &context))
                .and_then(|response| hunk_from_proto(response.hunk, &context))
        });

        Ok(Box::pin(first.chain(rest)))
    }

    /// `gitserver.Client.ResolveRevision`. `ensure_revision` maps to the
    /// request's optional field (the baseline only sets it when true).
    pub async fn resolve_revision(
        &self,
        repo: &str,
        revspec: &str,
        ensure_revision: bool,
    ) -> Result<String, Error> {
        let addr = self
            .addr_for(repo)
            .ok_or_else(|| Error::Message("no gitserver address configured".to_owned()))?
            .to_owned();
        self.resolve_revision_at(&addr, repo, revspec, ensure_revision)
            .await
    }

    /// `ResolveRevision` against an explicitly resolved shard (site-config
    /// pins; see [`Self::stream_blame_file_at`]).
    pub async fn resolve_revision_at(
        &self,
        addr: &str,
        repo: &str,
        revspec: &str,
        ensure_revision: bool,
    ) -> Result<String, Error> {
        let mut client = self.client(addr).await?;
        let context = format!("resolve revision {repo}@{revspec}");
        let response = client
            .resolve_revision(pb::ResolveRevisionRequest {
                repo_name: repo.to_owned(),
                rev_spec: revspec.as_bytes().to_vec(),
                ensure_revision: ensure_revision.then_some(true),
            })
            .await
            .map_err(|status| translate_status(status, &context))?;
        Ok(response.into_inner().commit_sha)
    }

    /// `gitserver.Client.GetDefaultBranch(repo, false)`; returns the full ref
    /// name (`refs/heads/main`).
    pub async fn default_branch(&self, repo: &str) -> Result<String, Error> {
        Ok(self.default_branch_commit(repo).await?.0)
    }

    /// `gitserver.Client.GetDefaultBranch(repo, false)` returning both the
    /// full ref name and the commit it points at (the Perforce changelist
    /// mapper needs the HEAD commit).
    pub async fn default_branch_commit(&self, repo: &str) -> Result<(String, String), Error> {
        let mut client = self.client_for(repo).await?;
        let context = format!("default branch {repo}");
        let response = client
            .default_branch(pb::DefaultBranchRequest {
                repo_name: repo.to_owned(),
                short_ref: false,
            })
            .await
            .map_err(|status| translate_status(status, &context))?;
        let response = response.into_inner();
        Ok((response.ref_name, response.commit))
    }

    /// `gitserver.Client.ListRefs` (streaming response).
    pub async fn list_refs(
        &self,
        repo: &str,
        heads_only: bool,
        tags_only: bool,
        points_at_commit: &[String],
        contains_sha: Option<&str>,
    ) -> Result<Vec<GitRef>, Error> {
        let mut client = self.client_for(repo).await?;
        let context = format!("list refs {repo}");
        let mut stream = client
            .list_refs(pb::ListRefsRequest {
                repo_name: repo.to_owned(),
                heads_only,
                tags_only,
                points_at_commit: points_at_commit.to_vec(),
                contains_sha: contains_sha.map(str::to_owned),
            })
            .await
            .map_err(|status| translate_status(status, &context))?
            .into_inner();

        let mut refs = Vec::new();
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|status| translate_status(status, &context))?
        {
            for git_ref in chunk.refs {
                refs.push(GitRef {
                    name: String::from_utf8_lossy(&git_ref.ref_name).into_owned(),
                    short_name: String::from_utf8_lossy(&git_ref.short_ref_name).into_owned(),
                    target_commit: git_ref.target_commit,
                    ref_oid: git_ref.ref_oid,
                    created_at: git_ref.created_at,
                    ref_type: match pb::git_ref::RefType::try_from(git_ref.ref_type) {
                        Ok(pb::git_ref::RefType::Branch) => RefType::Branch,
                        Ok(pb::git_ref::RefType::Tag) => RefType::Tag,
                        _ => RefType::Unknown,
                    },
                    is_head: git_ref.is_head,
                });
            }
        }
        Ok(refs)
    }

    /// `gitserver.Client.ReadDir`: the non-recursive first chunk. A missing
    /// directory is reported as `FileNotFoundPayload` and maps to
    /// [`Error::NotFound`] (the handler treats it like `os.IsNotExist`).
    pub async fn read_dir(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
    ) -> Result<Vec<FileInfo>, Error> {
        Ok(self
            .read_dir_entries(repo, commit, path, false)
            .await?
            .into_iter()
            .map(|entry| FileInfo { name: entry.name })
            .collect())
    }

    /// `gitserver.Client.ReadDir` with `recursive` and the directory flag
    /// preserved (`inference.gitService.ReadDir` drains the whole iterator,
    /// including the tree entries `-t` adds).
    pub async fn read_dir_entries(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<DirEntry>, Error> {
        let mut client = self.client_for(repo).await?;
        let context = format!("read dir {repo}@{commit}:{path}");
        let mut stream = client
            .read_dir(pb::ReadDirRequest {
                repo_name: repo.to_owned(),
                commit_sha: commit.to_owned(),
                path: Some(path.as_bytes().to_vec()),
                recursive,
            })
            .await
            .map_err(|status| translate_status(status, &context))?
            .into_inner();

        let mut files = Vec::new();
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|status| translate_status(status, &context))?
        {
            for file_info in chunk.file_info {
                files.push(dir_entry(&file_info));
            }
        }
        Ok(files)
    }

    /// `gitserver.Client.FirstEverCommit`: the first commit ever made to the
    /// repository on the default branch. A response without a commit is the
    /// zero commit, matching `gitdomain.CommitFromProto(nil)`.
    pub async fn first_ever_commit(&self, repo: &str) -> Result<Commit, Error> {
        let mut client = self.client_for(repo).await?;
        let context = format!("first ever commit {repo}");
        let response = client
            .first_ever_commit(pb::FirstEverCommitRequest {
                repo_name: repo.to_owned(),
            })
            .await
            .map_err(|status| translate_status(status, &context))?;
        let commit = response.into_inner().commit.unwrap_or_default();
        Ok(commit_from_proto(commit))
    }

    /// `gitserver.Client.GetCommit`.
    pub async fn get_commit(&self, repo: &str, commit: &str) -> Result<Commit, Error> {
        let mut client = self.client_for(repo).await?;
        let context = format!("get commit {repo}@{commit}");
        let response = client
            .get_commit(pb::GetCommitRequest {
                repo_name: repo.to_owned(),
                commit: commit.to_owned(),
                include_modified_files: false,
            })
            .await
            .map_err(|status| translate_status(status, &context))?;
        let commit = response.into_inner().commit.unwrap_or_default();
        Ok(commit_from_proto(commit))
    }

    /// `gitserver.Client.Commits` (`CommitsOptions.ToProto` + a `CommitLog`
    /// stream), reduced to the commit fields the code-intel commit graph
    /// updater reads.
    pub async fn commits(
        &self,
        repo: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<Commit>, Error> {
        // `CommitsOptions.ToProto`.
        if !options.ranges.is_empty() && options.all_refs {
            return Err(Error::Message(
                "cannot specify both a range and AllRefs".to_owned(),
            ));
        }
        if options.ranges.is_empty() && !options.all_refs {
            return Err(Error::Message("must specify a range or AllRefs".to_owned()));
        }
        let order = match options.order.unwrap_or(CommitsOrder::Default) {
            CommitsOrder::Default => pb::commit_log_request::CommitLogOrder::Unspecified,
            CommitsOrder::CommitDate => pb::commit_log_request::CommitLogOrder::CommitDate,
            CommitsOrder::TopoDate => pb::commit_log_request::CommitLogOrder::TopoDate,
        };

        let mut client = self.client_for(repo).await?;
        let context = format!("commits {repo}");
        let mut stream = client
            .commit_log(pb::CommitLogRequest {
                repo_name: repo.to_owned(),
                ranges: options
                    .ranges
                    .iter()
                    .map(|r| r.as_bytes().to_vec())
                    .collect(),
                all_refs: options.all_refs,
                after: options.after,
                before: options.before,
                max_commits: options.max_commits,
                skip: options.skip,
                follow_only_first_parent: false,
                include_modified_files: false,
                order: order as i32,
                message_query: Vec::new(),
                author_query: Vec::new(),
                path: Vec::new(),
                follow_path_renames: false,
            })
            .await
            .map_err(|status| translate_status(status, &context))?
            .into_inner();

        let mut commits = Vec::new();
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|status| translate_status(status, &context))?
        {
            for response in chunk.commits {
                if let Some(commit) = response.commit {
                    commits.push(commit_from_proto(commit));
                }
            }
        }
        Ok(commits)
    }
}

/// `gitdomain.CommitFromProto`, reduced to the fields gitserver clients read.
fn commit_from_proto(commit: pb::GitCommit) -> Commit {
    let author = commit.author.unwrap_or_default();
    Commit {
        oid: commit.oid,
        parents: commit.parents,
        author_name: String::from_utf8_lossy(&author.name).into_owned(),
        author_email: String::from_utf8_lossy(&author.email).into_owned(),
        author_date: author.date,
        committer_date: commit.committer.and_then(|committer| committer.date),
        message: String::from_utf8_lossy(&commit.message).into_owned(),
    }
}

/// Translate a gRPC status into the baseline's `gitdomain` error types by
/// decoding the `grpc-status-details-bin` payload (`internal/gitserver/errwrap.go`).
fn translate_status(status: tonic::Status, context: &str) -> Error {
    if let Ok(rpc) = payloads::RpcStatus::decode(status.details()) {
        for any in &rpc.details {
            if any
                .type_url
                .ends_with("gitserver.v1.RevisionNotFoundPayload")
            {
                if let Ok(payload) = payloads::RevisionNotFoundPayload::decode(any.value.as_slice())
                {
                    return Error::RevisionNotFound {
                        repo: payload.repo,
                        spec: payload.spec,
                    };
                }
            }
            if any.type_url.ends_with("gitserver.v1.RepoNotFoundPayload") {
                if let Ok(payload) = payloads::RepoNotFoundPayload::decode(any.value.as_slice()) {
                    return Error::RepoNotFound {
                        repo: payload.repo,
                        clone_in_progress: payload.clone_in_progress,
                    };
                }
            }
            if any.type_url.ends_with("gitserver.v1.FileNotFoundPayload") {
                return Error::NotFound(status.message().to_owned());
            }
        }
    }

    let message = format!("{context}: {status}");
    if status.code() == tonic::Code::NotFound {
        Error::NotFound(message)
    } else {
        Error::Message(message)
    }
}

/// `gitserver.Client.StreamBlameFile`'s error mapping. A `FileNotFoundPayload`
/// becomes the baseline's `*os.PathError{Op:"open", Err:os.ErrNotExist}`
/// (`grpcBlameHunkReader` callers check `os.IsNotExist`); every other status
/// goes through the shared payload translation.
fn blame_status_error(status: tonic::Status, path: &str, context: &str) -> Error {
    if let Ok(rpc) = payloads::RpcStatus::decode(status.details()) {
        for any in &rpc.details {
            if any.type_url.ends_with("gitserver.v1.FileNotFoundPayload") {
                return Error::FileNotExist {
                    path: path.to_owned(),
                };
            }
        }
    }
    translate_status(status, context)
}

/// `gitdomain.HunkFromBlameProto`. The baseline returns a nil `*Hunk` for a
/// response without a hunk and the handler then panics; a drop-in server
/// answers with a stream error instead.
fn hunk_from_proto(hunk: Option<pb::BlameHunk>, context: &str) -> Result<BlameHunk, Error> {
    let Some(hunk) = hunk else {
        return Err(Error::Message(format!(
            "{context}: blame response without a hunk"
        )));
    };
    let author = hunk.author.map_or_else(
        || Signature {
            name: String::new(),
            email: String::new(),
            date: None,
        },
        |author| Signature {
            // The baseline converts the bytes to a Go string, which keeps
            // invalid UTF-8; `from_utf8_lossy` replaces it, like the JSON
            // encoder does.
            name: String::from_utf8_lossy(&author.name).into_owned(),
            email: author.email,
            date: author.date,
        },
    );
    Ok(BlameHunk {
        start_line: hunk.start_line,
        end_line: hunk.end_line,
        start_byte: hunk.start_byte,
        end_byte: hunk.end_byte,
        commit_id: hunk.commit,
        previous_commit: hunk.previous_commit.map(|previous| PreviousCommit {
            commit_id: previous.commit,
            filename: previous.filename,
        }),
        author,
        message: hunk.message,
        filename: hunk.filename,
    })
}

/// Payloads carried in `grpc-status-details-bin` (`google.rpc.Status`). The
/// gitserver crate has the server-side encoders; these are the client-side
/// decoders.
mod payloads {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct RpcStatus {
        #[prost(int32, tag = "1")]
        pub code: i32,
        #[prost(string, tag = "2")]
        pub message: String,
        #[prost(message, repeated, tag = "3")]
        pub details: Vec<prost_types::Any>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RepoNotFoundPayload {
        #[prost(string, tag = "1")]
        pub repo: String,
        #[prost(bool, tag = "2")]
        pub clone_in_progress: bool,
        #[prost(string, tag = "3")]
        pub clone_progress: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RevisionNotFoundPayload {
        #[prost(string, tag = "1")]
        pub repo: String,
        #[prost(string, tag = "2")]
        pub spec: String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dir_entry_reads_the_mode_dir_bit() {
        let directory = pb::FileInfo {
            name: b"vendor".to_vec(),
            mode: 0o40000 | MODE_DIR,
            ..Default::default()
        };
        assert!(dir_entry(&directory).is_dir);
        assert_eq!(dir_entry(&directory).name, "vendor");

        let file = pb::FileInfo {
            name: b"main.go".to_vec(),
            mode: 0o100644,
            ..Default::default()
        };
        assert!(!dir_entry(&file).is_dir);
    }
}
