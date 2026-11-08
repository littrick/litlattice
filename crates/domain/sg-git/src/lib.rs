#![forbid(unsafe_code)]

//! `sg-git`: gitserver gRPC client shared by the Rust services.
//!
//! The baseline obtains gitserver addresses from site configuration. The pilot
//! reads `SRC_GIT_SERVERS` directly and picks the first address (all
//! gitservers in a cluster hold the same repositories).

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::pin::Pin;

use bytes::Bytes;
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
    /// Any other failure.
    Message(String),
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message) | Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for String {
    fn from(err: Error) -> Self {
        err.to_string()
    }
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

    fn addr_for(&self, _repo: &str) -> Option<&str> {
        // TODO(M8): rendezvous-hash `repo` across addresses like
        // internal/gitserver/connection. A single-address pilot is enough for
        // the first replacement step.
        self.addrs.first().map(String::as_str)
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
}
