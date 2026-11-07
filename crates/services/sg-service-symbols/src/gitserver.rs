#![forbid(unsafe_code)]

//! Minimal gitserver client used by the pilot: `Archive` and `ReadFile`.
//!
//! The baseline symbols service obtains gitserver addresses from site config;
//! the Rust pilot reads `SRC_GIT_SERVERS` directly and picks the first address
//! (all gitservers in a cluster hold the same repositories).

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};

use sg_proto::gitserver as pb;

type Client = pb::gitserver_service_client::GitserverServiceClient<tonic::transport::Channel>;

pub type TarByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

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

/// Operations the symbols service needs from gitserver. Production code uses
/// [`GitserverClient`]; tests supply an in-memory archive.
#[async_trait]
pub trait GitserverReader: Send + Sync {
    /// Stream a tar archive for `repo@commit`. An empty `paths` means the full
    /// archive.
    async fn fetch_tar(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<TarByteStream, String>;

    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, String>;

    async fn read_file(&self, repo: &str, commit: &str, path: &str) -> Result<Vec<u8>, String>;
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

    async fn client(&self, addr: &str) -> Result<Client, String> {
        if let Some(client) = self.clients.lock().await.get(addr) {
            return Ok(client.clone());
        }
        let client = Client::connect(format!("http://{addr}"))
            .await
            .map_err(|err| format!("connect to gitserver {addr}: {err}"))?;
        self.clients
            .lock()
            .await
            .insert(addr.to_owned(), client.clone());
        Ok(client)
    }

    /// Stream a tar archive for `repo@commit`. An empty `paths` means the full
    /// archive.
    pub async fn fetch_tar(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<TarByteStream, String> {
        let started = Instant::now();
        let result = self.fetch_tar_inner(repo, commit, paths).await;
        crate::metrics::Metrics::global().gitserver_observe("FetchTar", started, result.is_err());
        result
    }

    async fn fetch_tar_inner(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<TarByteStream, String> {
        let addr = self
            .addr_for(repo)
            .ok_or_else(|| "no gitserver address configured".to_owned())?;
        let mut client = self.client(addr).await?;

        let request = pb::ArchiveRequest {
            repo: repo.to_owned(),
            treeish: commit.to_owned(),
            format: pb::ArchiveFormat::Tar as i32,
            paths: paths.iter().map(|path| path.as_bytes().to_vec()).collect(),
        };

        let response = client
            .archive(request)
            .await
            .map_err(|status| format!("archive {repo}@{commit}: {status}"))?
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
    ) -> Result<Vec<ChangedFile>, String> {
        let started = Instant::now();
        let result = self.changed_files_inner(repo, base, head).await;
        crate::metrics::Metrics::global().gitserver_observe(
            "ChangedFiles",
            started,
            result.is_err(),
        );
        result
    }

    async fn changed_files_inner(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, String> {
        let addr = self
            .addr_for(repo)
            .ok_or_else(|| "no gitserver address configured".to_owned())?;
        let mut client = self.client(addr).await?;

        let request = pb::ChangedFilesRequest {
            repo_name: repo.to_owned(),
            base: Some(base.as_bytes().to_vec()),
            head: head.as_bytes().to_vec(),
        };

        let mut response = client
            .changed_files(request)
            .await
            .map_err(|status| format!("changed files {repo} {base}..{head}: {status}"))?
            .into_inner();

        let mut files = Vec::new();
        while let Some(chunk) = response
            .message()
            .await
            .map_err(|status| format!("changed files {repo} {base}..{head}: {status}"))?
        {
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

    /// Read one file at `repo@commit:path` (used by `LocalCodeIntel`).
    pub async fn read_file(&self, repo: &str, commit: &str, path: &str) -> Result<Vec<u8>, String> {
        let addr = self
            .addr_for(repo)
            .ok_or_else(|| "no gitserver address configured".to_owned())?;
        let mut client = self.client(addr).await?;

        let request = pb::ReadFileRequest {
            repo_name: repo.to_owned(),
            commit: commit.to_owned(),
            path: path.as_bytes().to_vec(),
        };

        let mut response = client
            .read_file(request)
            .await
            .map_err(|status| format!("read file {repo}@{commit}:{path}: {status}"))?
            .into_inner();

        let mut data = Vec::new();
        while let Some(chunk) = response
            .message()
            .await
            .map_err(|status| format!("read file {repo}@{commit}:{path}: {status}"))?
        {
            data.extend_from_slice(&chunk.data);
        }
        Ok(data)
    }
}

#[async_trait]
impl GitserverReader for GitserverClient {
    async fn fetch_tar(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<TarByteStream, String> {
        GitserverClient::fetch_tar(self, repo, commit, paths).await
    }

    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, String> {
        GitserverClient::changed_files(self, repo, base, head).await
    }

    async fn read_file(&self, repo: &str, commit: &str, path: &str) -> Result<Vec<u8>, String> {
        GitserverClient::read_file(self, repo, commit, path).await
    }
}
