#![forbid(unsafe_code)]

//! Metrics-instrumented wrapper around the shared [`sg_git::GitserverClient`].

use async_trait::async_trait;
use std::time::Instant;

pub use sg_git::{ChangeStatus, ChangedFile, TarByteStream};

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
    inner: sg_git::GitserverClient,
}

impl GitserverClient {
    pub fn new(addrs: Vec<String>) -> Self {
        Self {
            inner: sg_git::GitserverClient::new(addrs),
        }
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
        let result = self
            .inner
            .archive(repo, commit, paths)
            .await
            .map_err(String::from);
        crate::metrics::Metrics::global().gitserver_observe("FetchTar", started, result.is_err());
        result
    }

    /// Paths that changed between two commits (`gitserver.ChangedFiles`).
    pub async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, String> {
        let started = Instant::now();
        let result = self
            .inner
            .changed_files(repo, base, head)
            .await
            .map_err(String::from);
        crate::metrics::Metrics::global().gitserver_observe(
            "ChangedFiles",
            started,
            result.is_err(),
        );
        result
    }

    /// Read one file at `repo@commit:path` (used by `LocalCodeIntel`).
    pub async fn read_file(&self, repo: &str, commit: &str, path: &str) -> Result<Vec<u8>, String> {
        self.inner
            .read_file(repo, commit, path)
            .await
            .map_err(String::from)
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
