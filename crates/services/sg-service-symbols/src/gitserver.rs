#![forbid(unsafe_code)]

//! Minimal gitserver client used by the pilot: `Archive` and `ReadFile`.
//!
//! The baseline symbols service obtains gitserver addresses from site config;
//! the Rust pilot reads `SRC_GIT_SERVERS` directly and picks the first address
//! (all gitservers in a cluster hold the same repositories).

use std::collections::HashMap;
use std::io;
use std::pin::Pin;

use bytes::Bytes;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};

use sg_proto::gitserver as pb;

type Client = pb::gitserver_service_client::GitserverServiceClient<tonic::transport::Channel>;

pub type TarByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

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
