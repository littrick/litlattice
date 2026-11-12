#![forbid(unsafe_code)]

//! Rust `embeddings` process, drop-in for `cmd/embeddings`.
//!
//! Baseline `cmd/embeddings/main.go` calls `sanitycheck.Pass()` then
//! `svcmain.SingleServiceMain(shared.Service)`; `shared.Main` connects the
//! frontend DB, builds the object-storage index downloader, and serves the
//! legacy `POST /search` API on `:9991`.
//!
//! The production backend follows `shared.Main`: the frontend PG
//! `repo_embedding_jobs` store feeds the cache, the `EMBEDDINGS_UPLOAD_*`
//! object storage downloads indexes, and the site configuration builds the
//! query client. Each system is injectable through `src/backend.rs` for the
//! differential harness.

use std::sync::Arc;

use async_trait::async_trait;
use sg_bin_embeddings::backend::{
    live_query_embedding, object_storage_config, postgres_dsn, CachedEmbeddingIndexGetter,
    EmbeddingIndexDownloader, EmbeddingJobsStore, ObjectStorageDownloader,
    ObjectStorageIndexStore, PgEmbeddingJobsStore, ProductionBackend,
};
use sg_bin_embeddings::{Config, EmbeddingError, Handler};
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use sg_db::OffsetDateTime;

/// `shared.addr`.
const ADDR: &str = ":9991";

/// Used when the process has no frontend DB connection configured; the
/// search request reports the standard job-store error instead of panicking.
struct UnavailableJobsStore {
    reason: String,
}

#[async_trait]
impl EmbeddingJobsStore for UnavailableJobsStore {
    async fn get_last_completed(&self, _repo_id: i32) -> Result<OffsetDateTime, EmbeddingError> {
        Err(EmbeddingError::other(format!(
            "repo embedding job store unavailable: {}",
            self.reason
        )))
    }
}

pub struct EmbeddingsService {
    config: Option<Config>,
    parse_error: Option<String>,
}

impl EmbeddingsService {
    pub fn new() -> Self {
        Self {
            config: None,
            parse_error: None,
        }
    }
}

impl Default for EmbeddingsService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for EmbeddingsService {
    fn name(&self) -> &'static str {
        "embeddings"
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        let mut get = |name: &str, default: &str| env.get(name, default, "");
        match Config::load_with(&mut get) {
            Ok(config) => {
                self.config = Some(config);
                Ok(())
            }
            Err(err) => {
                self.parse_error = Some(err);
                Ok(())
            }
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(err) = &self.parse_error {
            return Err(ConfigError::new(err.clone()));
        }
        let Some(config) = &self.config else {
            return Ok(());
        };
        match config.validate_error() {
            None => Ok(()),
            Some(message) => Err(ConfigError::new(message)),
        }
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self.config.clone().unwrap_or_default();
        let getenv = |key: &str| std::env::var(key).ok();
        let jobs: Arc<dyn EmbeddingJobsStore> = match postgres_dsn(&getenv) {
            Ok(dsn) => {
                let pool = sg_db::connect(&dsn, 10).await.map_err(|err| {
                    ServiceError::new(format!("failed to connect to database: {err}"))
                })?;
                Arc::new(PgEmbeddingJobsStore::new(pool))
            }
            Err(reason) => {
                tracing::warn!(%reason, "frontend DB is not configured");
                Arc::new(UnavailableJobsStore { reason })
            }
        };
        let store: Arc<dyn sg_bin_embeddings::backend::IndexObjectStore> =
            Arc::new(ObjectStorageIndexStore::new(object_storage_config(&config)));
        let downloader: Arc<dyn EmbeddingIndexDownloader> =
            Arc::new(ObjectStorageDownloader::new(store));
        let index_getter = Arc::new(CachedEmbeddingIndexGetter::new(
            jobs,
            downloader,
            config.embeddings_cache_size,
        ));
        let backend = Arc::new(ProductionBackend::new(
            live_query_embedding(sg_bin_embeddings::backend::site_configuration()),
            index_getter,
        ));

        // Go's `net.Listen("tcp", ":9991")` binds the IPv4 wildcard.
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", &ADDR[1..]))
            .await
            .map_err(|err| ServiceError::new(format!("failed to listen on {ADDR}: {err}")))?;
        ctx.ready.mark_ready();
        tracing::info!(service = "embeddings", addr = ADDR, "embeddings server listening");

        let handler = Arc::new(Handler::new(backend));
        loop {
            tokio::select! {
                _ = ctx.shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((mut stream, _peer)) = accepted else { continue };
                    let handler = Arc::clone(&handler);
                    tokio::spawn(async move {
                        if let Err(err) = serve_connection(&mut stream, &handler).await {
                            tracing::debug!(error = %err, "embeddings connection failed");
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

/// A minimal HTTP/1.1 request/response loop (one request per connection), with
/// the same status/body as the Go `net/http` handler output.
async fn serve_connection(
    stream: &mut tokio::net::TcpStream,
    handler: &Handler,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_header_end(&buffer) {
            break index;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or("/").to_owned();

    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    let body_start = header_end + 4;
    while buffer.len().saturating_sub(body_start) < content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = buffer[body_start..].to_vec();

    let response = handler.handle(&path, &method, &body).await;
    let status_text = match response.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        429 => "Too Many Requests",
        _ => "Internal Server Error",
    };
    // `http.Error` sets `X-Content-Type-Options: nosniff` in addition to the
    // text/plain content type.
    let nosniff = if response.content_type == "text/plain; charset=utf-8" {
        "X-Content-Type-Options: nosniff\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {} {status_text}\r\nContent-Type: {}\r\nContent-Length: {}\r\n{nosniff}Connection: close\r\n\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    stream.flush().await?;
    Ok(())
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

#[tokio::main]
async fn main() {
    // Baseline `cmd/embeddings/main.go`: `sanitycheck.Pass()`.
    sg_core::sanity_check();
    sg_log::init("embeddings");
    let services: Vec<Box<dyn Service>> = vec![Box::new(EmbeddingsService::new())];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
