#![forbid(unsafe_code)]

//! Rust `embeddings` process, drop-in for `cmd/embeddings`.
//!
//! Baseline `cmd/embeddings/main.go` calls `sanitycheck.Pass()` then
//! `svcmain.SingleServiceMain(shared.Service)`; `shared.Main` connects the
//! frontend DB, builds the object-storage index downloader, and serves the
//! legacy `POST /search` API on `:9991`.
//!
//! The DB-backed index download and the site-configured embeddings provider
//! are not ported (M9.17c): the binary starts the same listener and routes,
//! but the injected backend reports the missing pieces per request. The
//! configuration/validation/handler surface is shared with the differential
//! harness in `src/lib.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use sg_bin_embeddings::{Config, EmbeddingBackend, EmbeddingError, Handler, RepoEmbeddingIndex};
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};

/// `shared.addr`.
const ADDR: &str = ":9991";

/// The production backend until the DB/object-storage/provider wiring lands.
struct NotPortedBackend;

impl EmbeddingBackend for NotPortedBackend {
    fn get_query_embedding(&self, _query: &str) -> Result<(Vec<f32>, String), EmbeddingError> {
        Err(EmbeddingError::other(
            "not ported: embeddings query client",
        ))
    }

    fn get_repo_embedding_index(
        &self,
        _repo_id: i32,
        _repo_name: &str,
    ) -> Result<RepoEmbeddingIndex, EmbeddingError> {
        Err(EmbeddingError::other(
            "not ported: embeddings repo index download",
        ))
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
        // Go's `net.Listen("tcp", ":9991")` binds the IPv4 wildcard.
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", &ADDR[1..]))
            .await
            .map_err(|err| ServiceError::new(format!("failed to listen on {ADDR}: {err}")))?;
        ctx.ready.mark_ready();
        tracing::info!(service = "embeddings", addr = ADDR, "embeddings server listening");

        let handler = Arc::new(Handler::new(Arc::new(NotPortedBackend)));
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

    let response = handler.handle(&path, &method, &body);
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
