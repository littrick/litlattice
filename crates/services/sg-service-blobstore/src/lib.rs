#![forbid(unsafe_code)]

//! Rust `blobstore` process, drop-in for Go `cmd/blobstore`.
//!
//! The service exposes the S3-compatible subset Sourcegraph's
//! `internal/object/s3_client.go` uses, backed by flat files under
//! `<data-dir>/buckets/<bucket>/<url-escaped-key>` (the same layout as the Go
//! implementation, so an existing data directory keeps working).

pub mod backend;
pub mod fs_backend;
pub mod multipart;
pub mod paths;
pub mod routes;
pub mod s3_xml;

use std::path::PathBuf;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};

pub use fs_backend::FsBlobStore;

/// Go `deploy.BlobstoreHostPort` in single-container / insecure-dev mode.
pub const DEFAULT_ADDR: &str = "127.0.0.1:9000";
/// Same function for cluster deployments.
pub const CLUSTER_ADDR: &str = "0.0.0.0:9000";

pub const DATA_DIR_ENV: &str = "BLOBSTORE_DATA_DIR";
pub const DATA_DIR_DEFAULT: &str = "/data";

pub fn service_name() -> &'static str {
    "blobstore"
}

/// Default listen address (`deploy.BlobstoreHostPort`).
pub fn default_addr() -> String {
    let single_container =
        std::env::var("DEPLOY_TYPE").is_ok_and(|value| value == "docker-container");
    let insecure_dev = std::env::var("INSECURE_DEV").is_ok_and(|value| !value.is_empty());
    if single_container || insecure_dev {
        DEFAULT_ADDR.to_owned()
    } else {
        CLUSTER_ADDR.to_owned()
    }
}

/// `cmd/blobstore/shared/service.go` configuration.
#[derive(Debug, Clone)]
pub struct BlobstoreConfig {
    pub data_dir: PathBuf,
    pub addr: String,
}

impl BlobstoreConfig {
    pub fn load(env: &mut EnvReader) -> Self {
        let data_dir = env.get(
            DATA_DIR_ENV,
            DATA_DIR_DEFAULT,
            "directory to store blobstore buckets and objects",
        );
        Self {
            data_dir: PathBuf::from(data_dir),
            addr: default_addr(),
        }
    }
}

/// The `svc` registered with `sg_core::run`.
#[derive(Debug)]
pub struct BlobstoreService {
    addr: Option<String>,
    data_dir: Option<PathBuf>,
    config: Option<BlobstoreConfig>,
}

impl BlobstoreService {
    pub fn new() -> Self {
        Self {
            addr: None,
            data_dir: None,
            config: None,
        }
    }

    /// Test hook: bind a specific address (use `127.0.0.1:0` for ephemeral).
    pub fn with_addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    /// Test hook: override `BLOBSTORE_DATA_DIR`.
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }
}

impl Default for BlobstoreService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for BlobstoreService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        let mut config = BlobstoreConfig::load(env);
        if let Some(addr) = self.addr.clone() {
            config.addr = addr;
        }
        if let Some(data_dir) = self.data_dir.clone() {
            config.data_dir = data_dir;
        }
        self.config = Some(config);
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| ConfigError::new("blobstore configuration was not loaded"))?;
        if config.addr.is_empty() {
            return Err(ConfigError::new(
                "blobstore listen address must not be empty",
            ));
        }
        if config.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::new(
                "blobstore data directory must not be empty",
            ));
        }
        Ok(())
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("blobstore configuration was not loaded"))?;

        let backend = FsBlobStore::new(config.data_dir.clone())
            .map_err(|error| ServiceError::new(format!("initializing blobstore: {error}")))?;
        let handler = routes::handler(std::sync::Arc::new(backend));

        let listener = tokio::net::TcpListener::bind(&config.addr)
            .await
            .map_err(|error| ServiceError::new(format!("bind {}: {error}", config.addr)))?;
        tracing::info!(addr = %config.addr, "listening");
        ctx.ready.mark_ready();
        sg_http::serve_http(listener, handler, ctx.shutdown)
            .await
            .map_err(|error| ServiceError::new(format!("serving blobstore: {error}")))
    }
}
