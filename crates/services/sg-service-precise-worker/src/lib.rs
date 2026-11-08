#![forbid(unsafe_code)]

//! `precise-code-intel-worker` service.
//!
//! This slice (9.7a) is the startup skeleton: the baseline environment
//! registry (worker + LSIF upload store settings), frontend configuration
//! discovery, keyring initialization, the frontend and code-intel database
//! connections, the upload-store configuration, and the `/healthz` server on
//! `:3188`.
//!
//! 9.7b adds the upload queue (`dbworker` store semantics) and the stalled
//! upload resetter; 9.7c adds the `workerutil.Worker` loop. Not yet covered
//! (later slices): the upload processor (object storage fetch, SCIP parsing
//! and code-graph writes), which is why the loop is not started by the
//! service yet.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use sg_config::ConfigClient;
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub mod config;
pub mod pathexistence;
pub mod prelude;
pub mod queue;
pub mod scip_stream;
pub mod worker;

use config::PreciseWorkerConfig;

pub const SERVICE_NAME: &str = "precise-code-intel-worker";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}

/// How long to wait between failed frontend-configuration fetches
/// (`conf.Get` retries in the baseline).
const CONFIG_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// `processor.NewUploadResetter`'s `Interval`.
const RESETTER_INTERVAL: Duration = Duration::from_secs(30);

pub struct PreciseWorkerService {
    /// Rust-only rollout knob (not part of the baseline registry) so the pilot
    /// can listen on a side port while the Go binary keeps `:3188`.
    addr_override: Option<String>,
    config: Option<PreciseWorkerConfig>,
}

impl PreciseWorkerService {
    pub fn new() -> Self {
        Self {
            addr_override: None,
            config: None,
        }
    }

    pub fn with_addr(addr: impl Into<String>) -> Self {
        Self {
            addr_override: Some(addr.into()),
            config: None,
        }
    }
}

impl Default for PreciseWorkerService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for PreciseWorkerService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        // `sanitycheck.Pass()` registers SANITY_CHECK in every service binary.
        let _ = env.get(
            "SANITY_CHECK",
            "false",
            "check that go-sqlite3 works then exit 0 if it's ok or 1 if not",
        );
        self.config = Some(PreciseWorkerConfig::load(env, self.addr_override.clone())?);
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.config
            .as_ref()
            .ok_or_else(|| {
                ConfigError::new("precise-code-intel-worker configuration was not loaded")
            })?
            .validate()
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self.config.clone().ok_or_else(|| {
            ServiceError::new("precise-code-intel-worker configuration was not loaded")
        })?;

        // `shared.Main` → `keyring.Init` calls `conf.Get()`, which blocks until
        // the frontend sends the initial configuration.
        let config_client = Arc::new(ConfigClient::from_env());
        let unified = wait_for_initial_config(&config_client, &ctx.shutdown)
            .await
            .ok_or_else(|| {
                ServiceError::new(
                    "precise-code-intel-worker stopped while waiting for the frontend configuration",
                )
            })?;

        // `keyring.Init(ctx)` reads `EncryptionKeys` from site config.
        let _keyring = sg_keyring::init(&unified.site).await.map_err(|err| {
            ServiceError::with_source("initializing keyring", std::io::Error::other(err))
        })?;

        // `mustInitializeDB` / `mustInitializeCodeIntelDB`. The baseline exits
        // with a fatal log on failure; returning the error here is the
        // equivalent (svcmain exits 1).
        let dsn = unified.service_connections.postgres_dsn.clone();
        if dsn.is_empty() {
            return Err(ServiceError::new(
                "postgres_dsn is not configured by the frontend",
            ));
        }
        let frontend_pool = sg_db::connect(&dsn, 10)
            .await
            .map_err(|err| ServiceError::with_source("connecting to the frontend database", err))?;

        let codeintel_dsn = unified.service_connections.code_intel_postgres_dsn.clone();
        if codeintel_dsn.is_empty() {
            return Err(ServiceError::new(
                "code_intel_postgres_dsn is not configured by the frontend",
            ));
        }
        let _codeintel_pool = sg_db::connect(&codeintel_dsn, 10).await.map_err(|err| {
            ServiceError::with_source("connecting to the codeintel database", err)
        })?;

        // `ready()`: migrations are done and the process accepts traffic.
        ctx.ready.mark_ready();

        // `lsifuploadstore.New` + `initializeUploadStore` (S3/blobstore).
        let _upload_store = initialize_upload_store(&config.upload_store)
            .await
            .map_err(|message| {
                ServiceError::new(format!("initializing upload store: {message}"))
            })?;

        // `httpserver.NewFromAddr(addr, ...)` with a `/healthz` handler.
        tracing::info!(
            addr = config.listen_addr,
            poll_interval = ?config.worker_poll_interval,
            concurrency = config.worker_concurrency,
            budget = config.worker_budget,
            "serving precise-code-intel-worker health endpoint"
        );
        // `processor.NewUploadResetter` + `workerutil.Worker`: the resetter
        // loop is part of this slice; the worker loop is implemented in
        // `worker.rs` but is not started until the upload processor exists
        // (9.7d), so the Rust process cannot consume uploads before it can
        // process them.
        let queue_store = Arc::new(queue::UploadQueueStore::new(frontend_pool));
        let tasks = TaskTracker::new();
        {
            let store = Arc::clone(&queue_store);
            let shutdown = ctx.shutdown.clone();
            tasks.spawn(async move { resetter_loop(store, shutdown).await });
        }

        let served = serve_health(&config.listen_addr, ctx.shutdown.clone()).await;
        ctx.shutdown.cancel();
        tasks.close();
        tasks.wait().await;
        served.map_err(|err| ServiceError::with_source("serving HTTP", err))
    }
}

/// `dbworker.Resetter.Start`: reset stalled records immediately, then every
/// `Interval` (30s) until shutdown.
async fn resetter_loop(store: Arc<queue::UploadQueueStore>, shutdown: CancellationToken) {
    loop {
        match store.reset_stalled().await {
            Ok(result) => {
                for (id, age) in &result.reset {
                    tracing::warn!(
                        name = "precise_code_intel_upload_worker_resetter",
                        id,
                        time_since_last_heartbeat = ?age,
                        "Reset stalled record back to 'queued' state"
                    );
                }
                for (id, age) in &result.failed {
                    tracing::warn!(
                        name = "precise_code_intel_upload_worker_resetter",
                        id,
                        time_since_last_heartbeat = ?age,
                        "Reset stalled record to 'failed' state"
                    );
                }
            }
            Err(err) => {
                tracing::error!(
                    name = "precise_code_intel_upload_worker_resetter",
                    error = %err,
                    "Failed to reset stalled records"
                );
            }
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = sleep(RESETTER_INTERVAL) => {}
        }
    }
}

/// `lsifuploadstore.New` for the S3-compatible backends. GCS is not
/// implemented by the Rust worker yet; the baseline would accept it, so this
/// is a documented difference (see docs/modules/07 §16).
pub fn upload_storage_config(
    config: &config::UploadStoreConfig,
) -> Result<sg_objectstorage::StorageConfig, String> {
    match config.backend.as_str() {
        "s3" | "blobstore" => Ok(sg_objectstorage::StorageConfig {
            backend: config.backend.clone(),
            manage_bucket: config.manage_bucket,
            bucket: config.bucket.clone(),
            s3: sg_objectstorage::S3Config {
                is_blobstore: false,
                region: config.s3_region.clone(),
                endpoint: config.s3_endpoint.clone(),
                use_path_style: config.s3_use_path_style,
                access_key_id: config.s3_access_key_id.clone(),
                secret_access_key: config.s3_secret_access_key.clone(),
                session_token: config.s3_session_token.clone(),
            },
        }
        .normalize()),
        "gcs" => Err("GCS upload storage is not implemented by the Rust worker".to_owned()),
        other => Err(format!("unsupported upload backend {other:?}")),
    }
}

/// `initializeUploadStore`: `object.Storage.Init` (S3 only for now).
pub async fn initialize_upload_store(
    config: &config::UploadStoreConfig,
) -> Result<sg_objectstorage::ObjectStore, String> {
    let storage_config = upload_storage_config(config)?;
    let store = sg_objectstorage::ObjectStore::new(storage_config)
        .await
        .map_err(|err| err.to_string())?;
    store.init().await.map_err(|err| err.to_string())?;
    Ok(store)
}

/// The baseline health server: `httpserver.NewHandler(nil)` installs exactly
/// one route, `/healthz`, which answers 200 with an empty body. Anything else
/// is gorilla/mux's `404 page not found`.
pub fn health_handler() -> sg_http::HttpHandler {
    Arc::new(|req: Request<Incoming>| Box::pin(async move { health_response(req) }))
}

fn health_response<B>(req: Request<B>) -> Response<tonic::body::BoxBody> {
    match req.uri().path() {
        "/healthz" => {
            // `w.WriteHeader(http.StatusOK)` writes no body and no
            // Content-Type.
            let mut response =
                Response::new(tonic::body::boxed(Full::new(hyper::body::Bytes::new())));
            *response.status_mut() = StatusCode::OK;
            response
        }
        _ => go_not_found(),
    }
}

/// Go's `http.NotFound`.
fn go_not_found() -> Response<tonic::body::BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(
        hyper::body::Bytes::from_static(b"404 page not found\n"),
    )));
    *response.status_mut() = StatusCode::NOT_FOUND;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    response
}

async fn serve_health(addr: &str, shutdown: CancellationToken) -> std::io::Result<()> {
    let listener = sg_http::listen(addr).await?;
    tracing::info!(addr, "health server listening");
    // `http.Server{ReadTimeout: 75s, WriteTimeout: 10m}` — hyper applies
    // neither; the health endpoint is a single small write.
    sg_http::serve_http(listener, health_handler(), shutdown).await
}

async fn wait_for_initial_config(
    client: &ConfigClient,
    shutdown: &CancellationToken,
) -> Option<Arc<sg_config::Unified>> {
    loop {
        if shutdown.is_cancelled() {
            return None;
        }
        match client.refresh().await {
            Ok(unified) => return Some(unified),
            Err(err) => {
                tracing::warn!(
                    addr = client.addr(),
                    error = %err,
                    "waiting for the initial frontend configuration"
                );
            }
        }

        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = sleep(CONFIG_RETRY_INTERVAL) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(response: Response<tonic::body::BoxBody>) -> Vec<u8> {
        use http_body_util::BodyExt;
        response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec()
    }

    #[tokio::test]
    async fn healthz_is_200_with_empty_body_and_no_content_type() {
        let req = Request::builder()
            .uri("/healthz")
            .body(())
            .expect("request");
        let response = health_response(req);
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
        assert!(body_of(response).await.is_empty());
    }

    #[tokio::test]
    async fn unknown_paths_are_go_not_found() {
        let req = Request::builder().uri("/nope").body(()).expect("request");
        let response = health_response(req);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .expect("content type"),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .expect("nosniff"),
            "nosniff"
        );
        assert_eq!(body_of(response).await, b"404 page not found\n");
    }
}
