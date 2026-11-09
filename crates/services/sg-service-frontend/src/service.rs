#![forbid(unsafe_code)]

//! `cmd/frontend/internal/cli/serve_cmd.go` process skeleton: the external and
//! internal listeners plus the debug server.
//!
//! The full baseline startup sequence (database, Redis, migrations, out-of-band
//! migration validation, configuration watcher, GraphQL schema, enterprise
//! hooks) is not ported yet. This slice binds the listeners, answers the
//! health/version surface, marks ready and shuts down all servers together.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use tokio::task::JoinSet;

use sg_config::SiteConfiguration;

use crate::config::FrontendConfig;
use crate::ipallowlist::{self, AllowlistEnv};
use crate::mux::SecurityConfig;
use crate::version::Version;
use crate::{debug, middleware};

/// `cmd/frontend` service name.
pub const SERVICE_NAME: &str = "frontend";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}

/// The frontend service. Only the transport skeleton is implemented; the
/// GraphQL API, UI and enterprise setup land in later waves.
pub struct FrontendService {
    config: Option<FrontendConfig>,
    version: Version,
    /// The `conf.Get()` snapshot. The configuration service is not ported
    /// yet, so this stays empty (IP allowlist disabled) until then.
    site_config: SiteConfiguration,
    allowlist_env: AllowlistEnv,
}

impl FrontendService {
    pub fn new() -> Self {
        Self {
            config: None,
            version: Version::load(),
            site_config: SiteConfiguration::default(),
            allowlist_env: AllowlistEnv::default(),
        }
    }

    /// Test hook: pin the version string instead of reading `/version.txt`.
    pub fn with_version(version: Version) -> Self {
        Self {
            version,
            ..Self::new()
        }
    }

    /// Test hook: the site configuration snapshot the middleware chain reads.
    pub fn with_site_config(mut self, site_config: SiteConfiguration) -> Self {
        self.site_config = site_config;
        self
    }

    /// Test hook: pin the `AUTH_ALLOWED_IP_ADDRESS_*` overrides.
    pub fn with_ip_allowlist_env(mut self, env: AllowlistEnv) -> Self {
        self.allowlist_env = env;
        self
    }

    pub fn config(&self) -> Option<&FrontendConfig> {
        self.config.as_ref()
    }
}

impl Default for FrontendService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for FrontendService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        self.config = Some(FrontendConfig::load(env)?);
        self.allowlist_env = AllowlistEnv::from_env();
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.config.is_some() {
            Ok(())
        } else {
            Err(ConfigError::new("frontend configuration was not loaded"))
        }
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("frontend configuration was not loaded"))?;

        let external_addr = config.external_bind_address().to_owned();
        let external = sg_http::listen(&external_addr).await.map_err(|err| {
            ServiceError::with_source(format!("failed to listen on {external_addr}"), err)
        })?;

        let internal = match config.internal_bind_address() {
            Some(addr) => {
                let addr = addr.to_owned();
                let listener = sg_http::listen(&addr).await.map_err(|err| {
                    ServiceError::with_source(format!("failed to listen on {addr}"), err)
                })?;
                Some((addr, listener))
            }
            None => None,
        };

        let ready = Arc::new(AtomicBool::new(false));
        let mut background = JoinSet::new();

        if let Some((addr, listener)) = internal {
            let shutdown = ctx.shutdown.clone();
            background.spawn(async move {
                // `internalgrpc.MultiplexHandlers`; no gRPC services are
                // registered yet, so gRPC traffic has no route, like a mux
                // without handlers.
                let routes = tonic::service::Routes::default();
                let handler = middleware::internal_handler();
                if let Err(err) = sg_http::serve(listener, routes, handler, shutdown).await {
                    tracing::error!(addr, error = %err, "frontend internal server failed");
                }
            });
        }

        if let Some(addr) = config.prof_http_addr.clone() {
            let shutdown = ctx.shutdown.clone();
            let ready = Arc::clone(&ready);
            background.spawn(async move { debug::serve(&addr, ready, shutdown).await });
        }

        sg_metrics::register_service_metadata(sg_metrics::VERSION);
        ctx.ready.mark_ready();
        ready.store(true, Ordering::SeqCst);

        let allowlist = ipallowlist::compile(&self.site_config, &self.allowlist_env).map(Arc::new);
        let security = SecurityConfig::from_site_config(&self.site_config);

        tracing::info!(addr = %external_addr, "frontend listening");
        let served = sg_http::serve_http(
            external,
            middleware::external_handler(self.version.bytes(), allowlist, security),
            ctx.shutdown.clone(),
        )
        .await;

        // A failure of the external listener must stop the sibling servers.
        ctx.shutdown.cancel();
        drain(&mut background).await;
        served.map_err(|err| ServiceError::with_source("frontend http server failed", err))
    }
}

/// Wait for every background server to observe the shutdown token.
async fn drain(background: &mut JoinSet<()>) {
    while background.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service_with_config(config: FrontendConfig) -> FrontendService {
        FrontendService {
            config: Some(config),
            ..FrontendService::with_version(Version::dev())
        }
    }

    #[test]
    fn service_name_is_frontend() {
        assert_eq!(service_name(), "frontend");
        assert_eq!(FrontendService::new().name(), "frontend");
    }

    #[test]
    fn configure_requires_env_registration() {
        let mut service = FrontendService::new();
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        assert!(service.config().is_some());
        assert!(service.validate().is_ok());
    }

    #[test]
    fn validate_requires_configuration() {
        assert!(FrontendService::new().validate().is_err());
    }

    #[test]
    fn configured_service_validates() {
        let service = service_with_config(FrontendConfig {
            insecure_dev: true,
            http_addr: "127.0.0.1:0".to_owned(),
            http_addr_internal: String::new(),
            prof_http_addr: None,
        });
        assert!(service.validate().is_ok());
        let config = service.config().expect("config");
        assert_eq!(config.external_bind_address(), "127.0.0.1:0");
        assert_eq!(config.internal_bind_address(), None);
    }
}
