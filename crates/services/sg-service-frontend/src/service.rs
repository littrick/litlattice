#![forbid(unsafe_code)]

//! `cmd/frontend/internal/cli/serve_cmd.go` process skeleton: the external and
//! internal listeners plus the debug server.
//!
//! The configuration path is ported (M9.8el): `overrideSiteConfig`, the DB
//! configuration service and a live, generation-cached middleware config. The
//! rest of the baseline startup sequence (Redis wait, migrations, out-of-band
//! migration validation, GraphQL schema, enterprise hooks) is not ported yet.
//! This slice binds the listeners, answers the health/version surface, marks
//! ready and shuts down all servers and the configuration tasks together.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use tokio::task::JoinSet;

use sg_config::SiteConfiguration;

use crate::config::FrontendConfig;
use crate::configuration_server;
use crate::graphql_api::{GraphqlDeps, SchemaGraphqlDeps};
use crate::ipallowlist::AllowlistEnv;
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
    /// The `conf.Get()` snapshot: the merged `SITE_CONFIG_FILE` at configure
    /// time, empty until the configuration service is ported.
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
        // Baseline `serve_cmd.go:135`: `overrideSiteConfig` fails startup when
        // the file cannot be read or merged. Without the variable the baseline
        // database seeds `confdefaults.Default`, so the port falls back to the
        // deployment default document (which includes the `builtin` auth
        // provider); `SITE_CONFIG_FILE` replaces it wholesale
        // (`WriteWithOverride`).
        self.site_config = match crate::site_config_file::load_from_env().map_err(|err| {
            ConfigError::new(format!("failed to apply site config overrides: {err}"))
        })? {
            Some(site_config) => site_config,
            None => crate::deploy_defaults::default_site_config(
                &std::env::var(crate::deploy_defaults::DEPLOY_TYPE_VAR).unwrap_or_default(),
            )
            .map_err(ConfigError::new)?,
        };
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

        // Baseline `serve_cmd.go:134-139`: apply the `SITE_CONFIG_FILE`
        // override first, then start the configuration service. Like the
        // baseline (whose `InitDB` fails before this point), the process needs
        // a database.
        let pool = configuration_server::FrontendConfigurationServer::connect_frontend_pool()
            .await
            .map_err(ServiceError::new)?;
        let blame_pool = pool.clone();
        let search_pool = blame_pool.clone();
        let source = crate::configuration_source::DbConfigurationSource::new(pool)
            .map_err(ServiceError::new)?;
        crate::configuration_startup::override_from_env(&source)
            .await
            .map_err(|err| {
                ServiceError::new(format!("failed to apply site config overrides: {err}"))
            })?;
        let wiring = configuration_server::FrontendConfigurationServer::start_with_source(
            source,
            ctx.shutdown.clone(),
        )
        .await
        .map_err(|err| {
            ServiceError::new(format!("failed to initialize configuration service: {err}"))
        })?;

        // Baseline `serve_cmd.go:213-219`: `graphqlbackend.NewSchema` assembles
        // the SDL and returns an error that aborts startup. The OSS schema is
        // `schema.graphql` + `outbound_webhooks.graphql` + `viewer.graphql`
        // (enterprise `OptionalResolver`s are nil). Resolvers and execution are
        // not ported yet, but assembling here keeps the fail-fast contract.
        let graphql_schema =
            Arc::new(sg_graphql::schema::assemble_oss().map_err(|err| {
                ServiceError::new(format!("failed to parse GraphQL schema: {err}"))
            })?);
        tracing::info!(
            types = graphql_schema.type_count(),
            query = graphql_schema.query_type.as_deref().unwrap_or("-"),
            mutation = graphql_schema.mutation_type.as_deref().unwrap_or("-"),
            "assembled GraphQL schema"
        );

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

        let blame_deps: Arc<dyn crate::blame::BlameDeps> = Arc::new(
            crate::blame::BackendDeps::new(blame_pool, Arc::clone(wiring.live())),
        );
        let search_deps: Arc<dyn crate::search_stream::SearchDeps> =
            Arc::new(crate::search_stream::BackendDeps::new(search_pool));
        // The assembled OSS schema is executed by `sg-graphql` (M9.8fb);
        // business resolvers are still unported (M9.8fc scope note).
        let graphql_deps: Arc<dyn GraphqlDeps> =
            Arc::new(SchemaGraphqlDeps::new(Arc::clone(&graphql_schema)));

        if let Some((addr, listener)) = internal {
            let shutdown = ctx.shutdown.clone();
            let store = Arc::clone(wiring.store());
            let live = Arc::clone(wiring.live());
            let graphql_deps = Arc::clone(&graphql_deps);
            background.spawn(async move {
                // `httpapi.go:397` registers the configuration service on the
                // internal gRPC server; `serve_cmd.go:397` multiplexes gRPC
                // and the internal HTTP handler on the same listener
                // (`internalgrpc.MultiplexHandlers`).
                let routes = tonic::service::Routes::new(
                    sg_proto::internalapi::config_service_server::ConfigServiceServer::new(
                        crate::configuration_api::ConfigApi::new(store),
                    ),
                );
                let handler = middleware::internal_handler(live, Arc::clone(&graphql_deps));
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

        tracing::info!(addr = %external_addr, "frontend listening");
        let served = sg_http::serve_http(
            external,
            middleware::external_handler_with_live(
                Arc::clone(wiring.live()),
                self.allowlist_env.clone(),
                self.version.bytes(),
                blame_deps,
                search_deps,
                graphql_deps,
            ),
            ctx.shutdown.clone(),
        )
        .await;

        // A failure of the external listener must stop the sibling servers.
        ctx.shutdown.cancel();
        drain(&mut background).await;
        wiring.shutdown().await;
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
    fn configure_loads_the_deployment_default_site_config() {
        if std::env::var(crate::site_config_file::SITE_CONFIG_FILE_VAR).is_ok() {
            // The ambient environment supplies its own configuration.
            return;
        }
        let mut service = FrontendService::new();
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        // The deployment default carries a builtin provider, so ForbidAll must
        // not reject the instance like an empty configuration would.
        assert!(!crate::auth::forbidden(&service.site_config));
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
