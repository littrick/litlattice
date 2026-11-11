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

/// `searchcontexts.IsEnabled` (`internal/search/searchcontexts/conf.go`):
/// `DISABLE_SEARCH_CONTEXTS` is parsed with Go's `strconv.ParseBool`; an
/// unparsable value leaves the feature enabled.
fn search_contexts_enabled() -> bool {
    match std::env::var("DISABLE_SEARCH_CONTEXTS") {
        Ok(value) => !matches!(value.as_str(), "1" | "t" | "T" | "TRUE" | "true" | "True"),
        Err(_) => true,
    }
}

/// The ranking GraphQL surface is only useful when its store tables exist
/// (they are created by the code intel migrations). Probing avoids a startup
/// failure on a frontend that has never run the code intel workers; when the
/// relation is absent the base schema stays untouched.
async fn ranking_tables_available(pool: &sqlx::PgPool) -> bool {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT to_regclass('codeintel_ranking_graph_keys')::text",
    )
    .fetch_one(pool)
    .await
    .map(|relation| relation.is_some())
    .unwrap_or(false)
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
        // `promauto` registers every GraphQL metric family at package init, so
        // they are scrapeable (with no samples) before the first request.
        crate::graphql_metrics::register();
        // The same applies to the log-event search metrics
        // (`user_usage_stats.go:288-352`).
        crate::log_events_metrics::register();
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
        let session_pool = pool.clone();
        let settings_pool = pool.clone();
        let sign_out_pool = pool.clone();
        // `app.NewHandler`'s `/-/sign-out` reads `conf.Get()` per request; the
        // wiring is rebuilt for each configuration generation in
        // `LiveMiddleware`, so the factory closes over the pool only.
        let sign_out_factory: crate::app_handlers::SignOutFactory = {
            let pool = sign_out_pool;
            Arc::new(move |site: &SiteConfiguration, external_url: &str| {
                crate::app_handlers::SignOutWiring {
                    deps: Arc::new(crate::sign_out::SignOutDeps::production(
                        pool.clone(),
                        site,
                        external_url,
                    )),
                    users: Some(Arc::new(crate::session_auth::PgSessionUsers::new(
                        pool.clone(),
                    ))),
                }
            })
        };
        let source = crate::configuration_source::DbConfigurationSource::new(pool)
            .map_err(ServiceError::new)?;
        crate::configuration_startup::override_from_env(&source)
            .await
            .map_err(|err| {
                ServiceError::new(format!("failed to apply site config overrides: {err}"))
            })?;
        let watch_source = source.clone();
        let extsvc_source = source.clone();
        let wiring = configuration_server::FrontendConfigurationServer::start_with_source(
            source,
            ctx.shutdown.clone(),
        )
        .await
        .map_err(|err| {
            ServiceError::new(format!("failed to initialize configuration service: {err}"))
        })?;
        // Baseline `serve_cmd.go:140`: `conf.MustValidateDefaults()` runs right
        // after `InitConfigurationServerFrontendOnly` and panics on a bad
        // default deployment document. The port surfaces the same text as a
        // startup error.
        crate::configuration_startup::validate_default_site_configs().map_err(ServiceError::new)?;
        // Baseline `serve_cmd.go:147-149`: `overrideGlobalSettings` writes the
        // `GLOBAL_SETTINGS_FILE` document to the `settings` table and fails
        // startup when it cannot (outside `overrideExtSvcConfig`'s scope).
        crate::configuration_watch::override_global_settings_from_env(&settings_pool)
            .await
            .map_err(|err| {
                ServiceError::new(format!("failed to override global settings: {err}"))
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

        // `overrideSiteConfig`/`overrideGlobalSettings` start one watcher each
        // (`go watchUpdate`). The polling replacement runs on the service's
        // JoinSet and observes the same shutdown token as the HTTP servers;
        // `serve_cmd.go`'s watcher failures only log and never fail startup.
        let extsvc_pool = settings_pool.clone();
        crate::configuration_watch::spawn_config_watchers(
            &mut background,
            watch_source,
            settings_pool,
            crate::configuration_watch::ConfigWatchPaths::from_env(),
            crate::configuration_watch::DEFAULT_WATCH_INTERVAL,
            ctx.shutdown.clone(),
        );

        let blame_deps: Arc<dyn crate::blame::BlameDeps> = Arc::new(
            crate::blame::BackendDeps::new(blame_pool, Arc::clone(wiring.live())),
        );
        // M9.8ii: `Query.search` and the internal `/.api/search/stream` handler
        // must share one `SearchDeps` instance; the search contexts and ranking
        // GraphQL stores read the same primary pool.
        let search_contexts_pool = search_pool.clone();
        let ranking_pool = search_pool.clone();
        // M9.8im: assemble the search runtime from the same static endpoints
        // `serve_cmd` uses (the live service connections). Missing zoekt
        // endpoints or an invalid endpoint list keeps the pre-existing
        // `TreeJobRunner::unconfigured()` behavior byte-for-byte.
        let search_site = wiring.live().current().site.clone();
        let search_deps: Arc<dyn crate::search_stream::SearchDeps> =
            match crate::service_connections::service_connections() {
                Ok(connections) => {
                    match crate::search_runtime::assemble(&search_pool, &connections, &search_site)
                    {
                        Ok(runtime) => Arc::new(crate::search_stream::BackendDeps::with_runtime(
                            search_pool,
                            runtime,
                        )),
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                "search runtime is not configured; search execution is not ported"
                            );
                            Arc::new(crate::search_stream::BackendDeps::new(search_pool))
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "service connections unavailable; search execution is not ported"
                    );
                    Arc::new(crate::search_stream::BackendDeps::new(search_pool))
                }
            };
        // `serve_cmd.go:143`: `keyring.Init` builds the encryption keys from
        // the site configuration and fails startup when one cannot be
        // constructed. The `conf.Watch` equivalent below rebuilds the ring in
        // place; a rebuild failure terminates the process like the baseline's
        // `panic`.
        let keyring = sg_keyring::init(&wiring.live().current().site)
            .await
            .map_err(|err| {
                ServiceError::with_source("initializing keyring", std::io::Error::other(err))
            })?;
        {
            let built_generation = wiring.live().generation();
            let keyring = Arc::clone(&keyring);
            let live = Arc::clone(wiring.live());
            let shutdown = ctx.shutdown.clone();
            background.spawn(async move {
                if let Err(err) = sg_keyring::watch(keyring, live, built_generation, shutdown).await
                {
                    tracing::error!(error = %err, "creating encryption keyring");
                    std::process::exit(2);
                }
            });
        }
        // `serve_cmd.go:153`: `overrideExtSvcConfig` runs after the keyring is
        // initialized so it can decrypt existing configs and encrypt new ones.
        // It fails startup when the file cannot be applied; the watcher below
        // only logs, like `watchUpdate`.
        crate::extsvc_config_file::override_ext_svc_config_from_env(
            &extsvc_source,
            &extsvc_pool,
            keyring.external_service_key(),
        )
        .await
        .map_err(|err| {
            ServiceError::new(format!("failed to override external service config: {err}"))
        })?;
        crate::extsvc_config_file::spawn_extsvc_config_watcher(
            &mut background,
            extsvc_source,
            extsvc_pool,
            keyring.external_service_key(),
            crate::configuration_watch::DEFAULT_WATCH_INTERVAL,
            ctx.shutdown.clone(),
        );
        // `Mutation.logEvent(s)` writes the `user_activity:*` all-time flags
        // through `redispool.Store`. Like the rate limiter below, a redigo
        // pool does not dial until the first command; an unreachable Redis at
        // startup only turns the flag writes into errors for the affected
        // events (the baseline behaves the same once the pool is broken).
        let log_events_store = match sg_redis::KeyValue::connect(&sg_redis::store_endpoint()).await
        {
            Ok(kv) => Some(Arc::new(kv)),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "log-event Redis store unavailable: user_activity flags will fail"
                );
                None
            }
        };
        // The assembled OSS schema is executed by `sg-graphql` (M9.8fb); the
        // session cookie authenticates external requests and the current-user
        // row backs `Query.currentUser` (M9.8fe). `Query.clientConfiguration`
        // reads `external_services` with that same pool.
        let clone_url_resolver = Arc::new(crate::clone_urls::ConfiguredCloneUrlResolver {
            first: Arc::new(crate::clone_urls::PgFirstRepoNameByCloneUrl::new(
                session_pool.clone(),
            )),
            services: Arc::new(crate::clone_urls::PgCloneUrlServiceSource::new(
                session_pool.clone(),
            )),
            live: Arc::clone(wiring.live()),
            key: keyring.external_service_key(),
        });
        let graphql_deps =
            SchemaGraphqlDeps::with_session(Arc::clone(&graphql_schema), session_pool.clone())
                .with_client_configuration(
                    Arc::new(
                        crate::client_configuration::PgExternalServiceConfigSource::new(
                            session_pool.clone(),
                        ),
                    ),
                    Arc::clone(&keyring),
                    Arc::clone(wiring.live()),
                )
                .with_external_services(
                    Arc::new(
                        crate::external_services::PgExternalServiceConnectionStore::new(
                            session_pool.clone(),
                        ),
                    ),
                    Arc::clone(&keyring),
                    Arc::clone(wiring.live()),
                )
                .with_webhook_logs(
                    Arc::new(crate::webhook_logs::PgWebhookLogConnectionStore::new(
                        session_pool.clone(),
                    )),
                    Arc::new(
                        crate::external_services::PgExternalServiceConnectionStore::new(
                            session_pool,
                        ),
                    ),
                    Arc::clone(&keyring),
                    Arc::clone(wiring.live()),
                )
                .with_clone_urls(clone_url_resolver)
                .with_log_events(log_events_store, Arc::clone(wiring.live()))
                .with_site_configuration(Arc::clone(wiring.store()), Arc::clone(wiring.server()));
        // M9.8ii: `internal/searchcontexts/init.go` installs the resolver only
        // when search contexts are enabled; the Pg store is registered through
        // the same optional seam.
        let search_contexts: crate::graphql_resolvers::SearchContextsDatabaseHandle = Arc::new(
            crate::search_contexts_store::PgSearchContextsStore::new(search_contexts_pool),
        );
        // The ranking surface is installed by the enterprise code intel hook;
        // probe for its tables so a frontend without migrations keeps the base
        // schema (and therefore byte-identical responses).
        let ranking = if ranking_tables_available(&ranking_pool).await {
            let live = Arc::clone(wiring.live());
            let raw_graph_key_root =
                sg_codeintel_ranking::document_reference_counts_graph_key(&live.current().site);
            Some((
                Arc::new(crate::ranking_store::PgRankingService::new(
                    ranking_pool,
                    live,
                )) as Arc<dyn crate::ranking_graphql::RankingService>,
                raw_graph_key_root,
            ))
        } else {
            None
        };
        // `serve_cmd.go:213`: `Query.search` plans and executes through the
        // same client deps as `/.api/search/stream`.
        let search_executor: Arc<dyn crate::search_graphql::SearchExecutor> = Arc::new(
            crate::search_graphql::BackendSearchExecutor::new(Arc::clone(&search_deps)),
        );
        // M9.8ix: `Repository.commit`, `GitObject` and `GitRef.target` read
        // through gitserver via the shared `sg-git` client.
        let graphql_deps = graphql_deps.with_git_objects(Arc::new(
            crate::git_graphql::GitserverGitObjects::new(Arc::clone(wiring.live())),
        ));
        let graphql_deps = graphql_deps
            .with_production_surfaces(
                Some(search_contexts),
                search_contexts_enabled(),
                ranking,
                Some(search_executor),
            )
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register production GraphQL surfaces: {err}"
                ))
            })?;
        // `serve_cmd.go:436-448`: the anonymous GraphQL limiter stores GCRA
        // state in the Redis cache pool (`gql:rl:` prefix). A redigo pool does
        // not dial at construction (failures surface per request); here an
        // unreachable Redis at startup only disables the limiter.
        let graphql_deps: Arc<dyn GraphqlDeps> =
            match sg_redis::KeyValue::connect(&sg_redis::cache_endpoint()).await {
                Ok(kv) => {
                    let watcher = crate::rate_limit::BasicLimitWatcher::new(Arc::new(
                        crate::rate_limit::RedisGcraStore::new(kv),
                    ));
                    Arc::new(graphql_deps.with_rate_limit(watcher, Arc::clone(wiring.live())))
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "GraphQL rate limiter disabled: Redis is unreachable"
                    );
                    Arc::new(graphql_deps)
                }
            };

        if let Some((addr, listener)) = internal {
            let shutdown = ctx.shutdown.clone();
            let store = Arc::clone(wiring.store());
            let live = Arc::clone(wiring.live());
            let graphql_deps = Arc::clone(&graphql_deps);
            let search_deps = Arc::clone(&search_deps);
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
                let handler =
                    middleware::internal_handler(live, Arc::clone(&graphql_deps), search_deps);
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
                Some(sign_out_factory),
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
