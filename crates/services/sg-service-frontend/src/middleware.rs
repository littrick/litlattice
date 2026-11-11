#![forbid(unsafe_code)]

//! The outer half of `newExternalHTTPHandler`'s middleware chain:
//!
//! ```text
//! ipAllowlist → tenant → instrumentation/trace → ForbidAll → GoGet →
//! BlackHole → healthCheck → ServeMux
//! ```
//!
//! The baseline wraps the mux with `ipAllowlist → tenant → instrumentation →
//! tracepkg → ForbidAll → GoGet → BlackHole → healthCheck → gcontext → Trace`.
//! `ForbidAll` reads the site configuration snapshot (m9_8ec); the
//! instrumentation/trace layers only contribute response headers here
//! (m9_8ed). Span export, metrics and slow-request logging are not ported.

use std::sync::Arc;

use bytes::Bytes;
use http::{header, HeaderValue, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use tonic::body::BoxBody;

use sg_config::LiveConfig;

use crate::actor_chain::{ChainActor, ChainConfig, ChainDeps, ChainOutcome, ExternalMode};
use crate::app_handlers::{AppConfig, AppDeps, AppEnv, SignOutFactory};
use crate::app_router;
use crate::auth;
use crate::blame::BlameDeps;
use crate::goimportpath;
use crate::graphql_api::GraphqlDeps;
use crate::health;
use crate::ipallowlist::{self, AllowlistEnv, IpAllowlistConfig};
use crate::mux::{self, SecurityConfig};
use crate::oauth_routes::{OAuthRouteConfig, OAuthRouteDeps, OAuthRouteFactory};
use crate::saml_wiring::{SamlRouteDeps, SamlRouteFactory};
use crate::search_stream::SearchDeps;
use crate::tenant;
use crate::trace_headers;
use crate::trace_policy;

/// `middleware.isBlackhole`.
pub fn is_blackhole<B>(req: &Request<B>) -> bool {
    // We no longer support github webhooks.
    if req.uri().path() == "/api/ext/github/webhook" {
        return true;
    }

    // We no longer support gRPC on the external API.
    req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        == Some("application/grpc")
}

/// `middleware.BlackHole`: `410 Gone` with no body.
pub fn blackhole_response() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = StatusCode::GONE;
    response
}

/// The end of the external chain: `http.NotFound`.
pub fn go_not_found() -> Response<BoxBody> {
    let mut response = text_response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        Bytes::from_static(b"404 page not found\n"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    response
}

/// A response with an explicit Content-Type (used by the mux handlers).
pub fn text_response(
    status: StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(body.into())));
    *response.status_mut() = status;
    if let Ok(value) = header::HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

/// The chain with an injected next handler; the differential harness passes a
/// `"next"` handler, the service passes [`go_not_found`].
pub fn external_response_with_next<B>(
    req: &Request<B>,
    version: &Bytes,
    next: impl FnOnce() -> Response<BoxBody>,
) -> Response<BoxBody> {
    if is_blackhole(req) {
        return blackhole_response();
    }
    if let Some(response) = health::health_check(req.uri().path(), version) {
        return response;
    }
    next()
}

/// The chain with the baseline's outer layers: `ipAllowlist` runs before the
/// tenant lookup, which runs before the instrumentation/trace layers;
/// `ForbidAll` follows them and precedes GoGet, BlackHole and healthCheck, so
/// a denied `/healthz` is a 403 and an unknown tenant beats a go-get response.
/// `None` means the allowlist is disabled (`configResolver()` returned nil);
/// `forbid_all` is [`auth::forbidden`]'s result for the current site
/// configuration.
///
/// The trace layers never reject: they mint IDs after the tenant middleware
/// succeeds and stamp `X-Trace`/`X-Trace-Span`/`X-Trace-URL` on whatever the
/// inner chain returns (including ForbidAll's 403). ipAllowlist and tenant
/// rejections happen outside them and carry no trace headers.
pub fn external_response_with_ipallowlist<B>(
    req: &mut Request<B>,
    version: &Bytes,
    allowlist: Option<&IpAllowlistConfig>,
    forbid_all: bool,
    trace_url_template: Option<&str>,
    external_url: &str,
    next: impl FnOnce(&mut Request<B>) -> Response<BoxBody>,
) -> Response<BoxBody> {
    if let Some(allowlist) = allowlist {
        if let Err(error) = allowlist.is_authorized(req) {
            return ipallowlist::forbidden_response(&error);
        }
    }
    if let Err(response) = tenant::external_tenant_from_hostname(req, tenant::fixed_tenant_one) {
        return response;
    }
    // `instrumentation.HTTPMiddleware` decides the per-request shouldTrace
    // value before starting the span; keep it on the request for inner layers
    // (GraphQL/slow-request capture). The trace response headers below are
    // unaffected.
    let should_trace = trace_policy::request_should_trace(req.headers(), req.uri().query());
    req.extensions_mut()
        .insert(trace_policy::ShouldTrace(should_trace));
    let trace = trace_headers::TraceIds::generate();
    let mut response = (|| {
        if forbid_all {
            return auth::forbidden_response();
        }
        if let Some(response) = goimportpath::response(req, external_url) {
            return response;
        }
        if is_blackhole(req) {
            return blackhole_response();
        }
        if let Some(response) = health::health_check(req.uri().path(), version) {
            return response;
        }
        next(req)
    })();
    trace_headers::apply(&mut response, &trace, trace_url_template, external_url);
    response
}

/// [`external_response_with_ipallowlist`] with an asynchronous inner handler,
/// used by the external listener where `/.api/blame/...` awaits the database
/// and gitserver before the response status is known.
pub async fn external_response_with_ipallowlist_async<'a, B, F>(
    req: &'a mut Request<B>,
    version: &Bytes,
    allowlist: Option<&IpAllowlistConfig>,
    forbid_all: bool,
    trace_url_template: Option<&str>,
    external_url: &str,
    next: F,
) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    F: for<'b> FnOnce(
        &'b mut Request<B>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Response<BoxBody>> + Send + 'b>,
    >,
{
    if let Some(allowlist) = allowlist {
        if let Err(error) = allowlist.is_authorized(req) {
            return ipallowlist::forbidden_response(&error);
        }
    }
    if let Err(response) = tenant::external_tenant_from_hostname(req, tenant::fixed_tenant_one) {
        return response;
    }
    let should_trace = trace_policy::request_should_trace(req.headers(), req.uri().query());
    req.extensions_mut()
        .insert(trace_policy::ShouldTrace(should_trace));
    let trace = trace_headers::TraceIds::generate();
    let mut response = if forbid_all {
        auth::forbidden_response()
    } else if let Some(response) = goimportpath::response(req, external_url) {
        response
    } else if is_blackhole(req) {
        blackhole_response()
    } else if let Some(response) = health::health_check(req.uri().path(), version) {
        response
    } else {
        next(req).await
    };
    trace_headers::apply(&mut response, &trace, trace_url_template, external_url);
    response
}

/// `newExternalHTTPHandler` without its config-dependent layers.
pub fn external_response<B>(
    req: &mut Request<B>,
    version: &Bytes,
    external_url: &str,
) -> Response<BoxBody> {
    external_response_with_ipallowlist(req, version, None, false, None, external_url, |_| {
        go_not_found()
    })
}

/// The hyper entry point for the external listener.
pub fn external_handler(
    version: Bytes,
    allowlist: Option<Arc<IpAllowlistConfig>>,
    forbid_all: bool,
    trace_url_template: Option<String>,
    security: SecurityConfig,
) -> sg_http::HttpHandler {
    let app_env = Arc::new(AppEnv::from_env());
    let app_config = AppConfig {
        external_url: security.external_url.clone(),
        cors_origin: security.cors_origin.clone(),
        ..AppConfig::default()
    };
    let app_deps = Arc::new(AppDeps::production());
    Arc::new(move |mut req: Request<Incoming>| {
        let version = version.clone();
        let allowlist = allowlist.clone();
        let trace_url_template = trace_url_template.clone();
        let security = security.clone();
        let app_env = Arc::clone(&app_env);
        let app_config = app_config.clone();
        let app_deps = Arc::clone(&app_deps);
        Box::pin(async move {
            external_response_with_ipallowlist_async(
                &mut req,
                &version,
                allowlist.as_deref(),
                forbid_all,
                trace_url_template.as_deref(),
                &security.external_url,
                |req| {
                    let security = security.clone();
                    let app_env = Arc::clone(&app_env);
                    let app_config = app_config.clone();
                    let app_deps = Arc::clone(&app_deps);
                    Box::pin(async move {
                        mux::route_async(req, &security, |route, request| {
                            let app_env = Arc::clone(&app_env);
                            let app_config = app_config.clone();
                            let app_deps = Arc::clone(&app_deps);
                            Box::pin(async move {
                                match route {
                                    mux::Route::App => {
                                        app_router::dispatch_async_with_body(
                                            request,
                                            &app_env,
                                            &app_config,
                                            &app_deps,
                                        )
                                        .await
                                    }
                                    // `/.assets/` is mounted beside the actor
                                    // chain in the baseline, not inside it.
                                    mux::Route::Assets { path } => match &app_deps.ui_assets {
                                        Some(assets) => crate::ui_assets::serve_asset(
                                            request,
                                            &path,
                                            &app_config.cors_origin,
                                            assets.readonly().as_ref(),
                                        ),
                                        None => {
                                            mux::not_found_dispatch(mux::Route::Assets { path })
                                        }
                                    },
                                    other => mux::not_found_dispatch(other),
                                }
                            })
                        })
                        .await
                    })
                },
            )
            .await
        })
    })
}

/// The middleware state derived from one configuration snapshot: what the
/// baseline computes lazily from `conf.Get()`(`conf.Cached`).
struct DerivedConfig {
    allowlist: Option<Arc<IpAllowlistConfig>>,
    forbid_all: bool,
    trace_url_template: Option<String>,
    security: SecurityConfig,
    /// The actor/authz chain's per-snapshot configuration (`conf.Get()` per
    /// request in the baseline).
    chain_config: ChainConfig,
    /// `/.auth/<provider>/{login,callback}` routing (`conf.Get()` per
    /// generation).
    oauth_config: OAuthRouteConfig,
    /// Production OAuth seams; `None` only in the tests that build
    /// [`LiveMiddleware`] without an OAuth factory.
    oauth: Option<Arc<OAuthRouteDeps>>,
    /// Production SAML seams (`conf.Get()` per generation).
    saml: Option<Arc<SamlRouteDeps>>,
    app_env: Arc<AppEnv>,
    app_config: AppConfig,
    app_deps: Arc<AppDeps>,
}

impl DerivedConfig {
    fn from_site(
        site: &sg_config::SiteConfiguration,
        env: &AllowlistEnv,
        app_env: Arc<AppEnv>,
        app_deps: Arc<AppDeps>,
        sign_out_factory: Option<&SignOutFactory>,
        userpasswd_factory: Option<&crate::userpasswd::UserpasswdFactory>,
        oauth_factory: Option<&OAuthRouteFactory>,
        saml_factory: Option<&SamlRouteFactory>,
    ) -> Self {
        // `conf.Watch` equivalent (`internal/tracer.newConfWatcher`): refresh
        // the process-global trace policy for each configuration generation.
        crate::trace_policy::apply_tracing_config(site);
        let security = SecurityConfig::from_site_config(site);
        let chain_config = ChainConfig::from_site(site, app_env.dotcom);
        let app_deps = match sign_out_factory {
            Some(factory) => {
                let wiring = factory(site, &security.external_url);
                Arc::new((*app_deps).clone().with_sign_out(wiring))
            }
            None => app_deps,
        };
        let app_deps = match userpasswd_factory {
            Some(factory) => {
                let wiring = factory(site, &security.external_url);
                Arc::new((*app_deps).clone().with_userpasswd(wiring))
            }
            None => app_deps,
        };
        Self {
            allowlist: ipallowlist::compile(site, env).map(Arc::new),
            forbid_all: auth::forbidden(site),
            trace_url_template: trace_headers::url_template(site),
            security,
            chain_config,
            oauth_config: OAuthRouteConfig::from_site(site, app_env.dotcom),
            oauth: oauth_factory.map(|factory| factory(site)),
            saml: saml_factory.map(|factory| factory(site)),
            app_env,
            app_config: AppConfig::from_site_config(site),
            app_deps,
        }
    }
}

/// `conf.Cached` for the config-dependent middleware layers: derived values
/// are cached by [`LiveConfig::generation`], so a configuration update is
/// picked up by the next request without a restart (except for the options in
/// `needRestartToApply`, which stay in the restart marker).
pub struct LiveMiddleware {
    live: Arc<LiveConfig>,
    allowlist_env: AllowlistEnv,
    app_env: Arc<AppEnv>,
    app_deps: Arc<AppDeps>,
    /// `/-/sign-out` wiring rebuilt per configuration generation (the
    /// baseline reads `conf.Get()` per request).
    sign_out_factory: Option<SignOutFactory>,
    /// Built-in auth wiring rebuilt per configuration generation.
    userpasswd_factory: Option<crate::userpasswd::UserpasswdFactory>,
    /// OAuth route dependencies rebuilt per configuration generation.
    oauth_factory: Option<OAuthRouteFactory>,
    /// SAML route dependencies rebuilt per configuration generation.
    saml_factory: Option<SamlRouteFactory>,
    cache: std::sync::Mutex<Option<(u64, Arc<DerivedConfig>)>>,
}

impl LiveMiddleware {
    pub fn new(
        live: Arc<LiveConfig>,
        allowlist_env: AllowlistEnv,
        app_env: Arc<AppEnv>,
        app_deps: Arc<AppDeps>,
        sign_out_factory: Option<SignOutFactory>,
        userpasswd_factory: Option<crate::userpasswd::UserpasswdFactory>,
        oauth_factory: Option<OAuthRouteFactory>,
        saml_factory: Option<SamlRouteFactory>,
    ) -> Self {
        Self {
            live,
            allowlist_env,
            app_env,
            app_deps,
            sign_out_factory,
            userpasswd_factory,
            oauth_factory,
            saml_factory,
            cache: std::sync::Mutex::new(None),
        }
    }

    fn current(&self) -> Arc<DerivedConfig> {
        // Read the generation before the snapshot: a racing `set` then leaves
        // a cache entry tagged with the older generation, which the next
        // request recomputes (the reverse order could pin stale values).
        let generation = self.live.generation();
        if let Ok(cache) = self.cache.lock() {
            if let Some((cached, derived)) = cache.as_ref() {
                if *cached == generation {
                    return Arc::clone(derived);
                }
            }
        }
        let derived = Arc::new(DerivedConfig::from_site(
            &self.live.current().site,
            &self.allowlist_env,
            Arc::clone(&self.app_env),
            Arc::clone(&self.app_deps),
            self.sign_out_factory.as_ref(),
            self.userpasswd_factory.as_ref(),
            self.oauth_factory.as_ref(),
            self.saml_factory.as_ref(),
        ));
        if let Ok(mut cache) = self.cache.lock() {
            *cache = Some((generation, Arc::clone(&derived)));
        }
        derived
    }
}

/// The hyper entry point for the external listener, reading the middleware
/// configuration from the live configuration snapshot (`conf.Get()` per
/// request, cached by generation).
#[allow(clippy::too_many_arguments)] // Dependency-injection seam; one argument per subsystem.
pub fn external_handler_with_live(
    live: Arc<LiveConfig>,
    allowlist_env: AllowlistEnv,
    sign_out_factory: Option<SignOutFactory>,
    userpasswd_factory: Option<crate::userpasswd::UserpasswdFactory>,
    oauth_factory: Option<OAuthRouteFactory>,
    saml_factory: Option<SamlRouteFactory>,
    version: Bytes,
    blame_deps: Arc<dyn BlameDeps>,
    search_deps: Arc<dyn SearchDeps>,
    graphql_deps: Arc<dyn GraphqlDeps>,
    chain_deps: Arc<ChainDeps>,
) -> sg_http::HttpHandler {
    let api_live = Arc::clone(&live);
    let app_env = Arc::new(AppEnv::from_env());
    let app_deps = Arc::new(AppDeps::production());
    let middleware = Arc::new(LiveMiddleware::new(
        live,
        allowlist_env,
        app_env,
        app_deps,
        sign_out_factory,
        userpasswd_factory,
        oauth_factory,
        saml_factory,
    ));
    Arc::new(move |mut req: Request<Incoming>| {
        let middleware = Arc::clone(&middleware);
        let api_live = Arc::clone(&api_live);
        let blame_deps = Arc::clone(&blame_deps);
        let search_deps = Arc::clone(&search_deps);
        let graphql_deps = Arc::clone(&graphql_deps);
        let chain_deps = Arc::clone(&chain_deps);
        let version = version.clone();
        Box::pin(async move {
            let derived = middleware.current();
            external_response_with_ipallowlist_async(
                &mut req,
                &version,
                derived.allowlist.as_deref(),
                derived.forbid_all,
                derived.trace_url_template.as_deref(),
                &derived.security.external_url,
                |req| {
                    let security = derived.security.clone();
                    let chain_config = derived.chain_config.clone();
                    let oauth_config = derived.oauth_config.clone();
                    let oauth = derived.oauth.clone();
                    let saml = derived.saml.clone();
                    let app_env = Arc::clone(&derived.app_env);
                    let app_config = derived.app_config.clone();
                    let app_deps = Arc::clone(&derived.app_deps);
                    let api_live = Arc::clone(&api_live);
                    let blame_deps = Arc::clone(&blame_deps);
                    let search_deps = Arc::clone(&search_deps);
                    let graphql_deps = Arc::clone(&graphql_deps);
                    let chain_deps = Arc::clone(&chain_deps);
                    Box::pin(async move {
                        mux::route_async(req, &security, |route, request| {
                            let app_env = Arc::clone(&app_env);
                            let app_config = app_config.clone();
                            let app_deps = Arc::clone(&app_deps);
                            let api_live = Arc::clone(&api_live);
                            let blame_deps = Arc::clone(&blame_deps);
                            let search_deps = Arc::clone(&search_deps);
                            let graphql_deps = Arc::clone(&graphql_deps);
                            let chain_config = chain_config.clone();
                            let chain_deps = Arc::clone(&chain_deps);
                            let oauth_config = oauth_config.clone();
                            let oauth = oauth.clone();
                            let saml = saml.clone();
                            Box::pin(async move {
                                match &route {
                                    // `/.api/` and `/` are the baseline's only
                                    // mounts wrapped in the actor chain; the
                                    // executor proxy implements its own auth.
                                    mux::Route::Api | mux::Route::App => {
                                        let mode = match &route {
                                            mux::Route::Api => ExternalMode::Api,
                                            _ => ExternalMode::App,
                                        };
                                        // The provider middlewares run before
                                        // `RequireAuthMiddleware` (and before
                                        // `AnonymousUID`/featureflag/PostAuth)
                                        // and take over their whole prefix
                                        // without consulting the actor.
                                        if mode == ExternalMode::App {
                                            if let Some(deps) = &oauth {
                                                if let Some(response) =
                                                    crate::oauth_routes::auth_prefix_response(
                                                        request,
                                                        &oauth_config,
                                                        deps,
                                                    )
                                                    .await
                                                {
                                                    return response;
                                                }
                                            }
                                            // `saml.Middleware` (`auth/init.go:50`)
                                            // serves `/.auth/saml/*` before the
                                            // actor is consulted.
                                            if let Some(deps) = &saml {
                                                if let Some(response) =
                                                    crate::saml_wiring::saml_route_response(
                                                        deps, request, false, false,
                                                    )
                                                    .await
                                                {
                                                    return response;
                                                }
                                            }
                                        }
                                        match crate::actor_chain::run_external_chain(
                                            request,
                                            mode,
                                            &chain_config,
                                            &chain_deps,
                                        )
                                        .await
                                        {
                                            ChainOutcome::Reject(response) => {
                                                // `RequireAuthMiddleware` is
                                                // inside the provider
                                                // middlewares: replace its
                                                // sign-in redirect when the
                                                // single-provider condition
                                                // holds.
                                                if mode == ExternalMode::App {
                                                    // `openidconnect.Middleware`
                                                    // runs before SAML in
                                                    // `auth/init.go`.
                                                    if let Some(deps) = &oauth {
                                                        if let Some(redirect) =
                                                            crate::oauth_routes::oidc_app_redirect(
                                                                request,
                                                                &oauth_config,
                                                                deps,
                                                                false,
                                                            )
                                                            .await
                                                        {
                                                            return redirect;
                                                        }
                                                    }
                                                    // SAML's single-provider
                                                    // redirect is unconditional
                                                    // (it does not depend on the
                                                    // access-requests setting).
                                                    if let Some(deps) = &saml {
                                                        if let Some(redirect) =
                                                            crate::saml_wiring::saml_route_response(
                                                                deps, request, false, true,
                                                            )
                                                            .await
                                                        {
                                                            return redirect;
                                                        }
                                                    }
                                                    if let Some(redirect) =
                                                        crate::oauth_routes::single_provider_redirect(
                                                            request,
                                                            &oauth_config,
                                                            false,
                                                        )
                                                    {
                                                        if crate::oauth_routes::is_sign_in_redirect(
                                                            &response,
                                                        ) {
                                                            return redirect;
                                                        }
                                                    }
                                                }
                                                response
                                            }
                                            ChainOutcome::Proceed { actor, vary } => {
                                                if mode == ExternalMode::App
                                                    && !actor.is_authenticated()
                                                {
                                                    if let Some(deps) = &oauth {
                                                        if let Some(redirect) =
                                                            crate::oauth_routes::oidc_app_redirect(
                                                                request,
                                                                &oauth_config,
                                                                deps,
                                                                false,
                                                            )
                                                            .await
                                                        {
                                                            return redirect;
                                                        }
                                                    }
                                                    if let Some(deps) = &saml {
                                                        if let Some(redirect) =
                                                            crate::saml_wiring::saml_route_response(
                                                                deps, request, false, true,
                                                            )
                                                            .await
                                                        {
                                                            return redirect;
                                                        }
                                                    }
                                                    if let Some(redirect) =
                                                        crate::oauth_routes::single_provider_redirect(
                                                            request,
                                                            &oauth_config,
                                                            false,
                                                        )
                                                    {
                                                        return redirect;
                                                    }
                                                }
                                                request.extensions_mut().insert(ChainActor(actor));
                                                let mut response = match &route {
                                                    mux::Route::Api => {
                                                        crate::external_api::api_response_with_deps(
                                                            request,
                                                            &api_live,
                                                            blame_deps,
                                                            search_deps,
                                                            graphql_deps,
                                                        )
                                                        .await
                                                    }
                                                    _ => {
                                                        app_router::dispatch_async_with_body(
                                                            request,
                                                            &app_env,
                                                            &app_config,
                                                            &app_deps,
                                                        )
                                                        .await
                                                    }
                                                };
                                                for value in vary {
                                                    response.headers_mut().append(
                                                        header::VARY,
                                                        HeaderValue::from_static(value),
                                                    );
                                                }
                                                response
                                            }
                                        }
                                    }
                                    // `/.assets/` is mounted on the outer mux
                                    // beside the actor chain.
                                    mux::Route::Assets { path } => {
                                        match &app_deps.ui_assets {
                                            Some(assets) => crate::ui_assets::serve_asset(
                                                request,
                                                path,
                                                &app_config.cors_origin,
                                                assets.readonly().as_ref(),
                                            ),
                                            None => mux::not_found_dispatch(route.clone()),
                                        }
                                    }
                                    other => mux::not_found_dispatch(other.clone()),
                                }
                            })
                        })
                        .await
                    })
                },
            )
            .await
        })
    })
}

/// The hyper entry point for the internal listener. The gitserver shard
/// list and pins are read from the live configuration per request, like the
/// baseline's `atomicGitServerConns` (which follows `conf.Get`).
pub fn internal_handler(
    live: Arc<LiveConfig>,
    graphql_deps: Arc<dyn GraphqlDeps>,
    search_deps: Arc<dyn SearchDeps>,
) -> sg_http::HttpHandler {
    std::sync::Arc::new(move |mut req: Request<Incoming>| {
        let live = Arc::clone(&live);
        let graphql_deps = Arc::clone(&graphql_deps);
        let search_deps = Arc::clone(&search_deps);
        Box::pin(async move {
            // `tenant.InternalHTTPMiddleware` is the outermost internal layer;
            // `actor.HTTPMiddleware` follows it. The resolved actor reaches
            // `graphql_api` through the `ChainActor` extension.
            if let Err(response) = tenant::internal_tenant_from_header(&mut req) {
                return response;
            }
            let actor = match crate::actor_middleware::apply_actor_header(req.headers()) {
                crate::actor_middleware::ActorHeaderDecision::Proceed(actor) => actor,
                crate::actor_middleware::ActorHeaderDecision::Reject { status, body } => {
                    return crate::actor_middleware::actor_header_reject_response(status, body);
                }
            };
            req.extensions_mut().insert(ChainActor(actor));
            // `featureflag.Middleware` sits inside `actor.HTTPMiddleware` on the
            // internal mux and only contributes this header.
            let mut response = crate::internal_api::internal_response_with_deps(
                &mut req,
                &live,
                graphql_deps,
                search_deps,
            )
            .await;
            response
                .headers_mut()
                .append(header::VARY, HeaderValue::from_static("Cookie"));
            response
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(path: &str) -> Request<()> {
        Request::builder().uri(path).body(()).expect("request")
    }

    #[test]
    fn blackhole_paths_are_gone() {
        assert_eq!(
            external_response(
                &mut request("/api/ext/github/webhook"),
                &Bytes::from_static(b"v"),
                ""
            )
            .status(),
            StatusCode::GONE
        );

        let mut grpc = Request::builder()
            .uri("/healthz")
            .header(header::CONTENT_TYPE, "application/grpc")
            .body(())
            .expect("request");
        assert_eq!(
            external_response(&mut grpc, &Bytes::from_static(b"v"), "").status(),
            StatusCode::GONE
        );

        // The content-type check is exact: parameters do not match.
        let mut grpc_params = Request::builder()
            .uri("/healthz")
            .header(header::CONTENT_TYPE, "application/grpc+proto")
            .body(())
            .expect("request");
        assert_eq!(
            external_response(&mut grpc_params, &Bytes::from_static(b"v"), "").status(),
            StatusCode::OK
        );
    }

    #[test]
    fn health_paths_are_served_by_the_middleware() {
        let response =
            external_response(&mut request("/healthz"), &Bytes::from_static(b"1.2.3"), "");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn everything_else_is_the_mux_404() {
        let response =
            external_response(&mut request("/.api/graphql"), &Bytes::from_static(b"v"), "");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
    }

    #[test]
    fn go_get_is_served_before_blackhole_and_health() {
        let mut req = request("/sourcegraph/sourcegraph?go-get=1");
        let response = external_response(
            &mut req,
            &Bytes::from_static(b"v"),
            "https://sourcegraph.com",
        );
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn forbid_all_precedes_go_get_health_and_the_mux() {
        for path in [
            "/sourcegraph/sourcegraph?go-get=1",
            "/healthz",
            "/__version",
            "/.api/graphql",
        ] {
            let mut req = request(path);
            let response = external_response_with_ipallowlist(
                &mut req,
                &Bytes::from_static(b"v"),
                None,
                true,
                None,
                "https://sourcegraph.com",
                |_| go_not_found(),
            );
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "text/plain; charset=utf-8",
                "{path}"
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::X_CONTENT_TYPE_OPTIONS)
                    .unwrap(),
                "nosniff",
                "{path}"
            );
        }
    }

    #[test]
    fn forbid_all_runs_after_the_allowlist() {
        let allowlist = IpAllowlistConfig {
            authorized_client_ips: vec!["10.0.0.1".parse().unwrap()],
            ..IpAllowlistConfig::default()
        };
        let mut denied = request("/");
        denied
            .extensions_mut()
            .insert(sg_http::RemoteAddr("192.0.2.10:1234".to_owned()));
        let response = external_response_with_ipallowlist(
            &mut denied,
            &Bytes::from_static(b"v"),
            Some(&allowlist),
            true,
            None,
            "",
            |_| go_not_found(),
        );
        // The allowlist's HTML rejection wins over ForbidAll's plain text.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
    }

    fn assert_hex(value: &str, len: usize) {
        assert_eq!(value.len(), len, "{value}");
        assert!(
            value
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()),
            "{value}"
        );
    }

    #[test]
    fn trace_headers_stamp_every_response_after_the_tenant() {
        let template = "https://traces.example/{{ .TraceID }}";
        for (path, forbid_all) in [
            ("/healthz", false),
            ("/sourcegraph/sourcegraph?go-get=1", false),
            ("/api/ext/github/webhook", false),
            ("/.api/graphql", false),
            ("/.api/graphql", true),
        ] {
            let mut req = request(path);
            let response = external_response_with_ipallowlist(
                &mut req,
                &Bytes::from_static(b"v"),
                None,
                forbid_all,
                Some(template),
                "https://ext",
                |_| go_not_found(),
            );
            let trace = response
                .headers()
                .get(trace_headers::TRACE_HEADER)
                .unwrap_or_else(|| panic!("{path} has no X-Trace"))
                .to_str()
                .unwrap();
            assert_hex(trace, 32);
            assert_hex(
                response
                    .headers()
                    .get(trace_headers::TRACE_SPAN_HEADER)
                    .unwrap()
                    .to_str()
                    .unwrap(),
                16,
            );
            assert_eq!(
                response
                    .headers()
                    .get(trace_headers::TRACE_URL_HEADER)
                    .unwrap()
                    .to_str()
                    .unwrap(),
                format!("https://traces.example/{trace}"),
                "{path}"
            );
        }
    }

    #[test]
    fn trace_url_is_written_empty_without_a_template() {
        let mut req = request("/.api/graphql");
        let response = external_response_with_ipallowlist(
            &mut req,
            &Bytes::from_static(b"v"),
            None,
            false,
            None,
            "",
            |_| go_not_found(),
        );
        assert_eq!(
            response
                .headers()
                .get(trace_headers::TRACE_URL_HEADER)
                .unwrap(),
            ""
        );
    }

    #[test]
    fn allowlist_rejections_carry_no_trace_headers() {
        let allowlist = IpAllowlistConfig {
            authorized_client_ips: vec!["10.0.0.1".parse().unwrap()],
            ..IpAllowlistConfig::default()
        };
        let mut denied = request("/");
        denied
            .extensions_mut()
            .insert(sg_http::RemoteAddr("192.0.2.10:1234".to_owned()));
        let response = external_response_with_ipallowlist(
            &mut denied,
            &Bytes::from_static(b"v"),
            Some(&allowlist),
            false,
            Some("https://traces.example/{{ .TraceID }}"),
            "",
            |_| go_not_found(),
        );
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(response
            .headers()
            .get(trace_headers::TRACE_HEADER)
            .is_none());
        assert!(response
            .headers()
            .get(trace_headers::TRACE_SPAN_HEADER)
            .is_none());
        assert!(response
            .headers()
            .get(trace_headers::TRACE_URL_HEADER)
            .is_none());
    }

    #[test]
    fn allowlist_denial_precedes_the_tenant_middleware() {
        let allowlist = IpAllowlistConfig {
            authorized_client_ips: vec!["10.0.0.1".parse().unwrap()],
            ..IpAllowlistConfig::default()
        };
        let mut denied = request("/some/path");
        denied
            .extensions_mut()
            .insert(sg_http::RemoteAddr("192.0.2.10:1234".to_owned()));
        let response = external_response_with_ipallowlist(
            &mut denied,
            &Bytes::from_static(b"v"),
            Some(&allowlist),
            false,
            None,
            "",
            |_| go_not_found(),
        );
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(denied
            .extensions()
            .get::<crate::tenant::TenantId>()
            .is_none());

        // An empty allowlist authorizes the request, which then reaches the
        // tenant mapper (success) and the next handler.
        let mut allowed = request("/some/path");
        allowed
            .extensions_mut()
            .insert(sg_http::RemoteAddr("192.0.2.10:1234".to_owned()));
        let response = external_response_with_ipallowlist(
            &mut allowed,
            &Bytes::from_static(b"v"),
            Some(&IpAllowlistConfig::default()),
            false,
            None,
            "",
            |_| go_not_found(),
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            allowed.extensions().get::<crate::tenant::TenantId>(),
            Some(&crate::tenant::TenantId(1))
        );
    }

    fn live_config(site: &str) -> Arc<LiveConfig> {
        let raw = sg_config::RawUnified {
            site: site.to_owned(),
            ..sg_config::RawUnified::default()
        };
        Arc::new(LiveConfig::new(
            sg_config::Unified::parse(&raw).expect("site configuration"),
        ))
    }

    #[test]
    fn live_middleware_recomputes_after_configuration_changes() {
        let live = live_config(
            r#"{"auth.providers":[{"type":"builtin","allowSignup":true}],
                "auth.allowedIpAddress":{"enabled":true,"userIpAddress":[]}}"#,
        );
        let middleware = LiveMiddleware::new(
            Arc::clone(&live),
            AllowlistEnv::default(),
            Arc::new(AppEnv::default()),
            Arc::new(AppDeps::default()),
            None,
            None,
            None,
            None,
        );

        let derived = middleware.current();
        assert!(!derived.forbid_all);
        assert!(derived.allowlist.is_some());

        // Same generation: the derived values are reused.
        let again = middleware.current();
        assert!(Arc::ptr_eq(&derived, &again));

        // A configuration update (empty providers, no allowlist) is visible to
        // the next call without rebuilding the middleware.
        let updated = sg_config::RawUnified {
            site: r#"{"auth.providers":[]}"#.to_owned(),
            ..sg_config::RawUnified::default()
        };
        live.set(sg_config::Unified::parse(&updated).expect("updated configuration"));
        let derived = middleware.current();
        assert!(derived.forbid_all);
        assert!(derived.allowlist.is_none());
    }
}
