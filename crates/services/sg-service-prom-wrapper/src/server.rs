#![forbid(unsafe_code)]

//! HTTP server and route dispatch (`main.go`).
//!
//! The Go wrapper builds a gorilla `mux.Router` (default `StrictSlash(false)`)
//! and registers, in order:
//!
//! 1. `/prom-wrapper/config-subscriber` (only when config syncing is enabled),
//! 2. `/alertmanager` prefix (only when Alertmanager is enabled),
//! 3. `/prom-wrapper/alerts-status`,
//! 4. `/` (Prometheus) as the catch-all.
//!
//! Each of the first two handlers is itself a `StrictSlash(true)` router. This
//! module reproduces the observable behavior: exact routes for the two
//! `prom-wrapper` endpoints (with a `301` trailing-slash redirect), prefix
//! routing for Alertmanager, and Prometheus as the fallback.

use std::sync::Arc;

use http::{header, HeaderValue, Request, Response, StatusCode};
use hyper::body::Incoming;
use tonic::body::BoxBody;

use crate::alertmanager::AlertmanagerClient;
use crate::proxy;
use crate::status::AlertsStatusReporter;
use crate::subscriber::SiteConfigSubscriber;

/// `srcprometheus.EndpointConfigSubscriber`.
pub const CONFIG_SUBSCRIBER_PATH: &str = "/prom-wrapper/config-subscriber";
/// `srcprometheus.EndpointAlertsStatus`.
pub const ALERTS_STATUS_PATH: &str = "/prom-wrapper/alerts-status";
/// `alertmanagerPathPrefix`, with its leading slash.
pub const ALERTMANAGER_PREFIX: &str = "/alertmanager";

const CONFIG_SUBSCRIBER_PATH_SLASH: &str = "/prom-wrapper/config-subscriber/";
const ALERTS_STATUS_PATH_SLASH: &str = "/prom-wrapper/alerts-status/";

/// Which upstream/handler a request should be dispatched to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    ConfigSubscriber,
    AlertsStatus,
    Alertmanager,
    Prometheus,
}

/// Flags that mirror which routes the Go wrapper registers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteFlags {
    pub disable_alertmanager: bool,
    pub disable_sourcegraph_config: bool,
}

/// Shared state for the export-port HTTP handler.
pub struct ServerState {
    pub prometheus_port: String,
    pub alertmanager_port: String,
    pub subscriber: Option<Arc<SiteConfigSubscriber>>,
    pub status: Arc<AlertsStatusReporter>,
    pub alertmanager: Arc<AlertmanagerClient>,
    pub disable_alertmanager: bool,
    pub disable_sourcegraph_config: bool,
}

impl ServerState {
    fn flags(&self) -> RouteFlags {
        RouteFlags {
            disable_alertmanager: self.disable_alertmanager,
            disable_sourcegraph_config: self.disable_sourcegraph_config,
        }
    }
}

/// Pure route selection for a request path.
///
/// Mirrors the registration order in `main.go`: the two wrapper endpoints win
/// over the Alertmanager prefix, which wins over the Prometheus catch-all. A
/// disabled feature is not registered in Go, so its path falls through to
/// Prometheus.
pub fn select_route(path: &str, flags: RouteFlags) -> Route {
    if config_subscriber_enabled(flags) && path == CONFIG_SUBSCRIBER_PATH {
        return Route::ConfigSubscriber;
    }
    if path == ALERTS_STATUS_PATH {
        return Route::AlertsStatus;
    }
    if !flags.disable_alertmanager
        && (path == ALERTMANAGER_PREFIX || path.starts_with("/alertmanager/"))
    {
        return Route::Alertmanager;
    }
    Route::Prometheus
}

/// The subscriber route exists only when the wrapper starts Alertmanager *and*
/// config syncing: in `main.go` the subscriber registration is nested inside
/// the `else` branch of `disableAlertmanager`.
fn config_subscriber_enabled(flags: RouteFlags) -> bool {
    !flags.disable_alertmanager && !flags.disable_sourcegraph_config
}

/// `StrictSlash(true)` behavior for the exact wrapper endpoints.
///
/// Returns the canonical path for a request that only differs by a trailing
/// slash, so the dispatcher can answer with a `301`.
pub fn trailing_slash_redirect(path: &str, flags: RouteFlags) -> Option<&'static str> {
    if config_subscriber_enabled(flags) && path == CONFIG_SUBSCRIBER_PATH_SLASH {
        return Some(CONFIG_SUBSCRIBER_PATH);
    }
    if path == ALERTS_STATUS_PATH_SLASH {
        return Some(ALERTS_STATUS_PATH);
    }
    None
}

/// Build the `sg_http::HttpHandler` served on `EXPORT_PORT`.
pub fn handler(state: Arc<ServerState>) -> sg_http::HttpHandler {
    Arc::new(move |request: Request<Incoming>| {
        let state = Arc::clone(&state);
        Box::pin(async move { dispatch(state, request).await })
    })
}

async fn dispatch(state: Arc<ServerState>, request: Request<Incoming>) -> Response<BoxBody> {
    let flags = state.flags();
    let path = request.uri().path();

    if let Some(target) = trailing_slash_redirect(path, flags) {
        return redirect(request.uri(), target);
    }

    match select_route(path, flags) {
        Route::ConfigSubscriber => match &state.subscriber {
            Some(subscriber) => subscriber.handle(request).await,
            // Defensive: a missing subscriber behaves like Go's unregistered
            // route and falls through to Prometheus.
            None => proxy_prometheus(&state, request).await,
        },
        Route::AlertsStatus => state.status.handle(request).await,
        Route::Alertmanager => {
            let authority = state.alertmanager_target();
            proxy::proxy(request, &authority).await
        }
        Route::Prometheus => proxy_prometheus(&state, request).await,
    }
}

impl ServerState {
    fn prometheus_target(&self) -> String {
        proxy_target(&self.prometheus_port)
    }

    fn alertmanager_target(&self) -> String {
        proxy_target(&self.alertmanager_port)
    }
}

fn proxy_target(port: &str) -> String {
    format!("127.0.0.1:{port}")
}

async fn proxy_prometheus(
    state: &Arc<ServerState>,
    request: Request<Incoming>,
) -> Response<BoxBody> {
    let authority = state.prometheus_target();
    proxy::proxy(request, &authority).await
}

/// `301` to the canonical path, preserving the query string.
fn redirect(uri: &http::Uri, target: &'static str) -> Response<BoxBody> {
    let location = match uri.query() {
        Some(query) => format!("{target}?{query}"),
        None => target.to_owned(),
    };

    let mut response = Response::new(tonic::body::empty_body());
    *response.status_mut() = StatusCode::MOVED_PERMANENTLY;
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::{
        select_route, trailing_slash_redirect, Route, RouteFlags, ALERTMANAGER_PREFIX,
        ALERTS_STATUS_PATH, CONFIG_SUBSCRIBER_PATH,
    };

    const ENABLED: RouteFlags = RouteFlags {
        disable_alertmanager: false,
        disable_sourcegraph_config: false,
    };
    const NO_ALERTMANAGER: RouteFlags = RouteFlags {
        disable_alertmanager: true,
        disable_sourcegraph_config: false,
    };
    const NO_CONFIG: RouteFlags = RouteFlags {
        disable_alertmanager: false,
        disable_sourcegraph_config: true,
    };

    #[test]
    fn selects_config_subscriber_endpoint() {
        assert_eq!(
            select_route(CONFIG_SUBSCRIBER_PATH, ENABLED),
            Route::ConfigSubscriber
        );
        assert_eq!(
            select_route(CONFIG_SUBSCRIBER_PATH, NO_CONFIG),
            Route::Prometheus
        );
        // The subscriber block is nested inside the Alertmanager-enabled branch.
        assert_eq!(
            select_route(CONFIG_SUBSCRIBER_PATH, NO_ALERTMANAGER),
            Route::Prometheus
        );
    }

    #[test]
    fn selects_alerts_status_endpoint() {
        assert_eq!(
            select_route(ALERTS_STATUS_PATH, ENABLED),
            Route::AlertsStatus
        );
        // The alerts reporter is registered even when Alertmanager is disabled
        // (it returns 503 itself); route selection must not skip it.
        assert_eq!(
            select_route(ALERTS_STATUS_PATH, NO_ALERTMANAGER),
            Route::AlertsStatus
        );
        assert_eq!(
            select_route(ALERTS_STATUS_PATH, NO_CONFIG),
            Route::AlertsStatus
        );
    }

    #[test]
    fn selects_alertmanager_prefix() {
        for path in [
            ALERTMANAGER_PREFIX,
            "/alertmanager/",
            "/alertmanager/api/v2/status",
            "/alertmanager/-/reload",
        ] {
            assert_eq!(select_route(path, ENABLED), Route::Alertmanager, "{path}");
        }
        assert_eq!(
            select_route("/alertmanager/api/v2/status", NO_ALERTMANAGER),
            Route::Prometheus
        );
    }

    #[test]
    fn everything_else_goes_to_prometheus() {
        for path in [
            "/",
            "/graph",
            "/api/v1/query",
            "/static/prometheus.js",
            "/prom-wrapper/other",
            "/alertmanagerfoo",
        ] {
            assert_eq!(select_route(path, ENABLED), Route::Prometheus, "{path}");
        }
    }

    #[test]
    fn trailing_slash_is_redirected_for_enabled_endpoints() {
        assert_eq!(
            trailing_slash_redirect("/prom-wrapper/config-subscriber/", ENABLED),
            Some(CONFIG_SUBSCRIBER_PATH)
        );
        assert_eq!(
            trailing_slash_redirect("/prom-wrapper/alerts-status/", ENABLED),
            Some(ALERTS_STATUS_PATH)
        );
        // Not registered when disabled: falls through to Prometheus instead.
        assert_eq!(
            trailing_slash_redirect("/prom-wrapper/config-subscriber/", NO_CONFIG),
            None
        );
        assert_eq!(
            trailing_slash_redirect("/prom-wrapper/config-subscriber/", NO_ALERTMANAGER),
            None
        );
        // Exact paths and prefix routes are not redirected.
        assert_eq!(
            trailing_slash_redirect(CONFIG_SUBSCRIBER_PATH, ENABLED),
            None
        );
        assert_eq!(trailing_slash_redirect("/alertmanager/", ENABLED), None);
    }
}
