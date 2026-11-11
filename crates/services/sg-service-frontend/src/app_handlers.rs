#![forbid(unsafe_code)]

//! Handlers registered by `cmd/frontend/internal/app` on the gorilla router
//! (`app.go`, `misc_handlers.go`, `opensearch.go`, `badge.go`).
//!
//! Everything the baseline reads from process-wide state (`conf.Get`,
//! `dotcom`, `deploy`, `env`, `assetsutil`) is injected through [`AppEnv`] and
//! [`AppConfig`]; the importers counter is an injectable async seam so the
//! differential harness can replace the internal GraphQL call with a mock.
//! The exact byte formats (redirect bodies, robots text, OpenSearch template,
//! badge URL escaping, `http.Error` bodies) were measured against the Go
//! oracle; see `docs/overviews/m9_8iu_app_router_overview.md` §4.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::header::{self, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;
use sg_session::{SessionUserLookup, COOKIE_NAME};
use time::OffsetDateTime;

use crate::sign_out::{serve_sign_out, SignOutDeps, SignOutRequest};

/// `badge.go:52`: the shields.io logo data URI.
const BADGE_LOGO: &str =
    "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA3Ny4wNzUgNzcuNyI+PHBhdGggZmlsbD0iI0ZGRiIgZD0iTTQ3LjMyMyA3Ny43Yy0zLjU5NCAwLTYuNzktMi4zOTYtNy43ODctNS45OWwtMTcuMTcyLTYxLjdjLS45OTgtNC4zOTMgMS41OTgtOC43ODYgNS45OS05Ljc4NCA0LjE5My0xIDguMzg3IDEuMzk3IDkuNTg0IDUuMzlsMTYuOTczIDYxLjdjMS4xOTggNC4zOTQtMS4zOTcgOC43ODYtNS41OSA5Ljk4NC0uNi4yLTEuMzk3LjQtMS45OTcuNHoiLz48cGF0aCBmaWxsPSIjRkZGIiBkPSJNMTcuMzcyIDcwLjcxYy00LjM5MyAwLTcuOTg3LTMuNTkzLTcuOTg3LTcuOTg1IDAtMS45OTcuOC0zLjk5NCAxLjk5Ny01LjM5Mkw1NC4xMTIgOS40MWMyLjk5NS0zLjM5MyA3Ljk4Ni0zLjU5MyAxMS4zOC0uNTk4czMuNTk1IDcuOTg3LjYgMTEuMzhsLTQyLjczIDQ3LjcyM2MtMS41OTcgMS43OTgtMy43OTQgMi43OTYtNS45OSAyLjc5NnoiLz48cGF0aCBmaWxsPSIjRkZGIiBkPSJNNjkuMDg3IDU2LjczNGMtLjc5OCAwLTEuNTk3LS4yLTIuNTk2LS40TDUuNTkgMzYuMzY4QzEuNCAzNC45Ny0uOTk3IDMwLjM3Ny40IDI2LjE4NGMxLjM5Ny00LjE5MyA1Ljk5LTYuNTkgMTAuMTgzLTUuMTlsNjAuOSAxOS45NjZjNC4xOTMgMS4zOTcgNi41OSA1Ljk5IDUuMTkgMTAuMTg0LS45OTYgMy4zOTQtMy45OSA1LjU5LTcuNTg2IDUuNTl6Ii8+PC9zdmc+";

/// `internal/backend/go_importers.go:147-150`.
const COUNT_GO_IMPORTERS_GRAPHQL_QUERY: &str = "\nquery CountGoImporters($query: String!, $version: SearchVersion!) {\n  search(query: $query, version: $version) { results { matchCount } }\n}";

/// `opensearch.go:13-21` (`html/template` source, placeholders substituted).
const OPEN_SEARCH_TEMPLATE: &str = r#"
<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/" xmlns:moz="http://www.mozilla.org/2006/browser/search/">
  <ShortName>{SITE_NAME}</ShortName>
  <Description>Search {SITE_NAME}</Description>
  <InputEncoding>UTF-8</InputEncoding>
  <Image height="16" width="16">data:image/png;base64,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</Image>
  <Url type="text/html" method="GET" template="{SEARCH_URL}" />
</OpenSearchDescription>
"#;

/// The deployment type the sitemap handler special-cases (`deploy.Dev`).
const DEPLOY_DEV: &str = "dev";
const RICKROLL_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

/// Process-wide values the handlers read in the baseline
/// (`ROBOTS_TXT_ALLOW`, `SOURCEGRAPHDOTCOM_MODE`, `DEPLOY_TYPE`,
/// `ASSETS_ROOT`, `INSECURE_DEV`, `SRC_FRONTEND_INTERNAL`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppEnv {
    /// Raw `ROBOTS_TXT_ALLOW` (parsed with `strconv.ParseBool` at use site).
    pub robots_allow: String,
    /// `dotcom.SourcegraphDotComMode()`.
    pub dotcom: bool,
    /// `deploy.Type()`, e.g. `"dev"`, `"kubernetes"`.
    pub deploy_type: String,
    /// `assetsutil.assetsRoot`.
    pub assets_root: String,
    /// `env.InsecureDev`.
    pub insecure_dev: bool,
    /// `version.Version()` (`"0.0.0+dev"` for dev builds).
    pub build_version: String,
    /// `internalapi.Client.URL`, the root of the internal HTTP API.
    pub internal_api_url: String,
}

impl Default for AppEnv {
    fn default() -> Self {
        Self {
            robots_allow: "false".to_owned(),
            dotcom: false,
            deploy_type: "kubernetes".to_owned(),
            assets_root: "/.assets".to_owned(),
            insecure_dev: false,
            build_version: "0.0.0+dev".to_owned(),
            internal_api_url: "http://sourcegraph-frontend-internal:80".to_owned(),
        }
    }
}

impl AppEnv {
    /// Reads the same environment variables as the baseline.
    pub fn from_env() -> Self {
        let get =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_owned());
        Self {
            robots_allow: get("ROBOTS_TXT_ALLOW", "false"),
            dotcom: go_parse_bool(&get("SOURCEGRAPHDOTCOM_MODE", "false")),
            deploy_type: {
                let value = get("DEPLOY_TYPE", "");
                if value.is_empty() {
                    "kubernetes".to_owned()
                } else {
                    value
                }
            },
            assets_root: get("ASSETS_ROOT", "/.assets"),
            insecure_dev: go_parse_bool(&get("INSECURE_DEV", "false")),
            // `internal/version` reads `/version.txt` at init and otherwise
            // keeps the unreleased-development default.
            build_version: std::fs::read_to_string("/version.txt")
                .unwrap_or_else(|_| "0.0.0+dev".to_owned()),
            internal_api_url: "http://sourcegraph-frontend-internal:80".to_owned(),
        }
    }
}

/// The site-configuration fields the handlers read (`conf.ExternalURLParsed()`,
/// `conf.Branding()`, `conf.Get().CorsOrigin`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppConfig {
    /// Raw `externalURL` (empty means the baseline's `http://example.com`).
    pub external_url: String,
    /// `branding.favicon` (empty means "use the default asset").
    pub branding_favicon: String,
    /// `conf.Get().CorsOrigin`; empty unless configured.
    pub cors_origin: String,
    /// `conf.Branding().BrandName` (defaults to `Sourcegraph`).
    pub brand_name: String,
    /// `branding.light != nil || branding.dark != nil` — suppresses the
    /// default mark preload in `newCommon`.
    pub branding_has_theme: bool,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            external_url: String::new(),
            branding_favicon: String::new(),
            cors_origin: String::new(),
            brand_name: "Sourcegraph".to_owned(),
            branding_has_theme: false,
        }
    }
}

impl AppConfig {
    pub fn from_site_config(config: &SiteConfiguration) -> Self {
        let raw = config.raw();
        let external_url = raw
            .get("externalURL")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let cors_origin = raw
            .get("corsOrigin")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let branding = raw.get("branding");
        let branding_favicon = branding
            .and_then(|branding| branding.get("favicon"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // `conf.Branding()` replaces a missing or empty name with the default.
        let brand_name = branding
            .and_then(|branding| branding.get("brandName"))
            .and_then(serde_json::Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or("Sourcegraph")
            .to_owned();
        let branding_has_theme = branding
            .map(|branding| {
                branding.get("light").is_some_and(|v| !v.is_null())
                    || branding.get("dark").is_some_and(|v| !v.is_null())
            })
            .unwrap_or(false);
        Self {
            external_url,
            branding_favicon,
            cors_origin,
            brand_name,
            branding_has_theme,
        }
    }

    /// `conf.ExternalURLParsed()`.
    ///
    /// The baseline caches the parsed URL and seeds it with
    /// `http://example.com` for the empty configuration value
    /// (`internal/conf/computed.go:442-456`); an empty `externalURL`
    /// therefore renders as `example.com`, not as an empty URL.
    pub fn external_url_parsed(&self) -> ParsedExternalUrl {
        if self.external_url.is_empty() {
            return ParsedExternalUrl {
                url: "http://example.com".to_owned(),
                host: "example.com".to_owned(),
            };
        }
        parse_absolute_url(&self.external_url)
    }
}

/// The subset of `*url.URL` the OpenSearch handler reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedExternalUrl {
    /// `u.String()`.
    pub url: String,
    /// `u.Host` (as written, including a port if present).
    pub host: String,
}

/// `backend.CountGoImporters`'s injectable transport. The production
/// implementation is [`SgHttpDoer`].
#[async_trait::async_trait]
pub trait GoImportersDoer: Send + Sync {
    /// `cli.Do(req)` + `io.ReadAll(resp.Body)`; the status is ignored, like the
    /// baseline.
    async fn post_json(&self, url: &str, body: Vec<u8>) -> Result<Vec<u8>, String>;
}

/// Production transport built on [`sg_http::HttpClient`].
pub struct SgHttpDoer {
    client: sg_http::HttpClient,
}

impl SgHttpDoer {
    pub fn new() -> Self {
        Self {
            client: sg_http::HttpClient::new(),
        }
    }
}

impl Default for SgHttpDoer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl GoImportersDoer for SgHttpDoer {
    async fn post_json(&self, url: &str, body: Vec<u8>) -> Result<Vec<u8>, String> {
        let response = self.client.post(url, &[], body).await?;
        Ok(response.body)
    }
}

/// `backend.MockCountGoImporters`.
pub type GoImportersMock = Arc<dyn Fn(&str) -> Result<i64, String> + Send + Sync>;

struct CounterInner {
    mock: Option<GoImportersMock>,
    doer: Option<Arc<dyn GoImportersDoer>>,
    /// `goImportersCountCache`: repo → (count, insert time), 4h TTL.
    cache: Mutex<HashMap<String, (i64, Instant)>>,
}

/// `backend.CountGoImporters` with the Redis-backed cache replaced by an
/// in-process map (the baseline's cache is also keyed only by repo name).
#[derive(Clone)]
pub struct GoImportersCounter {
    inner: Arc<CounterInner>,
}

impl Default for GoImportersCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl GoImportersCounter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CounterInner {
                mock: None,
                doer: None,
                cache: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn with_mock(mock: GoImportersMock) -> Self {
        Self {
            inner: Arc::new(CounterInner {
                mock: Some(mock),
                doer: None,
                cache: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn with_doer(doer: Arc<dyn GoImportersDoer>) -> Self {
        Self {
            inner: Arc::new(CounterInner {
                mock: None,
                doer: Some(doer),
                cache: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// `CountGoImporters`: mock first, then the dotcom restriction, then the
    /// 4h cache, then the internal GraphQL query.
    pub async fn count(&self, env: &AppEnv, repo: &str) -> Result<i64, String> {
        if let Some(mock) = &self.inner.mock {
            return mock(repo);
        }

        if !env.dotcom {
            // Avoid confusing users by exposing this on self-hosted instances.
            return Err(
                "counting Go importers is not supported on self-hosted instances".to_owned(),
            );
        }

        let ttl = Duration::from_secs(14400); // 4 hours
        if let Ok(cache) = self.inner.cache.lock() {
            if let Some((count, inserted)) = cache.get(repo) {
                if inserted.elapsed() < ttl {
                    return Ok(*count);
                }
            }
        }

        let doer = self.inner.doer.as_ref().ok_or_else(|| {
            "counting Go importers requires an internal GraphQL client".to_owned()
        })?;

        let body = serde_json::json!({
            "Query": COUNT_GO_IMPORTERS_GRAPHQL_QUERY,
            "Variables": {
                "query": count_go_importers_search_query(repo),
                "version": "V3",
            },
        });
        let body = serde_json::to_vec(&body).map_err(|err| err.to_string())?;
        let url = gql_url(&env.internal_api_url, "CountGoImporters");
        let response = doer.post_json(&url, body).await?;

        let value: serde_json::Value =
            serde_json::from_slice(&response).map_err(|err| format!("Decode: {err}"))?;
        if let Some(errors) = json_field_ci(&value, "Errors").and_then(|v| v.as_array()) {
            if !errors.is_empty() {
                return Err(format!("graphql: errors: {errors:?}"));
            }
        }
        let count = json_field_ci(&value, "Data")
            .and_then(|data| json_field_ci(data, "Search"))
            .and_then(|search| json_field_ci(search, "Results"))
            .and_then(|results| json_field_ci(results, "MatchCount"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);

        if let Ok(mut cache) = self.inner.cache.lock() {
            cache.insert(repo.to_owned(), (count, Instant::now()));
        }
        Ok(count)
    }
}

/// The `/-/sign-out` dependencies for one configuration generation.
///
/// Built by the live middleware from the `PgPool` (the session store is read
/// from the environment) so that `auth.providers` changes are picked up like
/// the baseline's per-request `conf.Get()`.
pub struct SignOutWiring {
    pub deps: Arc<SignOutDeps>,
    /// Actor resolution for `recordSecurityEvent`; `None` leaves the actor
    /// anonymous.
    pub users: Option<Arc<dyn SessionUserLookup>>,
}

/// Builds [`SignOutWiring`] from the current site configuration and the
/// external URL.
pub type SignOutFactory = Arc<dyn Fn(&SiteConfiguration, &str) -> SignOutWiring + Send + Sync>;

/// The dependencies the app router needs beyond configuration.
#[derive(Clone, Default)]
pub struct AppDeps {
    pub go_importers: GoImportersCounter,
    /// `/-/sign-out`; `None` until the live middleware supplies a pool.
    pub sign_out: Option<Arc<SignOutWiring>>,
    /// The remaining app handlers (M9.8jb); every entry is optional so an
    /// unwired process still answers the mux 404.
    pub more: crate::app_handlers_more::MoreDeps,
    /// The UI asset provider (`ui/assets.Provider`): the web build manifest and
    /// the hashed bundles it names. `None` keeps the UI route at the mux 404
    /// that M9.8iu measured.
    pub ui_assets: Option<Arc<sg_ui::UiAssets>>,
}

impl AppDeps {
    /// Production wiring (the internal API client is created lazily by the
    /// caller through [`GoImportersCounter::with_doer`]).
    pub fn production() -> Self {
        // `cmd/frontend/shared/service.go:24-26`.
        let root = if std::env::var("WEB_BUILDER_DEV_SERVER").as_deref() == Ok("1") {
            sg_ui::DEV_ASSETS_PATH
        } else {
            sg_ui::DEFAULT_ASSET_PATH
        };
        Self {
            go_importers: GoImportersCounter::with_doer(Arc::new(SgHttpDoer::new())),
            sign_out: None,
            more: crate::app_handlers_more::MoreDeps::default(),
            ui_assets: Some(Arc::new(sg_ui::UiAssets::for_path(root))),
        }
    }

    /// Test wiring with `backend.MockCountGoImporters` semantics.
    pub fn with_mock(mock: GoImportersMock) -> Self {
        Self {
            go_importers: GoImportersCounter::with_mock(mock),
            sign_out: None,
            more: crate::app_handlers_more::MoreDeps::default(),
            ui_assets: None,
        }
    }

    /// Attach the per-generation sign-out wiring.
    pub fn with_sign_out(mut self, wiring: SignOutWiring) -> Self {
        self.sign_out = Some(Arc::new(wiring));
        self
    }

    /// Attach a UI asset provider (hashed bundle manifest + file reader).
    pub fn with_ui_assets(mut self, assets: Arc<sg_ui::UiAssets>) -> Self {
        self.ui_assets = Some(assets);
        self
    }
}

/// The `/-/sign-out` request adapter (`serveSignOutHandler`).
///
/// The baseline resolves the actor in outer middleware and hands the cookies
/// to `session` helpers; here the actor is resolved from the same `sgs`
/// cookie with the session read path, and the response is translated back to
/// the `http.Redirect` + `Set-Cookie` shape.
pub async fn sign_out<B>(req: &Request<B>, deps: &AppDeps) -> Response<BoxBody> {
    let Some(wiring) = deps.sign_out.as_deref() else {
        return not_found();
    };
    let headers = req.headers();
    let session_cookie = cookie_value(headers, COOKIE_NAME);
    let sign_out_cookie = cookie_value(headers, "sg-signout").is_some();
    let anonymous_uid = cookie_value(headers, "sourcegraphAnonymousUid");
    let now = OffsetDateTime::now_utc();

    let actor_uid = match (&wiring.users, session_cookie.as_deref()) {
        (Some(users), Some(cookie)) => {
            match sg_session::authenticate(
                wiring.deps.store.as_ref(),
                users.as_ref(),
                &wiring.deps.cookie_key,
                Some(cookie),
                now,
            )
            .await
            {
                sg_session::SessionAuthOutcome::Authenticated { uid } => uid,
                _ => 0,
            }
        }
        _ => 0,
    };

    let request = SignOutRequest {
        actor_uid,
        path: req.uri().path(),
        // `r.URL.Host` is empty for server requests.
        host: "",
        anonymous_uid: anonymous_uid.as_deref(),
        session_cookie: session_cookie.as_deref(),
        sign_out_cookie,
        now,
    };
    let outcome = serve_sign_out(&wiring.deps, &request).await;

    let mut response =
        empty_response(StatusCode::from_u16(outcome.status).unwrap_or(StatusCode::SEE_OTHER));
    if let Ok(value) = HeaderValue::from_str(&outcome.location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    if let Ok(value) = HeaderValue::from_str(&outcome.content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    for cookie in &outcome.set_cookies {
        if let Ok(value) = HeaderValue::from_str(cookie) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
    }
    *response.body_mut() = tonic::body::boxed(Full::new(Bytes::from(outcome.body)));
    response
}

/// `r.Cookie(name)`: the first matching cookie across the `Cookie` headers.
fn cookie_value(headers: &http::HeaderMap, name: &str) -> Option<String> {
    for value in headers.get_all(header::COOKIE) {
        let Ok(cookies) = value.to_str() else {
            continue;
        };
        for cookie in cookies.split(';') {
            let cookie = cookie.trim();
            let Some((key, value)) = cookie.split_once('=') else {
                continue;
            };
            if key.trim() == name {
                let value = value.trim();
                let value = value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                    .unwrap_or(value);
                return Some(value.to_owned());
            }
        }
    }
    None
}

/// `robotsTxt` (`misc_handlers.go:22-42`).
pub fn robots_txt(env: &AppEnv) -> Response<BoxBody> {
    let allow = go_parse_bool(&env.robots_allow);
    let mut body = String::from("User-agent: *\n");
    if allow {
        body.push_str("Allow: /\n");
        if env.dotcom {
            body.push_str("Sitemap: https://sourcegraph.com/sitemap.xml.gz\n");
            body.push_str("Disallow: /search?q=*\n");
        }
    } else {
        body.push_str("Disallow: /\n");
    }
    body.push('\n');
    crate::middleware::text_response(StatusCode::OK, "text/plain", body)
}

/// `sitemapXmlGz` (`misc_handlers.go:44-51`). `number` is the raw route
/// variable including its underscore (or empty).
pub fn sitemap_xml_gz<B>(req: &Request<B>, env: &AppEnv, number: &str) -> Response<BoxBody> {
    if env.dotcom || env.deploy_type == DEPLOY_DEV {
        let location = format!(
            "https://storage.googleapis.com/sitemap-sourcegraph-com/sitemap{number}.xml.gz"
        );
        return redirect(req, &location, 302);
    }
    empty_response(StatusCode::NOT_FOUND)
}

/// `favicon` (`misc_handlers.go:53-66`).
pub fn favicon<B>(req: &Request<B>, env: &AppEnv, config: &AppConfig) -> Response<BoxBody> {
    // `assetsutil.URL` + the `v2` cache-busting query value, with Go's
    // `strings.Replace(url, "v2=", "v2", 1)` collapsing `?v2=` to `?v2`.
    let path = if config.branding_favicon.is_empty() {
        format!("{}?v2", assets_url(env, "/img/sourcegraph-mark.svg"))
    } else {
        config.branding_favicon.clone()
    };
    redirect(req, &path, 301)
}

/// `openSearch` (`opensearch.go:23-50`).
pub fn open_search(config: &AppConfig) -> Response<BoxBody> {
    let external = config.external_url_parsed();
    let site_name = if external.url == "https://sourcegraph.com" {
        "Sourcegraph".to_owned()
    } else {
        format!("Sourcegraph ({})", external.host)
    };
    let search_url = format!("{}/search?q={{searchTerms}}", external.url);
    let body = OPEN_SEARCH_TEMPLATE
        .replace("{SITE_NAME}", &template_html_escape(&site_name))
        .replace("{SEARCH_URL}", &template_html_escape(&search_url));
    crate::middleware::text_response(StatusCode::OK, "application/xml", body)
}

/// `serveRepoBadge` (`badge.go:44-67`).
pub async fn repo_badge<B>(
    req: &Request<B>,
    repo: &str,
    env: &AppEnv,
    deps: &AppDeps,
) -> Response<BoxBody> {
    let total_refs = match deps.go_importers.count(env, repo).await {
        Ok(count) => count,
        Err(err) => {
            // `errors.Wrap(err, "Defs.TotalRefs")` then `errcode.HTTP` maps a
            // generic error to 500.
            return go_error(500, &format!("Defs.TotalRefs: {err}"), env.insecure_dev);
        }
    };

    let style = query_param(req.uri().query(), "style").unwrap_or_default();
    let mut query = format!("logo={}", go_query_escape(BADGE_LOGO));
    if !style.is_empty() {
        query.push_str("&style=");
        query.push_str(&go_query_escape(&style));
    }
    let path = go_path_escape(&format!(
        "/badge/used by-{}-brightgreen.svg",
        badge_value_fmt(total_refs)
    ));
    let location = format!("https://img.shields.io{path}?{query}");
    redirect(req, &location, 307)
}

/// `badgeValueFmt` (`badge.go:31-42`).
pub fn badge_value_fmt(total_refs: i64) -> String {
    let desc = if total_refs >= 1000 {
        format!("{:.1}k projects", total_refs as f64 / 1000.0)
    } else {
        format!("{total_refs} projects")
    };
    // A prefixed space keeps shields.io from rendering `used by |12k`.
    format!(" {desc}")
}

/// The `/-/debug/headers` handler (`app.go:77-84`).
pub fn debug_headers<B>(req: &Request<B>) -> Response<BoxBody> {
    let mut entries: Vec<(String, Vec<String>)> = Vec::new();
    for (name, value) in req.headers() {
        let canonical = canonical_header_name(name.as_str());
        let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        match entries.last_mut() {
            Some((existing, values)) if *existing == canonical => values.push(value),
            _ => entries.push((canonical, vec![value])),
        }
    }
    if entries.iter().any(|(name, _)| name == "Cookie") {
        entries.retain(|(name, _)| name != "Cookie");
        entries.push(("Cookie".to_owned(), vec!["REDACTED".to_owned()]));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut body = String::new();
    for (name, values) in entries {
        for value in values {
            let value = value.replace(['\n', '\r'], " ");
            let value = trim_go_string(&value);
            body.push_str(&name);
            body.push_str(": ");
            body.push_str(value);
            body.push_str("\r\n");
        }
    }
    crate::middleware::text_response(StatusCode::OK, "text/plain; charset=utf-8", body)
}

/// `http.Redirect` (`net/http/server.go:2226-2292`).
pub fn redirect<B>(req: &Request<B>, location: &str, code: u16) -> Response<BoxBody> {
    let method = req.method();
    let location = redirect_location(req, location);

    let mut response = empty_response(StatusCode::from_u16(code).unwrap_or(StatusCode::FOUND));
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    if method == Method::GET || method == Method::HEAD {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
    }
    if method == Method::GET {
        let body = format!(
            "<a href=\"{}\">{}</a>.\n",
            redirect_html_escape(&location),
            status_text(code)
        );
        *response.body_mut() = tonic::body::boxed(Full::new(Bytes::from(body + "\n")));
    }
    response
}

/// `errorutil.Handler`'s error callback + `http.Error`.
pub fn go_error(status: u16, error: &str, insecure_dev: bool) -> Response<BoxBody> {
    let text = status_text(status);
    let body = if insecure_dev {
        format!("Error: HTTP {status} {text}\n\nError: {error}")
    } else {
        format!("Error: HTTP {status}: {text}")
    };
    let mut response = crate::middleware::text_response(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        "text/plain; charset=utf-8",
        body + "\n",
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// `http.NotFound` (used for every route whose handler is not ported yet).
pub fn not_found() -> Response<BoxBody> {
    crate::middleware::go_not_found()
}

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = status;
    response
}

/// Mirrors `cleanPath`/`path.Clean` for the `http.Redirect` relative branch.
fn redirect_location<B>(req: &Request<B>, location: &str) -> String {
    if is_absolute_url(location) {
        return location.to_owned();
    }

    let mut url = location.to_owned();
    if !url.starts_with('/') {
        let old_path = req.uri().path();
        let old_path = if old_path.is_empty() { "/" } else { old_path };
        let old_dir = match old_path.rfind('/') {
            Some(index) => &old_path[..=index],
            None => "",
        };
        url = format!("{old_dir}{url}");
    }

    let (mut path, query) = match url.find('?') {
        Some(index) => (url[..index].to_owned(), url[index..].to_owned()),
        None => (url, String::new()),
    };
    let trailing = path.ends_with('/');
    path = go_path_clean(&path);
    if trailing && !path.ends_with('/') {
        path.push('/');
    }
    path.push_str(&query);
    path
}

fn is_absolute_url(url: &str) -> bool {
    match url.find("://") {
        Some(index) => index > 0,
        None => url.starts_with("//"),
    }
}

/// Go `html/template`'s text/attribute escaper (also escapes `+`).
fn template_html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\0' => escaped.push('\u{FFFD}'),
            '"' => escaped.push_str("&#34;"),
            '&' => escaped.push_str("&amp;"),
            '\'' => escaped.push_str("&#39;"),
            '+' => escaped.push_str("&#43;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            ch => escaped.push(ch),
        }
    }
    escaped
}

/// Go `htmlReplacer` used by `http.Redirect` (does not escape `+`).
fn redirect_html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&#34;"),
            '\'' => escaped.push_str("&#39;"),
            ch => escaped.push(ch),
        }
    }
    escaped
}

/// `statusText` for the codes the ported handlers emit.
fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "",
    }
}

/// `strconv.ParseBool`.
fn go_parse_bool(value: &str) -> bool {
    matches!(
        value,
        "1" | "t" | "T" | "TRUE" | "true" | "True" | "0" | "f" | "F" | "FALSE" | "false" | "False"
    ) && matches!(value, "1" | "t" | "T" | "TRUE" | "true" | "True")
}

/// `assetsutil.URL(path.Join(baseURL.Path, p))`, rendered as a string.
fn assets_url(env: &AppEnv, path: &str) -> String {
    let root = &env.assets_root;
    match root.find("://") {
        Some(index) => {
            let prefix = &root[..index + 3];
            let rest = &root[index + 3..];
            let (authority, base_path) = match rest.find('/') {
                Some(slash) => (&rest[..slash], &rest[slash..]),
                None => (rest, ""),
            };
            let joined = go_path_clean(&format!("{base_path}/{path}"));
            format!("{prefix}{authority}{joined}")
        }
        None => go_path_clean(&format!("{root}/{path}")),
    }
}

/// `path.Clean`, without the `mux.cleanPath` trailing-slash preservation.
pub(crate) fn go_path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let rooted = path.starts_with('/');
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                if segments.last().is_some_and(|last| *last != "..") {
                    segments.pop();
                } else if !rooted {
                    segments.push("..");
                }
            }
            segment => segments.push(segment),
        }
    }
    let mut cleaned = String::new();
    if rooted {
        cleaned.push('/');
    }
    cleaned.push_str(&segments.join("/"));
    if cleaned.is_empty() {
        ".".to_owned()
    } else {
        cleaned
    }
}

/// `url.QueryEscape` (`encodeQueryComponent`): space becomes `+`.
pub(crate) fn go_query_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        let safe = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~');
        if safe {
            escaped.push(byte as char);
        } else if byte == b' ' {
            escaped.push('+');
        } else {
            escaped.push('%');
            escaped.push_str(&format!("{byte:02X}"));
        }
    }
    escaped
}

/// `url.escape(s, encodePath)` — only `?` among the reserved characters is
/// escaped; everything outside the unreserved set is percent-encoded.
pub(crate) fn go_path_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        let safe = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~');
        let reserved_kept = matches!(
            byte,
            b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@'
        );
        if safe || reserved_kept {
            escaped.push(byte as char);
        } else {
            escaped.push('%');
            escaped.push_str(&format!("{byte:02X}"));
        }
    }
    escaped
}

/// `url.Values.Get` without the `+`-as-space surprise: Go's `Query()` decodes
/// `+` to a space, which is what `form_urlencoded` does here.
pub(crate) fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    let query = query?;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key.as_ref() == name)
        .map(|(_, value)| value.into_owned())
}

/// `textproto.TrimString`: trims spaces and tabs at both ends.
fn trim_go_string(value: &str) -> &str {
    value.trim_matches(|ch| ch == ' ' || ch == '\t')
}

/// `textproto.CanonicalMIMEHeaderKey`: upper-cases the first character and
/// every character following a hyphen.
fn canonical_header_name(name: &str) -> String {
    let mut canonical = String::with_capacity(name.len());
    let mut upper = true;
    for ch in name.chars() {
        if upper {
            canonical.extend(ch.to_uppercase());
        } else {
            canonical.push(ch);
        }
        upper = ch == '-';
    }
    canonical
}

/// `regexp.QuoteMeta` (`\.+*?()|[]{}^$`).
fn quote_meta(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len());
    for ch in value.chars() {
        if "\\.+*?()|[]{}^$".contains(ch) {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted
}

/// `countGoImportersSearchQuery` (`internal/backend/go_importers.go:127-145`).
fn count_go_importers_search_query(repo: &str) -> String {
    [
        "type:file",
        "f:(^|/)go\\.mod$",
        "patterntype:regexp",
        &format!("content:^\\s*{}(/\\S+)?\\s+v\\d", quote_meta(repo)),
        "count:all",
        "visibility:public",
        "timeout:20s",
    ]
    .join(" ")
}

/// `gqlURL` (`internal/backend/go_importers.go:117-125`).
fn gql_url(internal_api_url: &str, query_name: &str) -> String {
    let (prefix, _rest) = match internal_api_url.find("://") {
        Some(index) => match internal_api_url[index + 3..].find('/') {
            Some(slash) => (
                &internal_api_url[..index + 3 + slash],
                &internal_api_url[index + 3 + slash..],
            ),
            None => (internal_api_url, ""),
        },
        None => (internal_api_url, ""),
    };
    format!("{prefix}/.internal/graphql?{query_name}")
}

/// Case-insensitive field lookup, like Go's `encoding/json`.
fn json_field_ci<'a>(value: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    value.as_object()?.iter().find_map(|(key, value)| {
        if key.eq_ignore_ascii_case(name) {
            Some(value)
        } else {
            None
        }
    })
}

/// `mustParseSourcegraphInternalURL`'s scheme/host/path subset.
fn parse_absolute_url(raw: &str) -> ParsedExternalUrl {
    let Some(index) = raw.find("://") else {
        return ParsedExternalUrl {
            url: raw.to_owned(),
            host: String::new(),
        };
    };
    let scheme = &raw[..index];
    let rest = &raw[index + 3..];
    let (authority, after_authority) = match rest.find(['/', '?', '#']) {
        Some(split) => (&rest[..split], &rest[split..]),
        None => (rest, ""),
    };
    let path = if after_authority.starts_with('/') {
        let end = after_authority
            .find(['?', '#'])
            .unwrap_or(after_authority.len());
        &after_authority[..end]
    } else {
        ""
    };
    let host = authority.rsplit('@').next().unwrap_or(authority);
    ParsedExternalUrl {
        url: format!("{scheme}://{authority}{path}"),
        host: host.to_owned(),
    }
}

/// Convenience wrapper used by the router dispatch.
pub fn rickroll<B>(req: &Request<B>) -> Response<BoxBody> {
    redirect(req, RICKROLL_URL, 302)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_matches_go() {
        for value in ["1", "t", "T", "TRUE", "true", "True"] {
            assert!(go_parse_bool(value), "{value}");
        }
        for value in ["0", "f", "F", "FALSE", "false", "False", "", "yes", "TRUE "] {
            assert!(!go_parse_bool(value), "{value}");
        }
    }

    #[test]
    fn badge_values() {
        assert_eq!(badge_value_fmt(12), " 12 projects");
        assert_eq!(badge_value_fmt(999), " 999 projects");
        assert_eq!(badge_value_fmt(1000), " 1.0k projects");
        assert_eq!(badge_value_fmt(1399), " 1.4k projects");
    }

    #[test]
    fn empty_external_url_is_example_com() {
        let parsed = AppConfig::default().external_url_parsed();
        assert_eq!(parsed.url, "http://example.com");
        assert_eq!(parsed.host, "example.com");
    }

    #[test]
    fn external_url_keeps_go_string_form() {
        let config = AppConfig {
            external_url: "https://sourcegraph.example.com".to_owned(),
            ..AppConfig::default()
        };
        let parsed = config.external_url_parsed();
        assert_eq!(parsed.url, "https://sourcegraph.example.com");
        assert_eq!(parsed.host, "sourcegraph.example.com");

        let config = AppConfig {
            external_url: "https://sourcegraph.example.com/".to_owned(),
            ..AppConfig::default()
        };
        assert_eq!(
            config.external_url_parsed().url,
            "https://sourcegraph.example.com/"
        );
    }

    #[test]
    fn escaping_matches_go() {
        assert_eq!(
            go_query_escape("a b+c/d=e;f,g:h"),
            "a+b%2Bc%2Fd%3De%3Bf%2Cg%3Ah"
        );
        assert_eq!(go_query_escape("~-._"), "~-._");
        assert_eq!(
            go_path_escape("/used by-1.4k/projects"),
            "/used%20by-1.4k/projects"
        );
        assert_eq!(go_path_escape("a?b:c"), "a%3Fb:c");
    }

    #[test]
    fn search_query_quotes_repo_meta() {
        assert_eq!(
            count_go_importers_search_query("github.com/foo/bar"),
            "type:file f:(^|/)go\\.mod$ patterntype:regexp content:^\\s*github\\.com/foo/bar(/\\S+)?\\s+v\\d count:all visibility:public timeout:20s"
        );
    }

    #[test]
    fn gql_url_replaces_the_path() {
        assert_eq!(
            gql_url(
                "http://sourcegraph-frontend-internal:80",
                "CountGoImporters"
            ),
            "http://sourcegraph-frontend-internal:80/.internal/graphql?CountGoImporters"
        );
        assert_eq!(
            gql_url("http://host:80/old?x=1", "N"),
            "http://host:80/.internal/graphql?N"
        );
    }

    #[test]
    fn assets_url_joins() {
        let env = AppEnv::default();
        assert_eq!(
            assets_url(&env, "/img/sourcegraph-mark.svg"),
            "/.assets/img/sourcegraph-mark.svg"
        );
        let env = AppEnv {
            assets_root: "https://cdn.example/base".to_owned(),
            ..AppEnv::default()
        };
        assert_eq!(
            assets_url(&env, "/img/x.svg"),
            "https://cdn.example/base/img/x.svg"
        );
    }

    #[test]
    fn headers_are_canonicalized() {
        assert_eq!(canonical_header_name("x-test"), "X-Test");
        assert_eq!(canonical_header_name("accept"), "Accept");
        assert_eq!(
            canonical_header_name("x-sourcegraph-client"),
            "X-Sourcegraph-Client"
        );
    }

    #[test]
    fn json_lookup_is_case_insensitive() {
        let value = serde_json::json!({"data": {"search": {"results": {"matchCount": 7}}}});
        assert_eq!(
            json_field_ci(&value, "Data")
                .and_then(|v| json_field_ci(v, "search"))
                .and_then(|v| json_field_ci(v, "results"))
                .and_then(|v| json_field_ci(v, "matchcount"))
                .and_then(serde_json::Value::as_i64),
            Some(7)
        );
    }
    struct EmptyStore;

    #[async_trait::async_trait]
    impl sg_session::SessionStore for EmptyStore {
        async fn load(&self, _id: &str) -> Result<Option<Vec<u8>>, sg_session::SessionStoreError> {
            Ok(None)
        }

        async fn save(
            &self,
            _id: &str,
            _payload: Vec<u8>,
            _ttl_seconds: i64,
        ) -> Result<(), sg_session::SessionStoreError> {
            Ok(())
        }

        async fn delete(&self, _id: &str) -> Result<(), sg_session::SessionStoreError> {
            Ok(())
        }
    }

    struct OkDb;

    #[async_trait::async_trait]
    impl crate::sign_out::SignOutDb for OkDb {
        async fn invalidate_sessions(&self, _uid: i32) -> Result<(), String> {
            Ok(())
        }

        async fn log_security_event(
            &self,
            _event: crate::sign_out::SecurityEventRecord,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn insert_event_log(
            &self,
            _event: sg_db::telemetry_events::EventLogRow,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct NoRemote;

    #[async_trait::async_trait]
    impl crate::sign_out::SsoRemoteSignOut for NoRemote {
        async fn sign_out(
            &self,
            _kind: crate::sign_out::SsoProviderKind,
            _session_data: Option<serde_json::Value>,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn sign_out_adapter_maps_the_baseline_response() {
        let wiring = SignOutWiring {
            deps: Arc::new(SignOutDeps {
                store: Arc::new(EmptyStore),
                ids: Arc::new(sg_session::FixedSessionIdSource(
                    "BUILTSESSIONIDBUILTSESSIONIDBUILTSESSIONID".into(),
                )),
                cookie_key: b"m9-wiring-key".to_vec(),
                secure: true,
                db: Arc::new(OkDb),
                sso_plans: Vec::new(),
                sso_remote: Arc::new(NoRemote),
                external_url: "https://example.com".to_owned(),
            }),
            users: None,
        };
        let deps = AppDeps::default().with_sign_out(wiring);
        let request = Request::builder()
            .method(Method::GET)
            .uri("/-/sign-out")
            .header(header::COOKIE, "sourcegraphAnonymousUid=anon-1")
            .body(())
            .expect("request");

        let response = sign_out(&request, &deps).await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers()[header::LOCATION], "/search");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let cookies: Vec<String> = response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect();
        assert_eq!(cookies.len(), 2, "{cookies:?}");
        assert!(cookies[0].starts_with("sgs="), "{cookies:?}");
        assert!(cookies[0].contains("Max-Age=2592000"), "{cookies:?}");
        assert_eq!(cookies[1], "sg-signout=true; Path=/; Secure");

        // Without a pool-backed wiring the route is the unported 404 (the
        // differential harness path).
        assert_eq!(
            sign_out(&request, &AppDeps::default()).await.status(),
            StatusCode::NOT_FOUND
        );
    }
}
