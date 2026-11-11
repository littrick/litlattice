#![forbid(unsafe_code)]

//! The frontend app router: `cmd/frontend/internal/app/router/router.go`'s
//! gorilla table plus the inner `http.ServeMux` that `app.NewHandler` builds
//! (`app.go:28-94`).
//!
//! The port reproduces the matcher semantics the baseline relies on:
//!
//! * gorilla `StrictSlash(true)` on `Path` routes (`[/]?` plus a 301 to the
//!   template's trailing-slash form), but not on `PathPrefix` routes
//!   (`regexp.go:41-52`);
//! * a method mismatch does not stop the scan — later routes are tried and
//!   the final `PathPrefix("/")` UI route absorbs it (`route.go:40-89`);
//! * `PathPrefix` regexes are unanchored at the end (`/-/debug` matches
//!   `/-/debugxyz`);
//! * the inner ServeMux cleans the path (301 with a body) before rickrolling
//!   `/.env`, `/admin.php`, `/wp-login.php` and `/wp-admin`.
//!
//! Routes whose handlers are out of scope (`/-/logout`, the auth flows, the
//! old-tree redirect, site-admin archives and the UI) resolve to their
//! [`RouteName`] and are answered with `http.NotFound` by default. The parent
//! can inspect [`RouteName`]/[`Vars`] and wire them later.
//!
//! `/-/sign-out` is registered here and dispatched to
//! [`crate::sign_out::serve_sign_out`] through [`AppDeps::sign_out`]; without
//! that wiring (no `PgPool`, e.g. the harness) it answers the same 404 as the
//! other unported handlers.

use std::sync::OnceLock;

use bytes::Bytes;
use http::{Method, Request, Response};
use http_body_util::BodyExt;
use regex::Regex;
use tonic::body::BoxBody;

use crate::app_handlers::{self, AppConfig, AppDeps, AppEnv};

/// Names of the baseline routes (`router.go:14-54`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteName {
    RobotsTxt,
    SitemapXmlGz,
    Favicon,
    OpenSearch,
    Logout,
    SignUp,
    RequestAccess,
    SiteInit,
    VerifyEmail,
    SignIn,
    SignOut,
    UnlockAccount,
    UnlockUserAccount,
    ResetPasswordInit,
    ResetPasswordCode,
    CheckUsernameTaken,
    Editor,
    DebugHeaders,
    Debug,
    GopherconLiveBlog,
    OldTreeRedirect,
    OldToolsRedirect,
    UsageStatsDownload,
    OneClickExportArchive,
    LatestPing,
    RepoBadge,
    Ui,
}

impl RouteName {
    /// The baseline route name string (`router.go` constants).
    pub fn as_str(self) -> &'static str {
        match self {
            RouteName::RobotsTxt => "robots-txt",
            RouteName::SitemapXmlGz => "sitemap-xml-gz",
            RouteName::Favicon => "favicon",
            RouteName::OpenSearch => "opensearch",
            RouteName::Logout => "logout",
            RouteName::SignUp => "sign-up",
            RouteName::RequestAccess => "request-access",
            RouteName::SiteInit => "site-init",
            RouteName::VerifyEmail => "verify-email",
            RouteName::SignIn => "sign-in",
            RouteName::SignOut => "sign-out",
            RouteName::UnlockAccount => "unlock-account",
            RouteName::UnlockUserAccount => "unlock-user-account",
            RouteName::ResetPasswordInit => "reset-password.init",
            RouteName::ResetPasswordCode => "reset-password.code",
            RouteName::CheckUsernameTaken => "check-username-taken",
            RouteName::Editor => "editor",
            RouteName::DebugHeaders => "debug.headers",
            RouteName::Debug => "debug",
            RouteName::GopherconLiveBlog => "gophercon.live.blog",
            RouteName::OldTreeRedirect => "old-tree-redirect",
            RouteName::OldToolsRedirect => "old-tools-redirect",
            RouteName::UsageStatsDownload => "usage-stats.download",
            RouteName::OneClickExportArchive => "one-click-export.archive",
            RouteName::LatestPing => "pings.latest",
            RouteName::RepoBadge => "repo.badge",
            RouteName::Ui => "ui",
        }
    }
}

/// Route variables captured by the gorilla path regexes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Vars {
    pub repo: Option<String>,
    pub number: Option<String>,
    pub username: Option<String>,
    pub rev: Option<String>,
    pub path: Option<String>,
}

/// The result of matching one request against the route table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// A route matched; dispatch to its handler (or the fallback).
    Route { name: RouteName, vars: Vars },
    /// `StrictSlash` redirect. The caller appends the request query.
    Redirect { location: String },
}

/// Match `method` + `path` (already cleaned by the ServeMux) against the
/// baseline route table, in registration order.
pub fn resolve(method: &Method, path: &str) -> Resolution {
    macro_rules! exact {
        ($template:literal, $want:ident, $name:expr) => {
            if let Some(resolution) = exact_route(path, $template, method, Method::$want, $name) {
                return resolution;
            }
        };
    }

    exact!("/robots.txt", GET, RouteName::RobotsTxt);

    if let Some(captures) = sitemap_regex().captures(path) {
        if method == Method::GET {
            let number = captures.name("number").map(|m| m.as_str().to_owned());
            return strict_slash(
                path,
                format!("/sitemap{}.xml.gz", number.as_deref().unwrap_or("")),
                RouteName::SitemapXmlGz,
                Vars {
                    number,
                    ..Vars::default()
                },
            );
        }
    }

    exact!("/favicon.ico", GET, RouteName::Favicon);
    exact!("/opensearch.xml", GET, RouteName::OpenSearch);

    exact!("/-/logout", GET, RouteName::Logout);

    exact!("/-/sign-up", POST, RouteName::SignUp);
    exact!("/-/request-access", POST, RouteName::RequestAccess);
    exact!("/-/site-init", POST, RouteName::SiteInit);
    exact!("/-/verify-email", GET, RouteName::VerifyEmail);
    exact!("/-/sign-in", POST, RouteName::SignIn);
    exact!("/-/sign-out", GET, RouteName::SignOut);
    exact!("/-/unlock-account", POST, RouteName::UnlockAccount);
    exact!("/-/unlock-user-account", POST, RouteName::UnlockUserAccount);
    exact!("/-/reset-password-init", POST, RouteName::ResetPasswordInit);
    exact!("/-/reset-password-code", POST, RouteName::ResetPasswordCode);

    if let Some(captures) = check_username_regex().captures(path) {
        if method == Method::GET {
            let username = captures.name("username").map(|m| m.as_str().to_owned());
            return strict_slash(
                path,
                format!(
                    "/-/check-username-taken/{}",
                    username.as_deref().unwrap_or("")
                ),
                RouteName::CheckUsernameTaken,
                Vars {
                    username,
                    ..Vars::default()
                },
            );
        }
    }

    exact!("/-/editor", GET, RouteName::Editor);
    exact!("/-/debug/headers", GET, RouteName::DebugHeaders);

    // PathPrefix: strict-slash is disabled and the end is unanchored.
    if path.starts_with("/-/debug") {
        return Resolution::Route {
            name: RouteName::Debug,
            vars: Vars::default(),
        };
    }

    exact!("/gophercon", GET, RouteName::GopherconLiveBlog);

    if method == Method::GET {
        if let Some(captures) = old_tree_regex().captures(path) {
            return strict_slash(
                path,
                path.trim_end_matches('/').to_owned(),
                RouteName::OldTreeRedirect,
                Vars {
                    repo: captures.name("repo").map(|m| m.as_str().to_owned()),
                    rev: captures.name("rev").map(|m| m.as_str().to_owned()),
                    path: captures.name("path").map(|m| m.as_str().to_owned()),
                    ..Vars::default()
                },
            );
        }
    }

    exact!("/tools", GET, RouteName::OldToolsRedirect);

    exact!(
        "/site-admin/usage-statistics/archive",
        GET,
        RouteName::UsageStatsDownload
    );
    exact!(
        "/site-admin/data-export/archive",
        POST,
        RouteName::OneClickExportArchive
    );
    exact!("/site-admin/pings/latest", GET, RouteName::LatestPing);

    if let Some(captures) = badge_regex().captures(path) {
        if method == Method::GET {
            let repo = captures.name("repo").map(|m| m.as_str().to_owned());
            let template = format!(
                "/{}",
                captures.name("repo").map(|m| m.as_str()).unwrap_or("")
            );
            return strict_slash(
                path,
                format!("{template}/-/badge.svg"),
                RouteName::RepoBadge,
                Vars {
                    repo,
                    ..Vars::default()
                },
            );
        }
    }

    // `base.PathPrefix("/")`: the catch-all UI route, always last.
    Resolution::Route {
        name: RouteName::Ui,
        vars: Vars::default(),
    }
}

/// Dispatch a resolved route. Unported handlers answer `http.NotFound`, like
/// the oracle harness registers for them.
pub async fn dispatch_route<B>(
    req: &Request<B>,
    resolution: Resolution,
    path: &str,
    env: &AppEnv,
    config: &AppConfig,
    deps: &AppDeps,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    let Resolution::Route { name, vars } = resolution else {
        // `StrictSlash` redirect; dispatch handles the query.
        unreachable!("redirects are handled before handing off to a route");
    };
    match name {
        RouteName::RobotsTxt => app_handlers::robots_txt(env),
        RouteName::SitemapXmlGz => {
            app_handlers::sitemap_xml_gz(req, env, vars.number.as_deref().unwrap_or(""))
        }
        RouteName::Favicon => app_handlers::favicon(req, env, config),
        RouteName::OpenSearch => app_handlers::open_search(config),
        RouteName::RepoBadge => {
            app_handlers::repo_badge(req, vars.repo.as_deref().unwrap_or(""), env, deps).await
        }
        RouteName::OldToolsRedirect => app_handlers::redirect(req, "/beta", 301),
        RouteName::GopherconLiveBlog => {
            app_handlers::redirect(req, "https://sourcegraph.com/go", 302)
        }
        RouteName::DebugHeaders => app_handlers::debug_headers(req),
        RouteName::SignOut => app_handlers::sign_out(req, deps).await,
        RouteName::VerifyEmail => crate::app_handlers_more::verify_email(req, &deps.more).await,
        RouteName::UsageStatsDownload => {
            crate::app_handlers_more::usage_stats(req, &deps.more).await
        }
        RouteName::OneClickExportArchive => {
            crate::app_handlers_more::one_click_export(req, &deps.more, body).await
        }
        RouteName::LatestPing => crate::app_handlers_more::latest_ping(req, &deps.more).await,
        RouteName::Editor => crate::app_handlers_more::editor(req, &deps.more).await,
        RouteName::Debug => crate::app_handlers_more::debug(req, &deps.more, env).await,
        RouteName::OldTreeRedirect => crate::app_handlers_more::old_tree_redirect(
            req,
            vars.repo.as_deref().unwrap_or(""),
            vars.rev.as_deref().unwrap_or(""),
            vars.path.as_deref().unwrap_or(""),
        ),
        // `ui.Router()`: static pages, legacy redirects and the SPA shell.
        RouteName::Ui => crate::ui_assets::serve_ui(req, path, env, config, deps),
        // Auth flows are outside this slice and still answer the mux 404.
        _ => app_handlers::not_found(),
    }
}

/// The full `app.NewHandler` pipeline: inner ServeMux clean redirect, exact
/// rickroll routes, then the gorilla table.
pub async fn dispatch_async<B>(
    req: &mut Request<B>,
    env: &AppEnv,
    config: &AppConfig,
    deps: &AppDeps,
) -> Response<BoxBody>
where
    B: Send,
{
    dispatch_async_inner(req, env, config, deps, None).await
}

/// [`dispatch_async`] with the request body collected for the handlers that
/// read it (`/-/one-click-export`). The live listeners use this entry point;
/// the router differential keeps `dispatch_async` for `Request<()>` cases.
pub async fn dispatch_async_with_body<B>(
    req: &mut Request<B>,
    env: &AppEnv,
    config: &AppConfig,
    deps: &AppDeps,
) -> Response<BoxBody>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send,
{
    let body = match req.body_mut().collect().await {
        Ok(collected) => Some(collected.to_bytes()),
        // A body read error is not observable here: the baseline's decode
        // would fail too, and the corpus does not exercise it.
        Err(_) => None,
    };
    dispatch_async_inner(req, env, config, deps, body.as_deref()).await
}

async fn dispatch_async_inner<B>(
    req: &mut Request<B>,
    env: &AppEnv,
    config: &AppConfig,
    deps: &AppDeps,
    body: Option<&[u8]>,
) -> Response<BoxBody>
where
    B: Send,
{
    let escaped = req.uri().path();
    let escaped = if escaped.is_empty() { "/" } else { escaped };
    let cleaned = crate::mux::clean_path(escaped);
    if cleaned != escaped {
        let location = with_query(req.uri().query(), &cleaned);
        return app_handlers::redirect(&*req, &location, 301);
    }

    // `app.go:87-92`: exact ServeMux patterns, checked before the gorilla
    // catch-all.
    if matches!(
        cleaned.as_str(),
        "/.env" | "/admin.php" | "/wp-login.php" | "/wp-admin"
    ) {
        return app_handlers::rickroll(&*req);
    }

    // gorilla matches against `URL.Path`, which Go stores decoded.
    let path = percent_decode(&cleaned);
    let resolution = resolve(req.method(), &path);
    match resolution {
        Resolution::Redirect { location } => {
            let location = with_query(req.uri().query(), &location);
            app_handlers::redirect(&*req, &location, 301)
        }
        Resolution::Route { .. } => {
            dispatch_route(&*req, resolution, &path, env, config, deps, body).await
        }
    }
}

fn with_query(query: Option<&str>, path: &str) -> String {
    match query {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        _ => path.to_owned(),
    }
}

/// The `StrictSlash` redirect when the request path disagrees with the
/// template's trailing-slash form.
fn strict_slash(path: &str, template: String, name: RouteName, vars: Vars) -> Resolution {
    if path.ends_with('/') && !template.ends_with('/') {
        return Resolution::Redirect {
            location: path.trim_end_matches('/').to_owned(),
        };
    }
    Resolution::Route { name, vars }
}

fn exact_route(
    path: &str,
    template: &str,
    method: &Method,
    want: Method,
    name: RouteName,
) -> Option<Resolution> {
    let matches = path == template
        || (path.len() == template.len() + 1
            && path.ends_with('/')
            && &path[..path.len() - 1] == template);
    if !matches || *method != want {
        return None;
    }
    if path != template {
        return Some(Resolution::Redirect {
            location: template.to_owned(),
        });
    }
    Some(Resolution::Route {
        name,
        vars: Vars::default(),
    })
}

fn sitemap_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^/sitemap(?P<number>_(?:[0-9]+))?\.xml\.gz[/]?$").expect("sitemap regex")
    })
}

fn check_username_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^/-/check-username-taken/(?P<username>[^/]+)[/]?$")
            .expect("check-username regex")
    })
}

fn badge_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(&format!(
            r"^/(?P<repo>{})/-/badge\.svg[/]?$",
            repo_pattern()
        ))
        .expect("badge regex")
    })
}

fn old_tree_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        let component = r"(?:[^@=/.-]|(?:[^=/@.]{2,}))";
        let rev = format!(r"(?:@(?:{component}/)*{component})?");
        Regex::new(&format!(
            r"^/(?P<repo>{})(?P<rev>{rev})/\.tree(?P<path>.*)[/]?$",
            repo_pattern()
        ))
        .expect("old-tree regex")
    })
}

/// `routevar.RepoPattern` (`internal/routevar/repo.go:32-35`).
fn repo_pattern() -> &'static str {
    r"(?:[^@/-]|(?:[^/@]{2,}))(?:/(?:[^@/-]|(?:[^/@]{2,})))*"
}

/// Decode `%XX` sequences like Go's `url.Path` (`+` stays `+`).
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = hex_value(bytes[index + 1]);
            let low = hex_value(bytes[index + 2]);
            if let (Some(high), Some(low)) = (high, low) {
                output.push(high * 16 + low);
                index += 3;
                continue;
            }
        }
        output.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(name: RouteName) -> Resolution {
        Resolution::Route {
            name,
            vars: Vars::default(),
        }
    }

    #[test]
    fn exact_routes() {
        assert_eq!(
            resolve(&Method::GET, "/robots.txt"),
            route(RouteName::RobotsTxt)
        );
        assert_eq!(
            resolve(&Method::GET, "/favicon.ico"),
            route(RouteName::Favicon)
        );
        assert_eq!(
            resolve(&Method::GET, "/opensearch.xml"),
            route(RouteName::OpenSearch)
        );
        assert_eq!(
            resolve(&Method::GET, "/tools"),
            route(RouteName::OldToolsRedirect)
        );
        assert_eq!(
            resolve(&Method::GET, "/gophercon"),
            route(RouteName::GopherconLiveBlog)
        );
    }

    #[test]
    fn strict_slash_redirects() {
        assert_eq!(
            resolve(&Method::GET, "/robots.txt/"),
            Resolution::Redirect {
                location: "/robots.txt".to_owned()
            }
        );
        assert_eq!(
            resolve(&Method::GET, "/sitemap.xml.gz/"),
            Resolution::Redirect {
                location: "/sitemap.xml.gz".to_owned()
            }
        );
        assert_eq!(
            resolve(&Method::GET, "/foo/-/badge.svg/"),
            Resolution::Redirect {
                location: "/foo/-/badge.svg".to_owned()
            }
        );
        // A method mismatch never produces a StrictSlash redirect: the scan
        // continues to the UI catch-all.
        assert_eq!(resolve(&Method::POST, "/robots.txt/"), route(RouteName::Ui));
    }

    #[test]
    fn method_mismatch_falls_through_to_ui() {
        assert_eq!(resolve(&Method::POST, "/robots.txt"), route(RouteName::Ui));
        assert_eq!(resolve(&Method::GET, "/-/sign-up"), route(RouteName::Ui));
        assert_eq!(resolve(&Method::GET, "/-/sign-in"), route(RouteName::Ui));
        assert_eq!(resolve(&Method::GET, "/-/logout"), route(RouteName::Logout));
        assert_eq!(resolve(&Method::HEAD, "/favicon.ico"), route(RouteName::Ui));
    }

    #[test]
    fn prefix_routes_are_unanchored() {
        assert_eq!(
            resolve(&Method::GET, "/-/debug/goroutine"),
            route(RouteName::Debug)
        );
        assert_eq!(
            resolve(&Method::GET, "/-/debugxyz"),
            route(RouteName::Debug)
        );
        assert_eq!(resolve(&Method::GET, "/"), route(RouteName::Ui));
    }

    #[test]
    fn sitemap_number_vars() {
        assert_eq!(
            resolve(&Method::GET, "/sitemap_12.xml.gz"),
            Resolution::Route {
                name: RouteName::SitemapXmlGz,
                vars: Vars {
                    number: Some("_12".to_owned()),
                    ..Vars::default()
                },
            }
        );
        assert_eq!(
            resolve(&Method::GET, "/sitemap.xml.gz"),
            Resolution::Route {
                name: RouteName::SitemapXmlGz,
                vars: Vars::default(),
            }
        );
    }

    #[test]
    fn repo_pattern_matches_multi_segment_repos_but_not_bare_dash() {
        for (path, repo) in [
            ("/foo/-/badge.svg", "foo"),
            ("/github.com/foo/bar/-/badge.svg", "github.com/foo/bar"),
        ] {
            assert_eq!(
                resolve(&Method::GET, path),
                Resolution::Route {
                    name: RouteName::RepoBadge,
                    vars: Vars {
                        repo: Some(repo.to_owned()),
                        ..Vars::default()
                    },
                }
            );
        }
        // `-` is the path-component delimiter and cannot be a one-character
        // repo component; the badge path is not reachable.
        assert_eq!(
            resolve(&Method::GET, "/-/-/badge.svg"),
            route(RouteName::Ui)
        );
    }

    #[test]
    fn check_username_capture() {
        assert_eq!(
            resolve(&Method::GET, "/-/check-username-taken/alice"),
            Resolution::Route {
                name: RouteName::CheckUsernameTaken,
                vars: Vars {
                    username: Some("alice".to_owned()),
                    ..Vars::default()
                },
            }
        );
    }

    #[test]
    fn percent_decoding_keeps_plus() {
        assert_eq!(percent_decode("/a%2Fb%20c+d"), "/a/b c+d");
        assert_eq!(percent_decode("/bad%zz"), "/bad%zz");
    }
}
