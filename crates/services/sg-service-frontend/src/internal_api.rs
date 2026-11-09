#![forbid(unsafe_code)]

//! The internal listener's HTTP surface (`RegisterInternalServices`,
//! `cmd/frontend/internal/httpapi/httpapi.go:355-403`).
//!
//! The baseline routes on a gorilla mux router, so this module reproduces the
//! router's observable behavior before dispatching:
//!
//! - `path.Clean` canonicalization with a 301, keeping the trailing slash;
//! - routes matched in registration order on the decoded `URL.Path`
//!   (`/git/{RepoName:.*}/info/refs`, `/git/{RepoName:.*}/git-upload-pack`,
//!   `/lsif/upload`, `/scip/upload`, `/compute/stream`, `/ping`);
//! - a path match with the wrong method answers 405 with no body;
//! - everything else falls through to the custom `no route` 404.
//!
//! `/git/...` redirects (307) to the gitserver shard that owns the repository
//! (`gitServiceHandler.redirectToGitServer`); the `JsonMiddleware` wrapper
//! already set `Content-Type: application/json`, so the redirect carries that
//! header and Go's `http.Redirect` skips its HTML body.
//!
//! `/lsif/upload` and `/scip/upload` (POST) and `/compute/stream` (GET/POST)
//! resolve to `enterprise.DefaultServices`' handlers in this build:
//! `makeNotFoundHandler` answers 404 with the
//! `<name> is only available in enterprise` body (sniffed as `text/plain`),
//! and `/scip/upload` (HEAD) is the 200 `noopHandler`.

use bytes::Bytes;
use http::header::{self, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

use sg_config::LiveConfig;
use sg_git::addresses::GitserverAddresses;

use crate::mux;

/// `/ping` (`internal.go:61-68`).
const PING_PATH: &str = "/ping";
const GIT_PREFIX: &str = "/git/";
const INFO_REFS: &str = "/info/refs";
const UPLOAD_PACK: &str = "/git-upload-pack";
const LSIF_UPLOAD_PATH: &str = "/lsif/upload";
const SCIP_UPLOAD_PATH: &str = "/scip/upload";
const COMPUTE_STREAM_PATH: &str = "/compute/stream";

/// The route a request resolved to, before the method is checked.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Ping,
    InfoRefs { repo: String },
    UploadPack { repo: String },
    LsifUpload,
    ScipUpload,
    ComputeStream,
}

impl Route {
    fn allows(&self, method: &Method) -> bool {
        match self {
            Route::Ping | Route::InfoRefs { .. } => method == Method::GET,
            Route::UploadPack { .. } | Route::ComputeStream => {
                method == Method::GET || method == Method::POST
            }
            Route::LsifUpload => method == Method::POST,
            Route::ScipUpload => method == Method::POST || method == Method::HEAD,
        }
    }
}

/// `Router.ServeHTTP` + the registered handlers.
pub fn internal_response<B>(req: &Request<B>, live: &LiveConfig) -> Response<BoxBody> {
    let raw_path = req.uri().path();
    let decoded = decode_path(if raw_path.is_empty() { "/" } else { raw_path });

    // gorilla `Router.ServeHTTP`: clean the path before matching and redirect
    // when it was not canonical. `Location` is built like `url.URL.String`.
    let cleaned = mux::clean_path(&decoded);
    if cleaned != decoded {
        let location = match req.uri().query() {
            Some(query) => format!("{}?{query}", escape_path(&cleaned)),
            None => escape_path(&cleaned),
        };
        return located(StatusCode::MOVED_PERMANENTLY, location, None);
    }

    // `StrictSlash(true)`: the route regexp accepts one optional trailing
    // slash and redirects it away before the handler runs.
    let (route, matched_path) = match match_route(&decoded) {
        Some(route) => (route, decoded.as_str()),
        None => match decoded.strip_suffix('/').map(match_route) {
            Some(Some(route)) => (route, &decoded[..decoded.len() - 1]),
            _ => return no_route(),
        },
    };
    if !route.allows(req.method()) {
        // gorilla `methodNotAllowed`: status only, no body.
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }
    if matched_path.len() != decoded.len() {
        return strict_slash_redirect(req.method(), matched_path, req.uri().query());
    }

    match route {
        Route::Ping => text_response(StatusCode::OK, "text/plain; charset=utf-8", "pong", false),
        Route::InfoRefs { repo } => redirect_to_git_server(req, &repo, INFO_REFS, live),
        Route::UploadPack { repo } => redirect_to_git_server(req, &repo, UPLOAD_PACK, live),
        Route::LsifUpload => enterprise_not_found("code intel upload"),
        Route::ScipUpload if req.method() == Method::HEAD => empty_response(StatusCode::OK),
        Route::ScipUpload => enterprise_not_found("code intel upload"),
        Route::ComputeStream => enterprise_not_found("compute streaming endpoint"),
    }
}

/// `routeRegexpGroup.setMatch`'s strict-slash branch: `http.Redirect` to the
/// path without its trailing slash.
pub(crate) fn strict_slash_redirect(
    method: &Method,
    path: &str,
    query: Option<&str>,
) -> Response<BoxBody> {
    let mut location = escape_path(path);
    if let Some(query) = query {
        location.push('?');
        location.push_str(query);
    }
    http_redirect(method, StatusCode::MOVED_PERMANENTLY, location)
}

/// Go `http.Redirect`: sets `text/html` for GET/HEAD, writes the HTML body
/// only for GET, and leaves other methods with just `Location`. The route
/// handler is replaced, so no `JsonMiddleware` content type is involved.
pub(crate) fn http_redirect(
    method: &Method,
    status: StatusCode,
    location: String,
) -> Response<BoxBody> {
    let location = hex_escape_non_ascii(&location);
    let status_text = match status {
        StatusCode::MOVED_PERMANENTLY => "Moved Permanently",
        StatusCode::FOUND => "Found",
        StatusCode::SEE_OTHER => "See Other",
        StatusCode::TEMPORARY_REDIRECT => "Temporary Redirect",
        StatusCode::PERMANENT_REDIRECT => "Permanent Redirect",
        _ => "",
    };
    match *method {
        Method::GET => {
            let body = format!(
                "<a href=\"{}\">{status_text}</a>.\n\n",
                html_escape(&location)
            );
            located_with_body(status, location, Some("text/html; charset=utf-8"), body)
        }
        Method::HEAD => located_with_body(
            status,
            location,
            Some("text/html; charset=utf-8"),
            Bytes::new(),
        ),
        _ => located(status, location, None),
    }
}

/// `enterprise.makeNotFoundHandler`: raw 404 write, sniffed as `text/plain`.
pub(crate) fn enterprise_not_found(name: &str) -> Response<BoxBody> {
    text_response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        &format!("{name} is only available in enterprise"),
        false,
    )
}

/// Go `html.EscapeString`.
fn html_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Gorilla routes in registration order; the `.*` capture is greedy, so the
/// repository is everything between `/git/` and the literal suffix.
fn match_route(path: &str) -> Option<Route> {
    if path == PING_PATH {
        return Some(Route::Ping);
    }
    if path == LSIF_UPLOAD_PATH {
        return Some(Route::LsifUpload);
    }
    if path == SCIP_UPLOAD_PATH {
        return Some(Route::ScipUpload);
    }
    if path == COMPUTE_STREAM_PATH {
        return Some(Route::ComputeStream);
    }
    let rest = path.strip_prefix(GIT_PREFIX)?;
    if let Some(repo) = rest.strip_suffix(INFO_REFS) {
        return Some(Route::InfoRefs {
            repo: repo.to_owned(),
        });
    }
    if let Some(repo) = rest.strip_suffix(UPLOAD_PACK) {
        return Some(Route::UploadPack {
            repo: repo.to_owned(),
        });
    }
    None
}

/// `redirectToGitServer` (`internal.go:44-58`).
fn redirect_to_git_server<B>(
    req: &Request<B>,
    repo: &str,
    git_path: &str,
    live: &LiveConfig,
) -> Response<BoxBody> {
    let unified = live.current();
    let addresses = GitserverAddresses::from_unified(&unified);
    let Some(host) = addresses.addr_for_repo(repo) else {
        // The baseline panics (`% uint64(len(addrs))` with an empty shard
        // list); a drop-in server answers an error instead.
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain; charset=utf-8",
            "no gitserver configured\n",
            true,
        );
    };
    let mut location = format!(
        "http://{host}{}",
        escape_path(&go_path_join(repo, git_path))
    );
    if let Some(query) = req.uri().query() {
        location.push('?');
        location.push_str(query);
    }
    located(
        StatusCode::TEMPORARY_REDIRECT,
        hex_escape_non_ascii(&location),
        Some("application/json"),
    )
}

/// Go `path.Join("/git", repo, gitPath)`: join, then `path.Clean` (the
/// trailing slash is not preserved, unlike `path.Clean` alone).
fn go_path_join(repo: &str, git_path: &str) -> String {
    let cleaned = mux::clean_path(&format!("{GIT_PREFIX}{repo}{git_path}"));
    if cleaned.len() > 1 {
        cleaned.trim_end_matches('/').to_owned()
    } else {
        cleaned
    }
}

/// A 3xx response with `Location` and, when the baseline set one, a
/// `Content-Type`.
pub(crate) fn located(
    status: StatusCode,
    location: String,
    content_type: Option<&'static str>,
) -> Response<BoxBody> {
    located_with_body(status, location, content_type, Bytes::new())
}

/// `located`, plus the `http.Redirect` HTML body (only written when the
/// handler had not already set a `Content-Type`).
fn located_with_body(
    status: StatusCode,
    location: String,
    content_type: Option<&'static str>,
    body: impl Into<Bytes>,
) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(body.into())));
    *response.status_mut() = status;
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
    response
}

/// A status-only response (no body, no `Content-Type`).
pub(crate) fn empty_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = status;
    response
}

/// The custom `NotFoundHandler` (`httpapi.go:399-403`).
fn no_route() -> Response<BoxBody> {
    text_response(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        "no route\n",
        true,
    )
}

/// `http.Error`'s body + headers (`nosniff`) or a plain sniffed text response.
pub(crate) fn text_response(
    status: StatusCode,
    content_type: &str,
    body: &str,
    nosniff: bool,
) -> Response<BoxBody> {
    let mut response =
        crate::middleware::text_response(status, content_type, Bytes::from(body.to_owned()));
    if nosniff {
        response.headers_mut().insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
    }
    response
}

/// Go `URL.Path` decoding: `%XX` becomes a byte; invalid escapes are kept
/// literally (the Go server instead rejects them with 400).
pub(crate) fn decode_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
            {
                out.push(high << 4 | low);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

const HEX: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
];

/// Go `escape(s, encodePath)`: keeps unreserved characters and the reserved
/// set except `?`, escaping everything else as uppercase `%XX`.
pub(crate) fn escape_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for &byte in path.as_bytes() {
        if should_escape_path(byte) {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize]);
            out.push(HEX[(byte & 0xf) as usize]);
        } else {
            out.push(byte as char);
        }
    }
    out
}

fn should_escape_path(byte: u8) -> bool {
    match byte {
        b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' => false,
        b'-' | b'_' | b'.' | b'~' => false,
        // §2.2 reserved characters allowed in `encodePath` (all except `?`).
        b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@' => false,
        _ => true,
    }
}

/// `net/http`'s `hexEscapeNonASCII`.
pub(crate) fn hex_escape_non_ascii(input: &str) -> String {
    if input.is_ascii() {
        return input.to_owned();
    }
    let mut out = String::with_capacity(input.len());
    for &byte in input.as_bytes() {
        if byte >= 0x80 {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize].to_ascii_lowercase());
            out.push(HEX[(byte & 0xf) as usize].to_ascii_lowercase());
        } else {
            out.push(byte as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use sg_config::{ServiceConnections, SiteConfiguration, Unified};

    fn live() -> LiveConfig {
        let site = SiteConfiguration::parse(
            r#"{"experimentalFeatures":{"gitServerPinnedRepos":{"pinned.example.com/r":"gitserver-2:3178"}}}"#,
        )
        .expect("site");
        let service_connections = ServiceConnections {
            git_servers: vec![
                "gitserver-0:3178".to_owned(),
                "gitserver-1:3178".to_owned(),
                "gitserver-2:3178".to_owned(),
            ],
            ..ServiceConnections::default()
        };
        LiveConfig::new(Unified {
            site,
            service_connections,
        })
    }

    fn request(method: Method, target: &str) -> Request<()> {
        Request::builder()
            .method(method)
            .uri(target)
            .body(())
            .expect("request")
    }

    #[test]
    fn ping_writes_pong() {
        let response = internal_response(&request(Method::GET, "/ping"), &live());
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn git_paths_redirect_to_the_shard() {
        let response = internal_response(
            &request(
                Method::GET,
                "/git/github.com/foo/bar/info/refs?service=git-upload-pack",
            ),
            &live(),
        );
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        let location = response.headers().get(header::LOCATION).unwrap();
        let location = location.to_str().unwrap();
        assert!(location.starts_with("http://gitserver-"));
        assert!(location.ends_with("/git/github.com/foo/bar/info/refs?service=git-upload-pack"));
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[test]
    fn pinned_repositories_use_the_pinned_shard() {
        let response = internal_response(
            &request(Method::GET, "/git/pinned.example.com/r/git-upload-pack"),
            &live(),
        );
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "http://gitserver-2:3178/git/pinned.example.com/r/git-upload-pack"
        );
    }

    #[tokio::test]
    async fn wrong_methods_and_unknown_paths_are_rejected() {
        assert_eq!(
            internal_response(&request(Method::POST, "/ping"), &live()).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        let response = internal_response(&request(Method::GET, "/nope"), &live());
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"no route\n");
    }

    #[test]
    fn non_canonical_paths_redirect() {
        let response = internal_response(&request(Method::GET, "/git//x/info/refs?a=b"), &live());
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/git/x/info/refs?a=b"
        );
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
    }

    #[test]
    fn strict_slash_redirects_to_the_template() {
        let response = internal_response(&request(Method::GET, "/ping/"), &live());
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/ping");
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );

        let response = internal_response(&request(Method::GET, "/git/x/info/refs/?a=b"), &live());
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/git/x/info/refs?a=b"
        );

        // A method mismatch is decided before the strict-slash branch.
        assert_eq!(
            internal_response(&request(Method::POST, "/git/x/info/refs/"), &live()).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }

    #[tokio::test]
    async fn enterprise_routes_answer_like_the_default_services() {
        let lsif = internal_response(&request(Method::POST, "/lsif/upload"), &live());
        assert_eq!(lsif.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            lsif.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        let body = lsif.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            &body[..],
            b"code intel upload is only available in enterprise"
        );

        let head = internal_response(&request(Method::HEAD, "/scip/upload"), &live());
        assert_eq!(head.status(), StatusCode::OK);
        assert!(head.headers().get(header::CONTENT_TYPE).is_none());

        let compute = internal_response(&request(Method::GET, "/compute/stream"), &live());
        assert_eq!(compute.status(), StatusCode::NOT_FOUND);
        let body = compute.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            &body[..],
            b"compute streaming endpoint is only available in enterprise"
        );

        assert_eq!(
            internal_response(&request(Method::GET, "/lsif/upload"), &live()).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            internal_response(&request(Method::HEAD, "/compute/stream"), &live()).status(),
            StatusCode::METHOD_NOT_ALLOWED
        );

        // StrictSlash: `http.Redirect` only writes the HTML body for GET.
        let post = internal_response(&request(Method::POST, "/lsif/upload/"), &live());
        assert_eq!(post.status(), StatusCode::MOVED_PERMANENTLY);
        assert!(post.headers().get(header::CONTENT_TYPE).is_none());
        let get = internal_response(&request(Method::GET, "/compute/stream/"), &live());
        assert_eq!(
            get.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
    }

    #[test]
    fn escapes_paths_and_non_ascii_like_go() {
        assert_eq!(escape_path("/a b/ü"), "/a%20b/%C3%BC");
        assert_eq!(escape_path("/x?y"), "/x%3Fy");
        assert_eq!(hex_escape_non_ascii("aü"), "a%c3%bc");
        assert_eq!(decode_path("/git/a%2Fb/info/refs"), "/git/a/b/info/refs");
    }
}
