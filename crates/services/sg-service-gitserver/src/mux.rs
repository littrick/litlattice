//! `net/http` `ServeMux` behavior the baseline relies on.
//!
//! Baseline `NewHTTPHandler` registers `/ping` and the `/git/` subtree on a
//! `ServeMux`. Before dispatching, the mux rewrites the path: it cleans it
//! (redirecting 301 when cleaning changed it), it redirects a subtree root
//! without its trailing slash (`/git` → `/git/`), and `http.StripPrefix`
//! hands the handler the *decoded* path. These helpers reproduce that
//! pipeline (Go 1.22 `net/http/server.go`).

use bytes::Bytes;
use http::{header, HeaderValue, Method, Request, Response, StatusCode};
use hyper::body::Incoming;

use crate::repo::clean_path_bytes;

/// Go `cleanPath`: `path.Clean` with a leading slash, preserving a trailing
/// slash. Operates on the escaped path, like the mux does.
pub(crate) fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return "/".to_owned();
    }
    let rooted = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let mut cleaned = String::from_utf8(clean_path_bytes(rooted.as_bytes()))
        .expect("cleaning an ASCII path yields ASCII");
    if rooted.ends_with('/') && cleaned != "/" {
        // `path.Clean` drops the trailing slash except for root.
        if rooted.len() == cleaned.len() + 1 && rooted.starts_with(&cleaned) {
            cleaned = rooted.clone();
        } else {
            cleaned.push('/');
        }
    }
    cleaned
}

/// Go `url.PathEscape`/`URL.EscapedPath` for `encodePath`: alphanumerics,
/// `-_.~` and the reserved `$&+,/:;=@` stay literal; everything else,
/// including `%`, is escaped byte-wise with uppercase hex.
pub(crate) fn escape_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for &byte in path.as_bytes() {
        let literal = matches!(
            byte,
            b'a'..=b'z'
                | b'A'..=b'Z'
                | b'0'..=b'9'
                | b'-'
                | b'_'
                | b'.'
                | b'~'
                | b'$'
                | b'&'
                | b'+'
                | b','
                | b'/'
                | b':'
                | b';'
                | b'='
                | b'@'
        );
        if literal {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Go `URL.Path`: percent-decode every valid `%XX` escape (`+` stays a
/// plus). The mux rejects malformed escapes before reaching a handler; a
/// residual escape is left alone and invalid UTF-8 is replaced, matching the
/// lossy decoding used elsewhere in this crate.
pub(crate) fn decode_path(path: &str) -> String {
    percent_encoding::percent_decode(path.as_bytes())
        .decode_utf8_lossy()
        .into_owned()
}

/// Go `htmlEscape` for the redirect body.
fn html_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
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

/// Go `hexEscapeNonASCII`: non-ASCII bytes become `%XX` in header values.
fn hex_escape_non_ascii(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for &byte in text.as_bytes() {
        if byte < 0x80 {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Absolute path used as a redirect target, with the raw query appended when
/// present (`url.URL{Path, RawQuery}.String()`).
fn location(path: &str, query: Option<&str>) -> String {
    match query {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        _ => path.to_owned(),
    }
}

/// Go `http.Redirect` with `301 Moved Permanently`: `Location`, an HTML
/// `Content-Type` for GET/HEAD, and a short HTML body for GET.
pub(crate) fn redirect(req: &Request<Incoming>, target: &str) -> Response<tonic::body::BoxBody> {
    let is_get = req.method() == Method::GET;
    let is_head = req.method() == Method::HEAD;
    let body = if is_get {
        Bytes::from(format!(
            "<a href=\"{}\">Moved Permanently</a>.\n\n",
            html_escape(target)
        ))
    } else {
        Bytes::new()
    };
    let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(body)));
    *response.status_mut() = StatusCode::MOVED_PERMANENTLY;
    let location = hex_escape_non_ascii(target);
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&location).expect("redirect target is a valid header value"),
    );
    if is_get || is_head {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
    }
    response
}

/// Redirect target for the mux's cleaned-path rewrite: the cleaned path is
/// re-escaped as a `url.URL.Path`, so an escape that survived cleaning is
/// escaped again (`%20` → `%2520`), exactly like the baseline.
pub(crate) fn cleaned_location(cleaned: &str, query: Option<&str>) -> String {
    location(&escape_path(cleaned), query)
}

/// Redirect target for the trailing-slash rewrite of a subtree root.
pub(crate) fn slash_location(path: &str, query: Option<&str>) -> String {
    location(&escape_path(path), query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleans_like_go() {
        assert_eq!(clean_path(""), "/");
        assert_eq!(clean_path("/git"), "/git");
        assert_eq!(clean_path("/git/"), "/git/");
        assert_eq!(clean_path("/git//x"), "/git/x");
        assert_eq!(clean_path("/git/./x"), "/git/x");
        assert_eq!(clean_path("/git/../ping"), "/ping");
        assert_eq!(clean_path("/git//"), "/git/");
        assert_eq!(clean_path("git/x"), "/git/x");
        // Escaped dot segments are not cleaned: `%2e` is not a literal `.`.
        assert_eq!(clean_path("/git/%2e%2e/ping"), "/git/%2e%2e/ping");
    }

    #[test]
    fn escapes_paths_like_go() {
        assert_eq!(
            escape_path("/git/test/git-refs-fixture/info/refs"),
            "/git/test/git-refs-fixture/info/refs"
        );
        assert_eq!(escape_path("/git/a%20b"), "/git/a%2520b");
        assert_eq!(escape_path("/a b"), "/a%20b");
        assert_eq!(escape_path("/a?b"), "/a%3Fb");
        assert_eq!(escape_path("/a!b'c"), "/a%21b%27c");
        assert_eq!(escape_path("/x@y:z;w=1+2$3&4,5"), "/x@y:z;w=1+2$3&4,5");
        assert_eq!(escape_path("/é"), "/%C3%A9");
    }

    #[test]
    fn decodes_paths_like_go() {
        assert_eq!(
            decode_path("/git/test/git-refs-fixture"),
            "/git/test/git-refs-fixture"
        );
        assert_eq!(
            decode_path("/git/test%2Fgit-refs-fixture"),
            "/git/test/git-refs-fixture"
        );
        assert_eq!(decode_path("/git/a%20b"), "/git/a b");
        assert_eq!(decode_path("/git/a+b"), "/git/a+b");
        assert_eq!(decode_path("/git/a%zzb"), "/git/a%zzb");
    }

    #[test]
    fn escapes_html_and_headers() {
        assert_eq!(html_escape("/a?b=1&c=2"), "/a?b=1&amp;c=2");
        assert_eq!(html_escape("<\"'>"), "&lt;&#34;&#39;&gt;");
        assert_eq!(hex_escape_non_ascii("/git/é"), "/git/%C3%A9");
        assert_eq!(hex_escape_non_ascii("/git/x"), "/git/x");
    }

    #[test]
    fn builds_locations() {
        assert_eq!(location("/git/", None), "/git/");
        assert_eq!(location("/git/", Some("")), "/git/");
        assert_eq!(
            location("/git/", Some("service=git-upload-pack")),
            "/git/?service=git-upload-pack"
        );
        assert_eq!(
            cleaned_location("/git/a%20b", Some("x=1")),
            "/git/a%2520b?x=1"
        );
        assert_eq!(slash_location("/git/", None), "/git/");
    }
}
