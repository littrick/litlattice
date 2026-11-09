#![forbid(unsafe_code)]

//! Port of `cmd/frontend/internal/cli/middleware/goimportpath.go`.
//!
//! `?go-get=1` requests answer with a `go-import` meta tag page for the
//! `sourcegraph/<repo>` and `sqs/<repo>` vanity import paths; every other
//! go-get path is a 404. All other requests pass through untouched.

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

use crate::tenant::http_error;

/// `SourcegraphComGoGetHandler`: `None` means "not a go-get request, continue".
pub fn response<B>(req: &Request<B>, external_url: &str) -> Option<Response<BoxBody>> {
    if query_value(req.uri().query().unwrap_or(""), "go-get").as_deref() != Some("1") {
        return None;
    }

    let path = percent_decode(req.uri().path(), false);
    if !path.starts_with('/') {
        return Some(http_error(
            StatusCode::BAD_REQUEST,
            format!("req.URL.Path doesn't have a leading /: {path:?}"),
        ));
    }

    // Handle "go get sourcegraph.com/{sourcegraph,sqs}/*".
    let elements: Vec<&str> = path[1..].split('/').collect();
    if elements.len() >= 2 && (elements[0] == "sourcegraph" || elements[0] == "sqs") {
        let host = external_url_host(external_url);
        let user = elements[0];
        let repo = elements[1];
        let import_prefix = join_path(&[&host, user, repo]);
        let repo_root = format!("https://github.com/{user}/{repo}");
        let body = format!(
            "<html><head><meta name=\"go-import\" content=\"{} git {}\"></head><body></body></html>",
            html_escape(&import_prefix),
            html_escape(&repo_root),
        );
        return Some(html_response(body));
    }

    Some(http_error(StatusCode::NOT_FOUND, "no such repository"))
}

/// `url.Values.Get`: first value of the named query parameter, with form
/// decoding (`+` is a space, percent escapes are decoded, malformed escapes
/// are kept literally like `net/url`'s tolerant parse).
fn query_value(query: &str, name: &str) -> Option<String> {
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        };
        if percent_decode(key, true) == name {
            return Some(percent_decode(value, true));
        }
    }
    None
}

fn percent_decode(input: &str, plus_as_space: bool) -> String {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                match (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                    (Some(high), Some(low)) => {
                        decoded.push(high * 16 + low);
                        i += 3;
                    }
                    _ => {
                        decoded.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' if plus_as_space => {
                decoded.push(b' ');
                i += 1;
            }
            byte => {
                decoded.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// `conf.ExternalURLParsed().Host`: a URL without a scheme has no host.
fn external_url_host(external_url: &str) -> String {
    let rest = if let Some(rest) = external_url.strip_prefix("//") {
        rest
    } else if let Some(index) = external_url.find("://") {
        &external_url[index + 3..]
    } else {
        return String::new();
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let authority = match authority.rfind('@') {
        Some(index) => &authority[index + 1..],
        None => authority,
    };
    authority.to_owned()
}

/// `path.Join`: elements are already split, so filtering empty parts matches
/// the baseline result for the URL shapes this handler sees.
fn join_path(parts: &[&str]) -> String {
    parts
        .iter()
        .filter(|part| !part.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("/")
}

/// `html/template`'s quoted-attribute escaping table (`html.go:53-68`).
fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\0' => escaped.push('\u{fffd}'),
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

fn html_response(body: String) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    *response.status_mut() = StatusCode::OK;
    // net/http sniffs the HTML body; set the same value explicitly because
    // hyper does not sniff.
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;
    use http_body_util::BodyExt;

    fn request(target: &str) -> Request<()> {
        Request::builder().uri(target).body(()).expect("request")
    }

    async fn body_of(response: Response<BoxBody>) -> String {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    #[test]
    fn only_go_get_one_is_handled() {
        assert!(response(&request("/sourcegraph/a"), "https://x").is_none());
        assert!(response(&request("/sourcegraph/a?go-get=0"), "https://x").is_none());
        assert!(response(&request("/sourcegraph/a?go-get=1&go-get=2"), "https://x").is_some());
        assert!(response(&request("/sourcegraph/a?go-get=2&go-get=1"), "https://x").is_none());
    }

    #[tokio::test]
    async fn vanity_paths_render_the_meta_tag() {
        let response = response(
            &request("/sourcegraph/foo?go-get=1"),
            "https://sourcegraph.com/",
        )
        .expect("handled");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            body_of(response).await,
            "<html><head><meta name=\"go-import\" content=\"sourcegraph.com/sourcegraph/foo git https://github.com/sourcegraph/foo\"></head><body></body></html>"
        );
    }

    #[tokio::test]
    async fn empty_external_url_and_extra_elements() {
        let response = response(&request("/sqs/bar/extra?go-get=1"), "").expect("handled");
        assert_eq!(
            body_of(response).await,
            "<html><head><meta name=\"go-import\" content=\"sqs/bar git https://github.com/sqs/bar\"></head><body></body></html>"
        );
    }

    #[tokio::test]
    async fn other_paths_are_not_found() {
        let other = response(&request("/other/repo?go-get=1"), "https://x").expect("handled");
        assert_eq!(other.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            other.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(body_of(other).await, "no such repository\n");

        let single_element =
            response(&request("/sourcegraph?go-get=1"), "https://x").expect("handled");
        assert_eq!(single_element.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn template_escaping_matches_html_template() {
        assert_eq!(
            html_escape("a+b&c\"d'e<f>g"),
            "a&#43;b&amp;c&#34;d&#39;e&lt;f&gt;g"
        );
    }

    #[test]
    fn external_url_host_matches_go_url_parse() {
        assert_eq!(
            external_url_host("https://sourcegraph.com/"),
            "sourcegraph.com"
        );
        assert_eq!(
            external_url_host("https://sourcegraph.com:3443"),
            "sourcegraph.com:3443"
        );
        assert_eq!(external_url_host("https://user:pass@host/x"), "host");
        assert_eq!(external_url_host("//host/path"), "host");
        assert_eq!(external_url_host("sourcegraph.com"), "");
        assert_eq!(external_url_host(""), "");
    }
}
