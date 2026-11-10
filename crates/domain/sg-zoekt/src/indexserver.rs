#![forbid(unsafe_code)]

//! Zoekt indexserver HTTP debug endpoints
//! (`internal/search/zoekt/reindex.go:19-105`).
//!
//! The baseline talks to a zoekt-webserver over `httpcli.InternalClient`:
//! [`Reindex`] POSTs `repo=<id>` to `/indexserver/debug/reindex`, and
//! [`GetIndexserverHost`] GETs `/indexserver/debug/host`. This port keeps the
//! exact request construction, status handling and error text, but injects the
//! HTTP client and the indexer map:
//!
//! - [`HttpClient`] replaces `httpcli.InternalClient`. No real network client
//!   lives in this crate; callers provide one.
//! - [`IndexerMap`] replaces `search.Indexers().Map.Get`. [`Indexers`]
//!   implements it without changing that module's public API.
//! - [`go_quote`] reproduces Go's `%q` (`strconv.Quote`) for the unexpected
//!   status message `"%s: %q"`.

use url::Url;

use crate::indexers::Indexers;

/// An outgoing HTTP request, the shape [`HttpClient`] receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// `"GET"` or `"POST"` in this module.
    pub method: &'static str,
    pub url: Url,
    /// Header name/value pairs in insertion order (the baseline adds exactly
    /// one `Content-Type` header for `Reindex`).
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

/// An HTTP response, the shape [`HttpClient`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The numeric status code.
    pub status: u16,
    /// The full status line text, i.e. Go's `resp.Status`
    /// (`"500 Internal Server Error"`). Used verbatim in the unexpected
    /// status error.
    pub status_text: String,
    /// The response body, or the error encountered while reading it. The
    /// baseline reads the body lazily, so a failure surfaces only on the
    /// paths that read it.
    pub body: Result<Vec<u8>, String>,
}

impl Response {
    /// Build a response with the standard reason phrase for `status`.
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            status_text: status_line(status),
            body: Ok(body.into()),
        }
    }

    /// Build a response whose body read fails with `message`.
    pub fn with_body_error(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            status_text: status_line(status),
            body: Err(message.into()),
        }
    }
}

/// `resp.Status` for a numeric code, e.g. `202 Accepted`.
///
/// `net/http` emits `"<code> <StatusText>"`; this covers the codes the debug
/// endpoints can return in practice and falls back to the bare code.
pub fn status_line(status: u16) -> String {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        418 => "I'm a teapot",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => return format!("{status}"),
    };
    format!("{status} {reason}")
}

/// The injected HTTP client (`httpcli.InternalClient.Do`).
pub trait HttpClient {
    fn do_request(&self, req: Request) -> Result<Response, Error>;
}

/// The injected `search.Indexers().Map.Get`.
pub trait IndexerMap {
    fn get(&self, name: &str) -> Result<String, Error>;
}

impl IndexerMap for Indexers {
    fn get(&self, name: &str) -> Result<String, Error> {
        match self.map.as_ref() {
            Some(map) => map
                .get(name)
                .map_err(|error| Error::Indexer(error.to_string())),
            // The baseline would dereference a nil `*endpoint.Map` and panic;
            // returning an error is the only faithful non-panicking mapping.
            None => Err(Error::Indexer("indexers are not configured".to_owned())),
        }
    }
}

/// `reindex.go` errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// `search.Indexers().Map.Get` failed.
    Indexer(String),
    /// `httpcli.InternalClient.Do` failed.
    Http(String),
    /// Reading the response body failed (`io.ReadAll`).
    Body(String),
    /// A fixed message: the 502 text and `webserver responded with %d`.
    Status(String),
    /// `errors.Newf("%s: %q", resp.Status, string(b))`.
    UnexpectedStatus { status: String, body: Vec<u8> },
    /// `url.Parse` / `ResolveReference` failed.
    Url(String),
    /// `json.Unmarshal` failed.
    Json(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Indexer(message)
            | Error::Http(message)
            | Error::Body(message)
            | Error::Status(message)
            | Error::Url(message)
            | Error::Json(message) => f.write_str(message),
            Error::UnexpectedStatus { status, body } => {
                write!(f, "{}: {}", status, go_quote(body))
            }
        }
    }
}

impl std::error::Error for Error {}

/// `Host` (`reindex.go:58-61`): `{"hostname": "..."}`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Host {
    pub name: String,
}

impl<'de> serde::Deserialize<'de> for Host {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // `encoding/json` leaves a non-pointer field at its zero value for a
        // missing key *and* for a JSON `null` (including a top-level `null`).
        #[derive(serde::Deserialize)]
        struct RawHost {
            #[serde(default, deserialize_with = "deserialize_hostname")]
            hostname: String,
        }

        fn deserialize_hostname<'de, D>(deserializer: D) -> Result<String, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
        }

        let raw = Option::<RawHost>::deserialize(deserializer)?;
        Ok(raw
            .map(|raw| Host { name: raw.hostname })
            .unwrap_or_default())
    }
}

/// `resolveIndexserver`: the Zoekt webserver hosting the index of `name`.
///
/// `http://` is prepended on a best-effort basis (case-sensitive prefix check,
/// exactly like `strings.HasPrefix`); `url.Parse` errors propagate.
pub fn resolve_indexserver(name: &str, indexers: &impl IndexerMap) -> Result<Url, Error> {
    let endpoint = indexers.get(name)?;

    let endpoint = if endpoint.starts_with("http://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    };

    Url::parse(&endpoint).map_err(|error| Error::Url(error.to_string()))
}

/// `Reindex`: force the indexserver to reindex the repo immediately.
pub fn reindex(
    http: &impl HttpClient,
    indexers: &impl IndexerMap,
    name: &str,
    id: u32,
) -> Result<(), Error> {
    let base = resolve_indexserver(name, indexers)?;
    let url = base
        .join("/indexserver/debug/reindex")
        .map_err(|error| Error::Url(error.to_string()))?;

    // `url.Values.Encode()` sorts keys and form-escapes; with the single
    // decimal `repo` key this is exactly `repo=<id>`.
    let body = format!("repo={id}").into_bytes();
    let request = Request {
        method: "POST",
        url,
        headers: vec![(
            "Content-Type".to_owned(),
            "application/x-www-form-urlencoded".to_owned(),
        )],
        body: Some(body),
    };

    let response = http.do_request(request)?;
    match response.status {
        202 => Ok(()),
        502 => Err(Error::Status(
            "Invalid response from Zoekt indexserver. The most likely cause is a broken socket connection."
                .to_owned(),
        )),
        _ => {
            let body = response.body.map_err(Error::Body)?;
            Err(Error::UnexpectedStatus {
                status: response.status_text,
                body,
            })
        }
    }
}

/// `GetIndexserverHost`: GET `/indexserver/debug/host` and decode the host.
pub fn get_indexserver_host(
    http: &impl HttpClient,
    indexers: &impl IndexerMap,
    name: &str,
) -> Result<Host, Error> {
    let base = resolve_indexserver(name, indexers)?;
    let url = base
        .join("/indexserver/debug/host")
        .map_err(|error| Error::Url(error.to_string()))?;

    let request = Request {
        method: "GET",
        url,
        headers: Vec::new(),
        body: None,
    };

    let response = http.do_request(request)?;
    if response.status != 200 {
        return Err(Error::Status(format!(
            "webserver responded with {}",
            response.status
        )));
    }

    let body = response.body.map_err(Error::Body)?;
    serde_json::from_slice(&body).map_err(|error| Error::Json(error.to_string()))
}

/// Go's `%q` for a string (`strconv.Quote`), operating on raw bytes.
///
/// Invalid UTF-8 bytes are emitted as `\xNN` one by one, matching
/// `strconv.Quote`'s `utf8.DecodeRuneInString` fallback. Printability of
/// non-ASCII runes approximates `unicode.IsPrint` (letters, marks, numbers,
/// punctuation, symbols and ASCII space), explicitly excluding control,
/// non-ASCII whitespace and common format/private-use code points.
pub fn go_quote(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('"');

    let mut rest = input;
    while !rest.is_empty() {
        if rest[0] < 0x80 {
            let byte = rest[0];
            rest = &rest[1..];
            match byte {
                b'"' => out.push_str("\\\""),
                b'\\' => out.push_str("\\\\"),
                0x07 => out.push_str("\\a"),
                0x08 => out.push_str("\\b"),
                0x0c => out.push_str("\\f"),
                b'\n' => out.push_str("\\n"),
                b'\r' => out.push_str("\\r"),
                b'\t' => out.push_str("\\t"),
                0x0b => out.push_str("\\v"),
                0x20..=0x7e => out.push(byte as char),
                _ => out.push_str(&format!("\\x{byte:02x}")),
            }
            continue;
        }

        match std::str::from_utf8(rest) {
            Ok(valid) => {
                for rune in valid.chars() {
                    quote_rune(rune, &mut out);
                }
                rest = &[];
            }
            Err(error) => {
                let valid_len = error.valid_up_to();
                if valid_len > 0 {
                    let valid = std::str::from_utf8(&rest[..valid_len])
                        .expect("from_utf8 reported this prefix as valid");
                    for rune in valid.chars() {
                        quote_rune(rune, &mut out);
                    }
                }
                // Go consumes one byte per invalid rune.
                let invalid_len = error.error_len().unwrap_or(rest.len() - valid_len);
                for byte in &rest[valid_len..valid_len + invalid_len] {
                    out.push_str(&format!("\\x{byte:02x}"));
                }
                rest = &rest[valid_len + invalid_len..];
            }
        }
    }

    out.push('"');
    out
}

fn quote_rune(rune: char, out: &mut String) {
    match rune {
        '"' => out.push_str("\\\""),
        '\\' => out.push_str("\\\\"),
        '\u{07}' => out.push_str("\\a"),
        '\u{08}' => out.push_str("\\b"),
        '\u{0c}' => out.push_str("\\f"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        '\u{0b}' => out.push_str("\\v"),
        _ if go_is_print(rune) => out.push(rune),
        _ => {
            let code = rune as u32;
            if code < 0x10000 {
                out.push_str(&format!("\\u{code:04x}"));
            } else {
                out.push_str(&format!("\\U{code:08x}"));
            }
        }
    }
}

/// Approximation of `unicode.IsPrint` for non-ASCII runes.
fn go_is_print(rune: char) -> bool {
    if rune == ' ' {
        return true;
    }
    if rune.is_control() || rune.is_whitespace() {
        return false;
    }
    let code = rune as u32;
    !matches!(
        code,
        0x00AD            // soft hyphen (Cf)
        | 0x200B..=0x200F // zero-width space..RLM (Cf)
        | 0x2028..=0x202E // line/paragraph separators (Zl/Zp) and bidi controls (Cf)
        | 0x2060..=0x2064 // word joiner and invisible operators (Cf)
        | 0xFEFF          // BOM / zero-width no-break space (Cf)
        | 0xE000..=0xF8FF // private use area (Co)
        | 0xF0000..=0x10FFFF // supplementary private use area (Co)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_matches_go_escapes() {
        assert_eq!(go_quote(b""), "\"\"");
        assert_eq!(go_quote(b"plain"), "\"plain\"");
        assert_eq!(go_quote(b"a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(go_quote(b"line\nnext\tend\r"), "\"line\\nnext\\tend\\r\"");
        assert_eq!(go_quote(b"\x07\x08\x0c\x0b"), "\"\\a\\b\\f\\v\"");
        assert_eq!(go_quote(b"\x00\x01\x1f\x7f"), "\"\\x00\\x01\\x1f\\x7f\"");
        assert_eq!(go_quote("é世".as_bytes()), "\"é世\"");
        assert_eq!(go_quote("\u{a0}".as_bytes()), "\"\\u00a0\"");
        assert_eq!(go_quote("\u{2028}".as_bytes()), "\"\\u2028\"");
        assert_eq!(go_quote("\u{1f600}".as_bytes()), "\"😀\"");
        assert_eq!(go_quote(&[0xff, b'a']), "\"\\xffa\"");
    }

    #[test]
    fn status_line_uses_standard_reason() {
        assert_eq!(status_line(202), "202 Accepted");
        assert_eq!(status_line(500), "500 Internal Server Error");
        assert_eq!(status_line(502), "502 Bad Gateway");
    }

    #[test]
    fn host_decodes_like_encoding_json() {
        let host: Host = serde_json::from_slice(br#"{"hostname":"h1"}"#).unwrap();
        assert_eq!(host.name, "h1");
        let host: Host = serde_json::from_slice(b"{}").unwrap();
        assert_eq!(host.name, "");
        let host: Host = serde_json::from_slice(br#"{"hostname":null}"#).unwrap();
        assert_eq!(host.name, "");
        let host: Host = serde_json::from_slice(b"null").unwrap();
        assert_eq!(host.name, "");
        assert!(serde_json::from_slice::<Host>(br#"{"hostname":5}"#).is_err());
        assert!(serde_json::from_slice::<Host>(br#"{"hostname":"a""#).is_err());
    }

    struct FakeMap {
        endpoint: Result<String, Error>,
    }

    impl IndexerMap for FakeMap {
        fn get(&self, _name: &str) -> Result<String, Error> {
            self.endpoint.clone()
        }
    }

    #[test]
    fn resolve_prefixes_only_when_needed() {
        let map = FakeMap {
            endpoint: Ok("indexer.internal:6070".to_owned()),
        };
        assert_eq!(
            resolve_indexserver("repo", &map).unwrap().as_str(),
            "http://indexer.internal:6070/"
        );

        let map = FakeMap {
            endpoint: Ok("http://indexer.internal:6070".to_owned()),
        };
        assert_eq!(
            resolve_indexserver("repo", &map).unwrap().as_str(),
            "http://indexer.internal:6070/"
        );
    }

    #[test]
    fn resolve_propagates_map_and_parse_errors() {
        let map = FakeMap {
            endpoint: Err(Error::Indexer("missing".to_owned())),
        };
        assert_eq!(
            resolve_indexserver("repo", &map).unwrap_err(),
            Error::Indexer("missing".to_owned())
        );

        let map = FakeMap {
            endpoint: Ok("host:notaport".to_owned()),
        };
        assert!(matches!(
            resolve_indexserver("repo", &map),
            Err(Error::Url(_))
        ));
    }

    #[test]
    fn unexpected_status_message_matches_baseline() {
        let error = Error::UnexpectedStatus {
            status: status_line(500),
            body: br#"boom "quoted""#.to_vec(),
        };
        assert_eq!(
            error.to_string(),
            "500 Internal Server Error: \"boom \\\"quoted\\\"\""
        );
    }
}
