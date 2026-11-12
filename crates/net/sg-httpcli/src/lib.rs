#![forbid(unsafe_code)]

//! `internal/httpcli`: the outbound HTTP request logging middleware.
//!
//! Ports `internal/httpcli/redis_logger_middleware.go` (the `redisLoggerMiddleware`
//! plus the read helpers) against the same Redis structure the Go frontend
//! writes: a single FIFO list at `v2:outbound-requests` holding one JSON
//! object per entry (`types.OutboundRequestLogItem`). No new key prefixes or
//! fields are introduced; the list is the `sg-rcache` [`FifoList`] the rest of
//! the port already uses.
//!
//! The Go middleware wraps an `httpcli.Doer`; the transport is abstracted by
//! [`Doer`] so tests can inject a fake response without any network access.
//! Field names, order, `encoding/json` HTML escaping, RFC3339Nano timestamps,
//! the `2006-01-02T15_04_05.999999999` key format, the size caps and the
//! no-TTL semantics all match the baseline byte for byte.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sg_rcache::FifoList;
use sg_redis::KeyValue;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

pub const CRATE_NAME: &str = "sg-httpcli";

/// `outboundRequestsRedisFIFOList`'s key (`redis_logger_middleware.go:25-31`).
/// The package-level `NewFIFOListDynamic` prepends `dataVersion` once, so the
/// Redis key is `v2:outbound-requests`.
pub const OUTBOUND_REQUESTS_LIST_KEY: &str = "outbound-requests";

/// `sourcegraphPrefix` (`redis_logger_middleware.go:33`).
pub const SOURCEGRAPH_PREFIX: &str = "github.com/sourcegraph/sourcegraph/";

// ---------------------------------------------------------------------------
// Log item
// ---------------------------------------------------------------------------

/// `types.OutboundRequestLogItem` (`internal/types/types.go:2028-2041`).
///
/// Declaration order is the Go struct order and therefore the JSON field
/// order produced by `encoding/json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboundRequestLogItem {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(
        rename = "startedAt",
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub started_at: OffsetDateTime,
    #[serde(rename = "method")]
    pub method: String,
    #[serde(rename = "url")]
    pub url: String,
    #[serde(rename = "requestHeaders")]
    pub request_headers: BTreeMap<String, Vec<String>>,
    #[serde(rename = "requestBody")]
    pub request_body: String,
    #[serde(rename = "statusCode")]
    pub status_code: i32,
    // `nil` in Go encodes as JSON `null`; the redaction path always produces a
    // non-nil (possibly empty) map.
    #[serde(rename = "responseHeaders", default)]
    pub response_headers: Option<BTreeMap<String, Vec<String>>>,
    #[serde(rename = "duration")]
    pub duration: f64,
    #[serde(rename = "errorMessage")]
    pub error_message: String,
    #[serde(rename = "creationStackFrame")]
    pub creation_stack_frame: String,
    #[serde(rename = "callStackFrame")]
    pub call_stack_frame: String,
}

/// Serialize exactly like `encoding/json.Marshal` for this struct:
/// `serde_json` for the shape, then Go's default HTML escaping pass.
impl OutboundRequestLogItem {
    pub fn to_go_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let bytes = serde_json::to_vec(self)?;
        Ok(escape_go_html(&bytes))
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// `outboundRequestLogLimit` / `redactOutboundRequestHeaders` plus the
/// `deploy.IsDev(deploy.Type())` input to
/// `shouldRedactSensitiveHeaders` (`redis_logger_middleware.go:43`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundRequestLogSettings {
    /// `siteConfig.OutboundRequestLogLimit`; `<= 0` disables logging.
    pub limit: i32,
    /// `siteConfig.RedactOutboundRequestHeaders` (nil defaults to `true`).
    pub redact_sensitive_headers: bool,
    /// `deploy.IsDev(deploy.Type())`.
    pub is_dev: bool,
}

impl Default for OutboundRequestLogSettings {
    fn default() -> Self {
        // `deploy.Type()` defaults to Kubernetes (non-dev), so redaction is on
        // by default.
        Self {
            limit: 0,
            redact_sensitive_headers: false,
            is_dev: false,
        }
    }
}

impl OutboundRequestLogSettings {
    /// `!deploy.IsDev(deploy.Type()) || redactOutboundRequestHeaders()`.
    pub fn should_redact(&self) -> bool {
        !self.is_dev || self.redact_sensitive_headers
    }
}

// ---------------------------------------------------------------------------
// Injectable transport
// ---------------------------------------------------------------------------

/// The request shape the middleware needs: the pieces of an `*http.Request`
/// the log item reads (`Method`, `URL`, `Header`, and a re-readable body).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestSpec {
    pub method: String,
    pub url: String,
    /// Header name/value pairs as sent. Names are canonicalized like
    /// `net/http.Header.Set` before being logged.
    pub headers: Vec<(String, String)>,
    /// `Some` mirrors `http.Request.GetBody` (the body can be re-read for the
    /// log); `None` mirrors `GetBody == nil`, where Go logs an empty body.
    pub body: Option<Vec<u8>>,
}

/// The response subset the middleware reads (`resp.Header`,
/// `resp.StatusCode`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseSpec {
    pub status_code: i32,
    pub headers: Vec<(String, String)>,
}

/// The outcome of a [`Doer`] call. Go's `(*http.Response, error)`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DoResult {
    pub response: Option<ResponseSpec>,
    pub error: Option<String>,
}

/// `httpcli.Doer`: the injected transport.
#[async_trait]
pub trait Doer: Send + Sync {
    async fn do_request(&self, req: &RequestSpec) -> DoResult;
}

impl RequestSpec {
    /// The logged request headers: `net/http` canonical names, values in
    /// order, one map key per canonical name.
    pub fn canonical_headers(&self) -> BTreeMap<String, Vec<String>> {
        canonical_header_map(&self.headers)
    }
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

/// `redisLoggerMiddleware` (`redis_logger_middleware.go:35-132`) around an
/// injected [`Doer`].
///
/// Times one [`Doer::do_request`] call, then (when `settings.limit > 0`)
/// builds and stores the log item in `fifo`. Returns the doer result
/// unchanged, exactly like the Go middleware.
pub async fn log_request(
    doer: &dyn Doer,
    settings: &OutboundRequestLogSettings,
    fifo: &FifoList,
    req: &RequestSpec,
) -> DoResult {
    let start = OffsetDateTime::now_utc();
    let result = doer.do_request(req).await;
    let duration = (OffsetDateTime::now_utc() - start).as_seconds_f64();

    // Feature is turned off, do not log.
    if settings.limit <= 0 {
        return result;
    }

    let item = build_log_item(settings, req, &result, start, duration);
    if let Ok(json) = item.to_go_json() {
        // The baseline logs and swallows the insert error (the insert happens
        // in a goroutine).
        let _ = fifo.insert(json).await;
    }
    result
}

/// Build a `types.OutboundRequestLogItem` from a request/result pair.
///
/// Split out from [`log_request`] so tests can pin the time and duration.
pub fn build_log_item(
    settings: &OutboundRequestLogSettings,
    req: &RequestSpec,
    result: &DoResult,
    started_at: OffsetDateTime,
    duration: f64,
) -> OutboundRequestLogItem {
    let mut request_headers = req.canonical_headers();
    let mut response_headers = result
        .response
        .as_ref()
        .map(|response| canonical_header_map(&response.headers));

    if settings.should_redact() {
        request_headers = redact_sensitive_headers(request_headers);
        response_headers = Some(redact_sensitive_headers(
            response_headers.unwrap_or_default(),
        ));
    }

    let request_body = req
        .body
        .as_deref()
        .map(|body| String::from_utf8_lossy(body).into_owned())
        .unwrap_or_default();
    let status_code = result
        .response
        .as_ref()
        .map(|response| response.status_code)
        .unwrap_or(0);
    let error_message = result.error.clone().unwrap_or_default();
    let started_at = started_at.to_offset(UtcOffset::UTC);

    OutboundRequestLogItem {
        id: go_id(started_at),
        started_at,
        method: req.method.clone(),
        url: req.url.clone(),
        request_headers,
        request_body,
        status_code,
        response_headers,
        duration,
        error_message,
        // Rust has no Go runtime frames; see the overview errata.
        creation_stack_frame: String::new(),
        call_stack_frame: String::new(),
    }
}

/// `NewFIFOListDynamic(redispool.Cache, "outbound-requests", ...)`: the Redis
/// key is `v2:outbound-requests`.
pub fn outbound_requests_fifo(kv: KeyValue, max_size: i32) -> FifoList {
    FifoList::new(kv, OUTBOUND_REQUESTS_LIST_KEY.to_owned(), i64::from(max_size))
}

// ---------------------------------------------------------------------------
// Reader helpers
// ---------------------------------------------------------------------------

/// `GetOutboundRequestLogItems` (`redis_logger_middleware.go:134-157`).
pub async fn get_outbound_request_log_items(
    fifo: &FifoList,
    after: &str,
    limit: i32,
) -> Result<Vec<OutboundRequestLogItem>, String> {
    if limit <= 0 {
        return Ok(Vec::new());
    }

    let mut items = get_outbound_request_log_items_matching(fifo, |item| {
        after.is_empty() || item.id.as_str() > after
    })
    .await?;

    if items.len() > limit as usize {
        items.truncate(limit as usize);
    }
    Ok(items)
}

/// `GetOutboundRequestLogItem` (`redis_logger_middleware.go:159-172`).
pub async fn get_outbound_request_log_item(
    fifo: &FifoList,
    key: &str,
) -> Result<OutboundRequestLogItem, String> {
    let items =
        get_outbound_request_log_items_matching(fifo, |item| item.id == key).await?;
    items
        .into_iter()
        .next()
        .ok_or_else(|| "item not found".to_owned())
}

/// `getOutboundRequestLogItems` (`redis_logger_middleware.go:174-201`):
/// fetch everything, filter, sort by ID ascending.
async fn get_outbound_request_log_items_matching(
    fifo: &FifoList,
    pred: impl Fn(&OutboundRequestLogItem) -> bool,
) -> Result<Vec<OutboundRequestLogItem>, String> {
    let raw_items = fifo
        .all()
        .await
        .map_err(|error| format!("list all log items: {error}"))?;

    let mut items = Vec::with_capacity(raw_items.len());
    for raw_item in raw_items {
        let item: OutboundRequestLogItem =
            serde_json::from_slice(&raw_item).map_err(|error| error.to_string())?;
        if pred(&item) {
            items.push(item);
        }
    }

    items.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(items)
}

// ---------------------------------------------------------------------------
// Header redaction
// ---------------------------------------------------------------------------

/// `IsRiskyHeader` (`client.go:1003-1007`).
pub fn is_risky_header(name: &str, values: &[String]) -> bool {
    is_risky_header_name(name) || contains_risky_header_value(values)
}

/// `isRiskyHeaderName` (`client.go:1009-1018`).
fn is_risky_header_name(name: &str) -> bool {
    let lower = name.to_lowercase();
    ["auth", "cookie", "token"]
        .iter()
        .any(|risky| lower.contains(risky))
}

/// `containsRiskyHeaderValue` (`client.go:1021-1031`).
fn contains_risky_header_value(values: &[String]) -> bool {
    values.iter().any(|value| {
        let lower = value.to_lowercase();
        ["bearer", "ghp_", "glpat-"]
            .iter()
            .any(|risky| lower.contains(risky))
    })
}

/// `redactSensitiveHeaders` (`redis_logger_middleware.go:204-214`).
pub fn redact_sensitive_headers(
    headers: BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    headers
        .into_iter()
        .map(|(name, values)| {
            if is_risky_header(&name, &values) {
                (name, vec!["REDACTED".to_owned()])
            } else {
                (name, values)
            }
        })
        .collect()
}

/// Aggregate header pairs into a canonical-name map.
pub fn canonical_header_map(headers: &[(String, String)]) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in headers {
        map.entry(canonical_header_key(name))
            .or_default()
            .push(value.clone());
    }
    map
}

/// `textproto.CanonicalMIMEHeaderKey`: upper-case the first letter and every
/// letter after a dash, lower-case the rest; return unchanged when the name
/// contains a byte that is not a valid header field byte.
pub fn canonical_header_key(name: &str) -> String {
    if name.bytes().any(|byte| !valid_header_field_byte(byte)) {
        return name.to_owned();
    }
    let mut out = Vec::with_capacity(name.len());
    let mut upper = true;
    for byte in name.bytes() {
        out.push(if upper {
            byte.to_ascii_uppercase()
        } else {
            byte.to_ascii_lowercase()
        });
        upper = byte == b'-';
    }
    // Valid header field bytes are ASCII, so this cannot fail.
    String::from_utf8(out).unwrap_or_else(|_| name.to_owned())
}

/// Go's `validHeaderFieldByte` (the token table).
fn valid_header_field_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
    ) || byte.is_ascii_alphanumeric()
}

// ---------------------------------------------------------------------------
// Stack frame formatting
// ---------------------------------------------------------------------------

/// `formatStackFrames` (`redis_logger_middleware.go:216-227`).
pub fn format_stack_frames(frames: &[(&str, &str, i32)]) -> String {
    let mut out = String::new();
    for (function, file, line) in frames {
        out.push_str(&format_stack_frame(function, file, *line));
        out.push('\n');
    }
    out.trim_end_matches('\n').to_owned()
}

/// `formatStackFrame` (`redis_logger_middleware.go:229-243`).
pub fn format_stack_frame(function: &str, file: &str, line: i32) -> String {
    let tree_and_func: Vec<&str> = function.split('/').collect();
    let package_and_func = tree_and_func.last().copied().unwrap_or("");
    let dot_pieces: Vec<&str> = package_and_func.split('.').collect();
    let package_name = dot_pieces.first().copied().unwrap_or("");
    let func_name = dot_pieces[1..].join(".");

    let mut tree = tree_and_func[..tree_and_func.len().saturating_sub(1)].join("/");
    tree.push('/');
    tree.push_str(package_name);
    let tree = tree.strip_prefix(SOURCEGRAPH_PREFIX).unwrap_or(&tree);

    let base = file.rsplit('/').next().unwrap_or(file);
    let file_name = join_path(tree, base);
    let file_name = file_name
        .strip_prefix("/main/")
        .unwrap_or(&file_name)
        .to_owned();

    format!("{file_name}:{line} (Function: {func_name})")
}

/// `filepath.Join` for the two-segment `tree + base` case used above.
fn join_path(tree: &str, base: &str) -> String {
    if tree.is_empty() {
        return base.to_owned();
    }
    format!("{}/{}", tree.trim_end_matches('/'), base)
}

// ---------------------------------------------------------------------------
// JSON encoding helpers (Go `encoding/json` compatibility)
// ---------------------------------------------------------------------------

/// Go's `time.Time` JSON form (RFC3339Nano), in UTC: fractional seconds are
/// optional and trailing zeros are trimmed.
fn go_rfc3339_nano(time: OffsetDateTime) -> String {
    let time = time.to_offset(UtcOffset::UTC);
    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second()
    );
    let nanos = time.nanosecond();
    if nanos != 0 {
        let fraction = format!("{nanos:09}");
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// `time.Now().UTC().Format("2006-01-02T15_04_05.999999999")`: the key uses
/// underscores between the time components and trims trailing fractional
/// zeros.
fn go_id(time: OffsetDateTime) -> String {
    let time = time.to_offset(UtcOffset::UTC);
    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}_{:02}_{:02}",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second()
    );
    let nanos = time.nanosecond();
    if nanos != 0 {
        let fraction = format!("{nanos:09}");
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }
    out
}

fn serialize_go_time<S>(time: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&go_rfc3339_nano(*time))
}

fn deserialize_go_time<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    OffsetDateTime::parse(&value, &Rfc3339).map_err(serde::de::Error::custom)
}

/// `encoding/json` escapes `<`, `>`, `&`, U+2028 and U+2029 inside strings by
/// default (`SetEscapeHTML(true)`); `serde_json` does not. Rewrite string
/// literals in place so the bytes match Go's output.
fn escape_go_html(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < input.len() {
        let byte = input[index];
        if !in_string {
            out.push(byte);
            if byte == b'"' {
                in_string = true;
            }
            index += 1;
            continue;
        }
        if escaped {
            out.push(byte);
            escaped = false;
            index += 1;
            continue;
        }
        match byte {
            b'\\' => {
                out.push(byte);
                escaped = true;
                index += 1;
            }
            b'"' => {
                out.push(byte);
                in_string = false;
                index += 1;
            }
            b'<' => {
                out.extend_from_slice(b"\\u003c");
                index += 1;
            }
            b'>' => {
                out.extend_from_slice(b"\\u003e");
                index += 1;
            }
            b'&' => {
                out.extend_from_slice(b"\\u0026");
                index += 1;
            }
            0xE2 if input.get(index + 1) == Some(&0x80)
                && matches!(input.get(index + 2), Some(0xA8) | Some(0xA9)) =>
            {
                if input[index + 2] == 0xA8 {
                    out.extend_from_slice(b"\\u2028");
                } else {
                    out.extend_from_slice(b"\\u2029");
                }
                index += 3;
            }
            _ => {
                out.push(byte);
                index += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8, nano: u32) -> OffsetDateTime {
        let month = time::Month::try_from(month).expect("month");
        time::Date::from_calendar_date(year, month, day)
            .expect("date")
            .with_hms_nano(hour, minute, second, nano)
            .expect("time")
            .assume_utc()
    }

    #[test]
    fn format_stack_frame_matches_the_baseline() {
        assert_eq!(
            format_stack_frame(
                "github.com/sourcegraph/sourcegraph/cmd/frontend/graphqlbackend.(*requestTracer).TraceQuery",
                "/Users/x/github.com/sourcegraph/sourcegraph/cmd/frontend/graphqlbackend/graphqlbackend.go",
                51,
            ),
            "cmd/frontend/graphqlbackend/graphqlbackend.go:51 (Function: (*requestTracer).TraceQuery)"
        );
        assert_eq!(
            format_stack_frame(
                "third-party/library.f",
                "/Users/x/github.com/third-party/library/file.go",
                11,
            ),
            "third-party/library/file.go:11 (Function: f)"
        );
        assert_eq!(
            format_stack_frame("main.f", "/Users/x/file.go", 11),
            "file.go:11 (Function: f)"
        );
    }

    #[test]
    fn canonical_header_key_matches_net_http() {
        assert_eq!(canonical_header_key("cache-control"), "Cache-Control");
        assert_eq!(canonical_header_key("X-Test-Header"), "X-Test-Header");
        assert_eq!(canonical_header_key("x-test-header"), "X-Test-Header");
        assert_eq!(canonical_header_key("bad header"), "bad header");
        assert_eq!(canonical_header_key(""), "");
    }

    #[test]
    fn redaction_matches_the_baseline_cases() {
        // The Go test uses a `http.Header` literal, so the names are not
        // canonicalized; mirror that with a literal BTreeMap.
        let headers: BTreeMap<String, Vec<String>> = [
            ("Authorization", vec!["all values", "should be", "removed"]),
            (
                "Bearer",
                vec!["this should be kept as the risky value is only in the name"],
            ),
            ("GHP_XXXX", vec!["this should be kept"]),
            ("GLPAT-XXXX", vec!["this should also be kept"]),
            ("GitHub-PAT", vec!["this should be removed: ghp_XXXX"]),
            ("GitLab-PAT", vec!["this should be removed", "glpat-XXXX"]),
            ("Innocent-Header", vec!["the word bearer"]),
            ("Set-Cookie", vec!["this is verboten"]),
            ("Token", vec!["a token should be removed"]),
            ("X-Powered-By", vec!["PHP"]),
            (
                "X-Token",
                vec!["something that smells like a token should also be removed"],
            ),
        ]
        .into_iter()
        .map(|(name, values)| {
            (
                name.to_owned(),
                values.into_iter().map(str::to_owned).collect(),
            )
        })
        .collect();
        let redacted = redact_sensitive_headers(headers);
        for key in ["Bearer", "GHP_XXXX", "GLPAT-XXXX", "X-Powered-By"] {
            let expected: Vec<String> = match key {
                "Bearer" => vec!["this should be kept as the risky value is only in the name"],
                "GHP_XXXX" => vec!["this should be kept"],
                "GLPAT-XXXX" => vec!["this should also be kept"],
                _ => vec!["PHP"],
            }
            .into_iter()
            .map(str::to_owned)
            .collect();
            assert_eq!(redacted.get(key), Some(&expected), "key {key}");
        }
        for key in [
            "Authorization",
            "GitHub-PAT",
            "GitLab-PAT",
            "Innocent-Header",
            "Set-Cookie",
            "Token",
            "X-Token",
        ] {
            assert_eq!(
                redacted.get(key),
                Some(&vec!["REDACTED".to_owned()]),
                "key {key}"
            );
        }
    }

    #[test]
    fn go_json_time_trimming() {
        assert_eq!(go_rfc3339_nano(utc(2021, 1, 1, 0, 0, 0, 0)), "2021-01-01T00:00:00Z");
        assert_eq!(
            go_rfc3339_nano(utc(2021, 1, 1, 0, 0, 0, 123_000_000)),
            "2021-01-01T00:00:00.123Z"
        );
        assert_eq!(
            go_rfc3339_nano(utc(2021, 1, 1, 0, 0, 0, 123_456_789)),
            "2021-01-01T00:00:00.123456789Z"
        );
        assert_eq!(
            go_id(utc(2021, 1, 1, 0, 0, 0, 123_000_000)),
            "2021-01-01T00_00_00.123"
        );
    }

    #[test]
    fn go_json_escapes_html_like_encoding_json() {
        let item = OutboundRequestLogItem {
            id: "id".to_owned(),
            started_at: utc(2021, 1, 1, 0, 0, 0, 0),
            method: "GET".to_owned(),
            url: "http://x/?a=1&b=<2>".to_owned(),
            request_headers: BTreeMap::new(),
            request_body: "<script>&\"</script>".to_owned(),
            status_code: 200,
            response_headers: None,
            duration: 0.5,
            error_message: String::new(),
            creation_stack_frame: String::new(),
            call_stack_frame: String::new(),
        };
        let json = String::from_utf8(item.to_go_json().expect("json")).expect("utf8");
        // Go escapes `<`, `>` and `&` inside strings.
        assert!(json.contains("\\u0026"), "ampersand escape missing: {json}");
        assert!(json.contains("\\u003c"), "less-than escape missing: {json}");
        assert!(json.contains("\\u003e"), "greater-than escape missing: {json}");
        assert!(!json.contains("&"), "raw ampersand survived: {json}");
        // Go field order and null response headers.
        assert!(
            json.starts_with(
                r#"{"id":"id","startedAt":"2021-01-01T00:00:00Z","method":"GET","url":"#,
            ),
            "{json}"
        );
        assert!(json.contains(r#""responseHeaders":null"#), "{json}");
    }

    #[test]
    fn item_round_trips_through_go_json() {
        let item = OutboundRequestLogItem {
            id: "2021-01-01T00_00_00.123".to_owned(),
            started_at: utc(2021, 1, 1, 0, 0, 0, 123_000_000),
            method: "PATCH".to_owned(),
            url: "http://test.aa?a=2".to_owned(),
            request_headers: BTreeMap::from([(
                "Cache-Control".to_owned(),
                vec!["no-cache".to_owned()],
            )]),
            request_body: "graph".to_owned(),
            status_code: 403,
            response_headers: Some(BTreeMap::from([(
                "Content-Type".to_owned(),
                vec!["text/plain; charset=utf-8".to_owned()],
            )])),
            duration: 0.00125,
            error_message: String::new(),
            creation_stack_frame: String::new(),
            call_stack_frame: String::new(),
        };
        let encoded = item.to_go_json().expect("encode");
        let decoded: OutboundRequestLogItem =
            serde_json::from_slice(&encoded).expect("decode");
        assert_eq!(decoded, item);
    }
}
