#![forbid(unsafe_code)]

//! The response-header half of `internal/instrumentation.HTTPMiddleware`
//! (`internal/instrumentation/http.go:55-93`) and
//! `internal/trace.HTTPMiddleware` (`internal/trace/httptrace.go:110-131`).
//!
//! The baseline runs both middlewares between the tenant middleware and
//! ForbidAll. otelhttp starts a span for every request (the frontend always
//! calls `tracer.Init`), and the wrappers write:
//!
//! - `X-Trace`: the 32-hex trace ID;
//! - `X-Trace-Span`: the 16-hex span ID;
//! - `X-Trace-URL`: `trace.URL(traceID)`, rendered from
//!   `observability.tracing.urlTemplate` + `externalURL` (an empty value is
//!   still written).
//!
//! This port does not ship OTel: IDs are random v4 UUID bytes and nothing is
//! exported (docs/overviews/m9_8ed §1.4). The URL renderer only supports the
//! documented `.TraceID`/`.ExternalURL` field actions and `{{-`/`-}}`
//! trimming.

use http::Response;
use sg_config::SiteConfiguration;
use tonic::body::BoxBody;
use uuid::Uuid;

/// `X-Trace` (set by `instrumentation.HTTPMiddleware`).
pub const TRACE_HEADER: &str = "x-trace";
/// `X-Trace-Span` (set by `instrumentation.HTTPMiddleware`).
pub const TRACE_SPAN_HEADER: &str = "x-trace-span";
/// `X-Trace-URL` (set by `trace.HTTPMiddleware`).
pub const TRACE_URL_HEADER: &str = "x-trace-url";

/// The pair of OTEL span identifiers the baseline attaches to a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceIds {
    /// `TraceID.String()`: 16 random bytes, lowercase hex.
    pub trace_id: String,
    /// `SpanID.String()`: 8 random bytes, lowercase hex.
    pub span_id: String,
}

impl TraceIds {
    /// Generate identifiers with the same shape as an OTEL SDK span context
    /// (`span.IsValid()` is true, so the headers are always written).
    pub fn generate() -> Self {
        let trace_id = Uuid::new_v4().simple().to_string();
        let span_id = Uuid::new_v4().simple().to_string()[..16].to_owned();
        Self { trace_id, span_id }
    }
}

/// `observability.tracing.urlTemplate`, or `None` when tracing is absent,
/// `urlTemplate` is not a string, or it is empty. An empty template makes
/// `setupTraceURL`'s renderer return `""`.
pub fn url_template(site: &SiteConfiguration) -> Option<String> {
    let template = site
        .raw()
        .get("observability.tracing")?
        .get("urlTemplate")?
        .as_str()?;
    if template.is_empty() {
        None
    } else {
        Some(template.to_owned())
    }
}

/// `trace.URL`: render the configured template with the trace ID and
/// `externalURL`; parse/render failures yield `""` (the baseline caches a nil
/// template and returns `""`).
pub fn render_url(template: Option<&str>, trace_id: &str, external_url: &str) -> String {
    let Some(template) = template else {
        return String::new();
    };
    render_template(template, trace_id, external_url).unwrap_or_default()
}

/// Go `text/template`'s field actions for the two documented map keys:
/// `{{ .TraceID }}`, `{{.ExternalURL}}`, with optional `{{-`/`-}}` trimming.
/// An unknown field renders `<no value>` (Go map lookup semantics); anything
/// that is not a single field action is unsupported and yields `None`.
fn render_template(template: &str, trace_id: &str, external_url: &str) -> Option<String> {
    let mut output = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find("}}")?;
        let mut action = &after[..end];

        if let Some(stripped) = action.strip_prefix('-') {
            action = stripped;
            let trimmed = output.trim_end();
            output.truncate(trimmed.len());
        }
        let trim_right = action.ends_with('-');
        if trim_right {
            action = &action[..action.len() - 1];
        }

        match action.trim() {
            ".TraceID" => output.push_str(trace_id),
            ".ExternalURL" => output.push_str(external_url),
            field if field.starts_with('.') && !field.contains(char::is_whitespace) => {
                // `{{ .Foo }}` on a `map[string]string` prints `<no value>`.
                output.push_str("<no value>");
            }
            _ => return None,
        }

        rest = &after[end + 2..];
        if trim_right {
            rest = rest.trim_start();
        }
    }
    output.push_str(rest);
    Some(output)
}

/// Attach the three headers to a finished response. Called after the tenant
/// middleware, so ipAllowlist/tenant rejections never reach it.
pub fn apply(
    response: &mut Response<BoxBody>,
    ids: &TraceIds,
    template: Option<&str>,
    external_url: &str,
) {
    let url = render_url(template, &ids.trace_id, external_url);
    set(response, TRACE_HEADER, &ids.trace_id);
    set(response, TRACE_SPAN_HEADER, &ids.span_id);
    set(response, TRACE_URL_HEADER, &url);
}

fn set(response: &mut Response<BoxBody>, name: &'static str, value: &str) {
    let name = http::HeaderName::from_static(name);
    // Go writes header values byte-for-byte; hyper validates them. Invalid
    // values (e.g. a template producing control characters) degrade to an
    // empty value here.
    let value =
        http::HeaderValue::from_str(value).unwrap_or_else(|_| http::HeaderValue::from_static(""));
    response.headers_mut().insert(name, value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    #[test]
    fn generated_ids_have_the_otel_shape() {
        let ids = TraceIds::generate();
        assert_eq!(ids.trace_id.len(), 32);
        assert_eq!(ids.span_id.len(), 16);
        assert!(ids
            .trace_id
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()));
        assert!(ids
            .span_id
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()));
        assert_ne!(TraceIds::generate().trace_id, ids.trace_id);
    }

    #[test]
    fn url_template_reads_the_flat_site_config_key() {
        assert_eq!(url_template(&site("{}")), None);
        assert_eq!(url_template(&site(r#"{"observability.tracing":{}}"#)), None);
        assert_eq!(
            url_template(&site(r#"{"observability.tracing":{"urlTemplate":""}}"#)),
            None
        );
        assert_eq!(
            url_template(&site(
                r#"{"observability.tracing":{"urlTemplate":"https://t/{{ .TraceID }}"}}"#
            )),
            Some("https://t/{{ .TraceID }}".to_owned())
        );
    }

    #[test]
    fn render_url_matches_go_text_template() {
        let trace = "abc123";
        let external = "https://ext";
        assert_eq!(render_url(None, trace, external), "");
        assert_eq!(render_url(Some(""), trace, external), "");
        assert_eq!(render_url(Some("static"), trace, external), "static");
        assert_eq!(
            render_url(
                Some("https://traces.example/{{ .TraceID }}"),
                trace,
                external
            ),
            "https://traces.example/abc123"
        );
        assert_eq!(
            render_url(Some("{{.ExternalURL}}/trace/{{.TraceID}}"), trace, external),
            "https://ext/trace/abc123"
        );
        assert_eq!(
            render_url(Some("pre{{- .TraceID -}}post"), trace, external),
            "preabc123post"
        );
        assert_eq!(
            render_url(Some("{{ .TraceID }}x{{ .ExternalURL }}"), trace, external),
            "abc123xhttps://ext"
        );
        assert_eq!(
            render_url(Some("{{ .Foo }}"), trace, external),
            "<no value>"
        );
        assert_eq!(render_url(Some("{{ .TraceID"), trace, external), "");
    }

    #[test]
    fn apply_writes_all_three_headers_including_empty_url() {
        let ids = TraceIds {
            trace_id: "0123456789abcdef0123456789abcdef".to_owned(),
            span_id: "0123456789abcdef".to_owned(),
        };
        let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(
            bytes::Bytes::new(),
        )));
        apply(&mut response, &ids, None, "https://ext");
        assert_eq!(
            response
                .headers()
                .get(TRACE_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            ids.trace_id
        );
        assert_eq!(
            response
                .headers()
                .get(TRACE_SPAN_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            ids.span_id
        );
        assert_eq!(
            response
                .headers()
                .get(TRACE_URL_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            ""
        );
    }
}
