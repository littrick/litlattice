#![forbid(unsafe_code)]

//! `internal/trace/policy`, `internal/tracer.tracePolicySampler` and the
//! sampling/attribute half of `internal/instrumentation.HTTPMiddleware`.
//!
//! The baseline keeps a process-global `TracePolicy` string
//! (`internal/trace/policy/policy.go:27`) that `internal/tracer`'s config
//! watcher updates from `observability.tracing`. Requests carry a boolean
//! "should trace" value in their context, set by the instrumentation
//! middleware from `X-Sourcegraph-Should-Trace` (or `?trace=`) and propagated
//! to outbound requests by `policy.Transport`.
//!
//! This port keeps the same three-valued policy and the same decision rules,
//! but models the tracing SDK as an injectable seam: [`SpanExporter`] receives
//! finished spans and the default ([`init_tracer`] with `None`) records the
//! policy/type only. Nothing is exported over the network.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use http::HeaderMap;
use serde_json::Value;

use sg_config::SiteConfiguration;

/// `X-Sourcegraph-Should-Trace` (`internal/trace/policy/policy.go:70`).
pub const SHOULD_TRACE_HEADER: &str = "x-sourcegraph-should-trace";

/// `sampling.retain` (`internal/instrumentation/http.go:124`).
pub const SAMPLING_RETAIN_KEY: &str = "sampling.retain";

/// `trace.UserLatencyBuckets` (`internal/trace/buckets.go:7`).
pub const USER_LATENCY_BUCKETS: [f64; 7] = [0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0];

/// `policy.TracePolicy` (`internal/trace/policy/policy.go:13`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TracePolicy {
    /// `none`: tracing is off.
    None,
    /// `selective`: only requests opting in (header/query) are traced.
    Selective,
    /// `all`: every request is traced.
    All,
}

impl TracePolicy {
    /// The string stored in site configuration (`observability.tracing.sampling`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Selective => "selective",
            Self::All => "all",
        }
    }

    /// Parse one of the three supported values. `None` means "not a policy"
    /// (the caller falls back to `selective`, like `newConfWatcher`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "selective" => Some(Self::Selective),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    fn encode(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Selective => 1,
            Self::All => 2,
        }
    }

    fn decode(value: u8) -> Self {
        match value {
            1 => Self::Selective,
            2 => Self::All,
            _ => Self::None,
        }
    }
}

/// `trPolicy` (`internal/trace/policy/policy.go:27`): defaults to `none`.
static TRACE_POLICY: AtomicU8 = AtomicU8::new(0);

/// `policy.SetTracePolicy`.
pub fn set_trace_policy(policy: TracePolicy) {
    TRACE_POLICY.store(policy.encode(), Ordering::SeqCst);
}

/// `policy.GetTracePolicy`.
pub fn get_trace_policy() -> TracePolicy {
    TracePolicy::decode(TRACE_POLICY.load(Ordering::SeqCst))
}

/// `policy.ShouldTrace` for an explicit policy.
///
/// A context value (the `Option<bool>`) wins; when it is absent the baseline
/// falls back to "the global policy is `all`".
pub fn should_trace_for(policy: TracePolicy, context_value: Option<bool>) -> bool {
    match context_value {
        Some(value) => value,
        None => policy == TracePolicy::All,
    }
}

/// `policy.ShouldTrace` against the process-global policy.
pub fn should_trace(context_value: Option<bool>) -> bool {
    should_trace_for(get_trace_policy(), context_value)
}

/// `strconv.ParseBool` (`internal/trace/policy/policy.go:89,96`).
///
/// `None` is the Go parse error; both callers ignore it and treat the result
/// as `false`.
pub fn parse_go_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `policy.RequestWantsTracing` (`internal/trace/policy/policy.go:87`).
///
/// The header takes precedence over the query; an empty header falls through.
/// Go only parses `RawQuery` when it contains the literal `trace=`.
pub fn request_wants_tracing(header: Option<&str>, raw_query: &str) -> bool {
    if let Some(value) = header.filter(|value| !value.is_empty()) {
        return parse_go_bool(value).unwrap_or(false);
    }
    if raw_query.contains("trace=") {
        // `url.Values.Get` returns the first value for the key.
        for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
            if key == "trace" {
                return parse_go_bool(&value).unwrap_or(false);
            }
        }
    }
    false
}

/// `policy.Transport.RoundTrip`'s header value (`strconv.FormatBool`).
pub fn propagation_header(should_trace: bool) -> &'static str {
    if should_trace {
        "true"
    } else {
        "false"
    }
}

/// The header value the instrumentation middleware passes in, if present.
pub fn should_trace_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(SHOULD_TRACE_HEADER)
        .and_then(|value| value.to_str().ok())
}

/// `instrumentation.HTTPMiddleware`'s trace-policy switch
/// (`internal/instrumentation/http.go:82-92`).
pub fn instrumentation_should_trace(
    policy: TracePolicy,
    header: Option<&str>,
    raw_query: &str,
) -> bool {
    match policy {
        TracePolicy::Selective => request_wants_tracing(header, raw_query),
        TracePolicy::All => true,
        TracePolicy::None => false,
    }
}

/// The per-request trace decision attached by the external chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShouldTrace(pub bool);

/// Compute and read the request's should-trace value using the live policy.
pub fn request_should_trace(headers: &HeaderMap, raw_query: Option<&str>) -> bool {
    instrumentation_should_trace(
        get_trace_policy(),
        should_trace_header(headers),
        raw_query.unwrap_or(""),
    )
}

// ---------------------------------------------------------------------------
// Sampling (`internal/tracer/policy_sampler.go`)
// ---------------------------------------------------------------------------

/// `tracePolicySampler.ShouldSample`.
///
/// `all` samples, `none` drops, and the selective default samples when the
/// parent context carries `shouldTrace=true`. When the parent has no value,
/// `policy.ShouldTrace`'s `all` fallback is false for the selective branch
/// (the global policy is already selective), so the span is dropped.
pub fn should_sample(policy: TracePolicy, parent_should_trace: Option<bool>) -> bool {
    match policy {
        TracePolicy::All => true,
        TracePolicy::None => false,
        TracePolicy::Selective => parent_should_trace.unwrap_or(false),
    }
}

/// `samplingRetainTracer.Start`: only explicitly-traced spans get the
/// `sampling.retain=true` attribute.
pub fn sampling_retain(should_trace: bool) -> Option<Attribute> {
    should_trace.then(|| Attribute {
        key: SAMPLING_RETAIN_KEY.to_owned(),
        value: AttributeValue::String("true".to_owned()),
    })
}

// ---------------------------------------------------------------------------
// Attributes (`internal/trace/attributes.go`)
// ---------------------------------------------------------------------------

/// One OpenTelemetry attribute (`attribute.KeyValue`).
#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
}

/// The attribute value kinds used by the ported surfaces.
#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    String(String),
    Int64(i64),
    Bool(bool),
    StringSlice(Vec<String>),
}

impl AttributeValue {
    /// `attribute.Value.Type().String()`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "STRING",
            Self::Int64(_) => "INT64",
            Self::Bool(_) => "BOOL",
            Self::StringSlice(_) => "STRINGSLICE",
        }
    }
}

/// `trace.Scoped`: prefix every key with `"<scope>."`, preserving values and
/// order.
pub fn scoped(scope: &str, attributes: Vec<Attribute>) -> Vec<Attribute> {
    attributes
        .into_iter()
        .map(|attribute| Attribute {
            key: format!("{scope}.{}", attribute.key),
            value: attribute.value,
        })
        .collect()
}

/// `trace.Stringers`: call `ToString` on each value and build a `StringSlice`.
pub fn stringers<T: ToString>(key: &str, values: &[T]) -> Attribute {
    Attribute {
        key: key.to_owned(),
        value: AttributeValue::StringSlice(values.iter().map(T::to_string).collect()),
    }
}

/// `trace.defaultErrorRuneLimit`.
pub const DEFAULT_ERROR_RUNE_LIMIT: usize = 512;

/// `trace.truncateError`: keep the first and last `max_runes/2` runes and
/// splice `" ...truncated... "` between them when the text is too long.
pub fn truncate_error(error: Option<&str>, max_runes: usize) -> Option<String> {
    let error = error?;
    let runes: Vec<char> = error.chars().collect();
    if runes.len() > max_runes {
        let half = max_runes / 2;
        let prefix: String = runes[..half].iter().collect();
        let suffix: String = runes[runes.len() - half..].iter().collect();
        Some(format!("{prefix} ...truncated... {suffix}"))
    } else {
        Some(error.to_owned())
    }
}

/// `trace.Error`: `"<nil>"` for no error, otherwise the truncated message.
pub fn error_attribute(error: Option<&str>) -> Attribute {
    Attribute {
        key: "error".to_owned(),
        value: AttributeValue::String(
            truncate_error(error, DEFAULT_ERROR_RUNE_LIMIT).unwrap_or_else(|| "<nil>".to_owned()),
        ),
    }
}

// ---------------------------------------------------------------------------
// Request context (`internal/trace/httptrace.go`, `internal/trace/context.go`)
// ---------------------------------------------------------------------------

/// `trace.SourceType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SourceType {
    /// `browser`
    Browser,
    /// `other` (the baseline default).
    #[default]
    Other,
}

impl SourceType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Browser => "browser",
            Self::Other => "other",
        }
    }
}

/// The tracing-related context values `trace.CopyContext` copies.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestTraceContext {
    pub should_trace: Option<bool>,
    pub graphql_request_name: Option<String>,
    pub source: Option<SourceType>,
}

impl RequestTraceContext {
    /// `trace.GraphQLRequestName`: `"unknown"` when unset.
    pub fn graphql_request_name(&self) -> String {
        self.graphql_request_name
            .clone()
            .unwrap_or_else(|| "unknown".to_owned())
    }

    /// `trace.RequestSource`: `other` when unset.
    pub fn request_source(&self) -> SourceType {
        self.source.unwrap_or_default()
    }

    /// `trace.CopyContext` copies the span and `shouldTrace`; this port copies
    /// the request-level fields it models.
    pub fn copy(&self) -> Self {
        self.clone()
    }
}

/// `trace.WithGraphQLRequestName`'s default-free getter.
pub fn graphql_request_name(name: Option<&str>) -> String {
    name.unwrap_or("unknown").to_owned()
}

/// `trace.RequestSource`'s default-free getter.
pub fn request_source(source: Option<SourceType>) -> SourceType {
    source.unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Startup configuration (`internal/tracer/watch.go`)
// ---------------------------------------------------------------------------

/// `tracer.TracerType` (`internal/tracer/tracer.go:37`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TracerType {
    None,
    Jaeger,
    OpenTelemetry,
}

impl TracerType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Jaeger => "jaeger",
            Self::OpenTelemetry => "opentelemetry",
        }
    }
}

/// The subset of `observability.tracing` this slice reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TracingConfig {
    pub sampling: String,
    pub tracer_type: String,
    pub debug: bool,
}

/// Read `observability.tracing` from the raw site configuration. `None` when
/// the key is absent (or not an object), matching `tracingConfig == nil`.
pub fn tracing_config_from_site(site: &SiteConfiguration) -> Option<TracingConfig> {
    let tracing = site.raw().get("observability.tracing")?.as_object()?;
    Some(TracingConfig {
        sampling: tracing
            .get("sampling")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        tracer_type: tracing
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        debug: tracing
            .get("debug")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// `observability.captureSlowGraphQLRequestsLimit`; missing/non-integer is `0`.
pub fn capture_slow_graphql_requests_limit(site: &SiteConfiguration) -> i64 {
    site.raw()
        .get("observability.captureSlowGraphQLRequestsLimit")
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

/// `observability.logSlowGraphQLRequests`: the slow-request threshold in
/// milliseconds (`graphqlbackend.go:137`). Missing/non-integer is `0`
/// (capture disabled).
pub fn log_slow_graphql_requests(site: &SiteConfiguration) -> i64 {
    site.raw()
        .get("observability.logSlowGraphQLRequests")
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

/// The outcome of one `newConfWatcher` invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtelStartup {
    pub policy: TracePolicy,
    pub tracer_type: TracerType,
    pub debug: bool,
}

/// `newConfWatcher` (`internal/tracer/watch.go:42-90`): derive the trace
/// policy and tracer type from the current site configuration.
///
/// - An unsupported `sampling` defaults to `selective`.
/// - An unsupported `type` defaults to `opentelemetry` when the policy is not
///   `none`; otherwise the tracer stays disabled.
/// - Without a tracing config the previous policy, `none` type and `false`
///   debug are used.
pub fn plan_otel_startup(
    tracing: Option<&TracingConfig>,
    previous_policy: TracePolicy,
) -> OtelStartup {
    let mut policy = previous_policy;
    let mut tracer_type = TracerType::None;
    let mut debug = false;
    if let Some(tracing) = tracing {
        policy = TracePolicy::parse(&tracing.sampling).unwrap_or(TracePolicy::Selective);
        tracer_type = match tracing.tracer_type.as_str() {
            "jaeger" => TracerType::Jaeger,
            "opentelemetry" => TracerType::OpenTelemetry,
            _ if policy != TracePolicy::None => TracerType::OpenTelemetry,
            _ => TracerType::None,
        };
        debug = tracing.debug;
    }
    OtelStartup {
        policy,
        tracer_type,
        debug,
    }
}

/// Apply one configuration snapshot, mirroring the `conf.Watch` callback:
/// only a present `observability.tracing` changes the global policy.
pub fn apply_tracing_config(site: &SiteConfiguration) -> Option<OtelStartup> {
    let config = tracing_config_from_site(site)?;
    let startup = plan_otel_startup(Some(&config), get_trace_policy());
    set_trace_policy(startup.policy);
    Some(startup)
}

// ---------------------------------------------------------------------------
// OTel initialization seam
// ---------------------------------------------------------------------------

/// A finished span handed to an injected exporter. This intentionally carries
/// only the fields this slice can produce; spans are never sent over a
/// network.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanData {
    pub name: String,
    pub trace_id: String,
    pub span_id: String,
    pub attributes: Vec<Attribute>,
}

/// The exporter seam standing in for the OTel SDK's span processors. Tests
/// inject a recording exporter; production passes `None` (no export).
pub trait SpanExporter: Send + Sync {
    fn export(&self, span: &SpanData);
    fn shutdown(&self) {}
}

/// The process-level tracer state established by `tracer.Init`.
pub struct TracerRuntime {
    startup: OtelStartup,
    exporter: Option<Arc<dyn SpanExporter>>,
}

impl TracerRuntime {
    /// Apply the policy and remember the exporter. No exporter means no
    /// output, which is the production wiring for this port.
    pub fn init(startup: OtelStartup, exporter: Option<Arc<dyn SpanExporter>>) -> Self {
        set_trace_policy(startup.policy);
        Self { startup, exporter }
    }

    pub fn startup(&self) -> &OtelStartup {
        &self.startup
    }

    /// Whether the baseline would have built a tracer provider.
    pub fn enabled(&self) -> bool {
        self.startup.tracer_type != TracerType::None
    }

    /// Hand a span to the exporter if one was injected. Returns whether it was
    /// exported.
    pub fn export(&self, span: &SpanData) -> bool {
        match (&self.exporter, self.enabled()) {
            (Some(exporter), true) => {
                exporter.export(span);
                true
            }
            _ => false,
        }
    }

    pub fn shutdown(&self) {
        if let Some(exporter) = &self.exporter {
            exporter.shutdown();
        }
    }
}

/// `tracer.Init`'s observable core: set the policy and return a runtime bound
/// to the (test-injected) exporter.
pub fn init_tracer(
    startup: &OtelStartup,
    exporter: Option<Arc<dyn SpanExporter>>,
) -> TracerRuntime {
    TracerRuntime::init(startup.clone(), exporter)
}

// ---------------------------------------------------------------------------
// OTLP exporter adapter (`internal/tracer/oteldefaults/exporters/otlp.go`,
// `internal/otlpenv/otlpenv.go`)
// ---------------------------------------------------------------------------

/// `defaultGRPCCollectorEndpoint` (`internal/otlpenv/otlpenv.go:24`).
pub const DEFAULT_OTLP_ENDPOINT: &str = "http://127.0.0.1:4317";

/// `otlpenv.Protocol`: only the values `NewOTLPTraceExporter` accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OtlpProtocol {
    /// `grpc`
    Grpc,
    /// `http/json`
    HttpJson,
}

impl OtlpProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::HttpJson => "http/json",
        }
    }

    /// `NewOTLPTraceExporter`'s protocol switch: `http/proto` (and anything
    /// else) is unsupported and surfaces as `unexpected protocol %q`.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "grpc" => Some(Self::Grpc),
            "http/json" => Some(Self::HttpJson),
            _ => None,
        }
    }
}

/// The resolved OTLP exporter settings (`internal/otlpenv`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtlpExporterConfig {
    /// The endpoint with any `http(s)://` scheme stripped.
    pub endpoint: String,
    pub protocol: OtlpProtocol,
    /// `otlpenv.IsInsecure`: the endpoint used `http://`.
    pub insecure: bool,
}

/// `otlpenv.GetEndpoint`: the first set env wins; unset uses the default.
pub fn otlp_endpoint(env: impl Fn(&str) -> Option<String>) -> String {
    env("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|| DEFAULT_OTLP_ENDPOINT.to_owned())
}

/// `otlpenv.GetProtocol`: the first set env wins; unset uses `grpc`.
pub fn otlp_protocol(env: impl Fn(&str) -> Option<String>) -> String {
    env("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|| "grpc".to_owned())
}

/// `trimSchema` (`exporters/otlp.go:70`): case-insensitive `http://`/`https://`.
pub fn trim_schema(endpoint: &str) -> String {
    let lower = endpoint.to_ascii_lowercase();
    for scheme in ["http://", "https://"] {
        if lower.starts_with(scheme) {
            return endpoint[scheme.len()..].to_owned();
        }
    }
    endpoint.to_owned()
}

/// `otlpenv.IsInsecure`: only a literal `http://` prefix counts.
pub fn otlp_is_insecure(endpoint: &str) -> bool {
    endpoint.to_ascii_lowercase().starts_with("http://")
}

/// Resolve the OTLP exporter settings, reproducing the two startup errors of
/// `NewOTLPTraceExporter` (`exporters/otlp.go:26-70`).
pub fn otlp_exporter_config_from_env(
    env: impl Fn(&str) -> Option<String>,
) -> Result<OtlpExporterConfig, String> {
    let endpoint = otlp_endpoint(&env);
    if endpoint.is_empty() {
        return Err(
            "please configure an exporter endpoint with OTEL_EXPORTER_OTLP_ENDPOINT".to_owned(),
        );
    }
    let raw_protocol = otlp_protocol(&env);
    let protocol = OtlpProtocol::parse(&raw_protocol)
        .ok_or_else(|| format!("unexpected protocol \"{raw_protocol}\""))?;
    Ok(OtlpExporterConfig {
        endpoint: trim_schema(&endpoint),
        protocol,
        insecure: otlp_is_insecure(&endpoint),
    })
}

/// The batch seam standing in for the OTLP/Jaeger clients. Tests inject a
/// recording transport; production uses [`NoopSpanTransport`], so an injected
/// exporter never performs I/O.
pub trait SpanTransport: Send + Sync {
    fn send(&self, spans: &[SpanData]) -> Result<(), String>;
    fn shutdown(&self) {}
}

/// The production transport: accepts and discards every batch.
pub struct NoopSpanTransport;

impl SpanTransport for NoopSpanTransport {
    fn send(&self, _spans: &[SpanData]) -> Result<(), String> {
        Ok(())
    }
}

/// `otlptrace.Exporter`: hands finished spans to the injected transport. The
/// wire encoding itself (protobuf/gRPC or HTTP/JSON) stays behind the
/// transport seam.
pub struct OtlpSpanExporter {
    config: OtlpExporterConfig,
    transport: Arc<dyn SpanTransport>,
}

impl OtlpSpanExporter {
    pub fn new(config: OtlpExporterConfig, transport: Arc<dyn SpanTransport>) -> Self {
        Self { config, transport }
    }

    pub fn config(&self) -> &OtlpExporterConfig {
        &self.config
    }
}

impl SpanExporter for OtlpSpanExporter {
    fn export(&self, span: &SpanData) {
        let _ = self.transport.send(std::slice::from_ref(span));
    }

    fn shutdown(&self) {
        self.transport.shutdown();
    }
}

/// The Jaeger exporter seam: the baseline builds a Jaeger client from `jaeger`
/// env config; here the transport stands in for it.
pub struct JaegerSpanExporter {
    transport: Arc<dyn SpanTransport>,
}

impl JaegerSpanExporter {
    pub fn new(transport: Arc<dyn SpanTransport>) -> Self {
        Self { transport }
    }
}

impl SpanExporter for JaegerSpanExporter {
    fn export(&self, span: &SpanData) {
        let _ = self.transport.send(std::slice::from_ref(span));
    }

    fn shutdown(&self) {
        self.transport.shutdown();
    }
}

/// `newOtelSpanProcessor` (`internal/tracer/otel.go:43`): pick the exporter for
/// the configured tracer type. `none` keeps the no-op path.
pub fn build_span_exporter(
    tracer_type: TracerType,
    transport: Arc<dyn SpanTransport>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Option<Arc<dyn SpanExporter>>, String> {
    match tracer_type {
        TracerType::None => Ok(None),
        TracerType::OpenTelemetry => {
            let config = otlp_exporter_config_from_env(env)?;
            Ok(Some(Arc::new(OtlpSpanExporter::new(config, transport))))
        }
        TracerType::Jaeger => Ok(Some(Arc::new(JaegerSpanExporter::new(transport)))),
    }
}

/// `tracer.Init`'s exporter installation: set the policy, and build the
/// exporter from the environment. On error the caller keeps running without an
/// exporter (the baseline's `newConfWatcher` warns and continues).
pub fn init_tracer_from_env(
    startup: &OtelStartup,
    transport: Arc<dyn SpanTransport>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<TracerRuntime, String> {
    let exporter = build_span_exporter(startup.tracer_type, transport, env)?;
    Ok(init_tracer(startup, exporter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_parsing_and_context_precedence() {
        assert_eq!(TracePolicy::parse("all"), Some(TracePolicy::All));
        assert_eq!(TracePolicy::parse("bogus"), None);
        assert!(!should_trace_for(TracePolicy::None, None));
        assert!(should_trace_for(TracePolicy::All, None));
        assert!(!should_trace_for(TracePolicy::Selective, None));
        assert!(should_trace_for(TracePolicy::None, Some(true)));
        assert!(!should_trace_for(TracePolicy::All, Some(false)));
    }

    #[test]
    fn request_opt_in_prefers_header_and_requires_trace_query() {
        assert!(request_wants_tracing(Some("1"), ""));
        assert!(!request_wants_tracing(Some("nope"), "trace=1"));
        // An empty header is treated as absent and falls through to the query.
        assert!(request_wants_tracing(Some(""), "trace=1"));
        assert!(request_wants_tracing(None, "a=1&trace=true"));
        assert!(!request_wants_tracing(None, "a=1"));
        assert!(!request_wants_tracing(None, "xtrace=1"));
        assert!(!request_wants_tracing(None, "trace=bogus"));
    }

    #[test]
    fn sampling_matches_trace_policy_sampler() {
        assert!(should_sample(TracePolicy::All, None));
        assert!(!should_sample(TracePolicy::None, Some(true)));
        assert!(should_sample(TracePolicy::Selective, Some(true)));
        assert!(!should_sample(TracePolicy::Selective, Some(false)));
        assert!(!should_sample(TracePolicy::Selective, None));
    }

    #[test]
    fn scoped_attributes_prefix_keys() {
        let attributes = scoped(
            "repo",
            vec![
                Attribute {
                    key: "name".to_owned(),
                    value: AttributeValue::String("x".to_owned()),
                },
                Attribute {
                    key: "count".to_owned(),
                    value: AttributeValue::Int64(3),
                },
            ],
        );
        assert_eq!(attributes[0].key, "repo.name");
        assert_eq!(attributes[1].key, "repo.count");
    }

    #[test]
    fn error_truncation_keeps_half_on_each_side() {
        let long = "a".repeat(600);
        let truncated = truncate_error(Some(&long), DEFAULT_ERROR_RUNE_LIMIT).unwrap();
        assert!(truncated.contains(" ...truncated... "));
        // 256 kept runes on each side plus the `a` in "truncated".
        assert_eq!(truncated.chars().filter(|ch| *ch == 'a').count(), 513);
        assert_eq!(
            error_attribute(None).value,
            AttributeValue::String("<nil>".to_owned())
        );
    }

    #[test]
    fn startup_plan_mirrors_conf_watcher() {
        let empty = TracingConfig::default();
        let plan = plan_otel_startup(Some(&empty), TracePolicy::None);
        assert_eq!(plan.policy, TracePolicy::Selective);
        assert_eq!(plan.tracer_type, TracerType::OpenTelemetry);

        let none = TracingConfig {
            sampling: "none".to_owned(),
            ..TracingConfig::default()
        };
        let plan = plan_otel_startup(Some(&none), TracePolicy::All);
        assert_eq!(plan.policy, TracePolicy::None);
        assert_eq!(plan.tracer_type, TracerType::None);

        let plan = plan_otel_startup(None, TracePolicy::All);
        assert_eq!(plan.policy, TracePolicy::All);
        assert_eq!(plan.tracer_type, TracerType::None);
    }

    #[test]
    fn otlp_env_mirrors_otlpenv_and_exporter_errors() {
        let empty = |_: &str| None;
        let config = otlp_exporter_config_from_env(empty).unwrap();
        assert_eq!(config.endpoint, "127.0.0.1:4317");
        assert_eq!(config.protocol, OtlpProtocol::Grpc);
        assert!(config.insecure);

        let env = |key: &str| match key {
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("https://collector:4318".to_owned()),
            "OTEL_EXPORTER_OTLP_PROTOCOL" => Some("http/json".to_owned()),
            _ => None,
        };
        let config = otlp_exporter_config_from_env(env).unwrap();
        assert_eq!(config.endpoint, "collector:4318");
        assert_eq!(config.protocol, OtlpProtocol::HttpJson);
        assert!(!config.insecure);

        // An explicitly empty endpoint disables the exporter.
        let env = |key: &str| (key == "OTEL_EXPORTER_OTLP_ENDPOINT").then(String::new);
        assert_eq!(
            otlp_exporter_config_from_env(env).unwrap_err(),
            "please configure an exporter endpoint with OTEL_EXPORTER_OTLP_ENDPOINT"
        );
        // `http/proto` is not supported by the baseline switch.
        let env =
            |key: &str| (key == "OTEL_EXPORTER_OTLP_PROTOCOL").then(|| "http/proto".to_owned());
        assert_eq!(
            otlp_exporter_config_from_env(env).unwrap_err(),
            "unexpected protocol \"http/proto\""
        );
        assert_eq!(trim_schema("HTTP://host:1"), "host:1");
    }

    #[test]
    fn exporter_is_selected_by_tracer_type_and_defaults_to_noop() {
        let startup = OtelStartup {
            policy: TracePolicy::Selective,
            tracer_type: TracerType::OpenTelemetry,
            debug: false,
        };
        let error = init_tracer_from_env(&startup, Arc::new(NoopSpanTransport), |key: &str| {
            (key == "OTEL_EXPORTER_OTLP_ENDPOINT").then(|| "".to_owned())
        })
        .err()
        .unwrap();
        assert_eq!(
            error,
            "please configure an exporter endpoint with OTEL_EXPORTER_OTLP_ENDPOINT"
        );

        let runtime =
            init_tracer_from_env(&startup, Arc::new(NoopSpanTransport), |_: &str| None).unwrap();
        assert!(runtime.enabled());
        let span = SpanData {
            name: "GraphQL".to_owned(),
            trace_id: "t".to_owned(),
            span_id: "s".to_owned(),
            attributes: vec![sampling_retain(true).unwrap()],
        };
        assert!(runtime.export(&span));

        let disabled = OtelStartup {
            tracer_type: TracerType::None,
            ..startup
        };
        let runtime =
            init_tracer_from_env(&disabled, Arc::new(NoopSpanTransport), |_: &str| None).unwrap();
        assert!(!runtime.enabled());
        assert!(!runtime.export(&span));
    }
}
