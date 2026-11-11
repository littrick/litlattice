//! M9.8jm differential for the OTLP wire encoder and HTTP exporter against the
//! Go OTel SDK v1.27 stack driven by
//! `internal/tracer/oteldefaults/exporters/zz_m9jm_oracle_test.go`.
//!
//! Both sides read the same corpus and emit normalised JSON:
//!
//! * `encode` cases produce the `ExportTraceServiceRequest` protobuf bytes
//!   (hex) and `protojson` text the Go exporter actually sends.
//! * `http` cases script HTTP responses and compare every attempt's path,
//!   headers and body plus the error class of the final result.
//! * `config` cases resolve `NewOTLPTraceExporter`'s endpoint/protocol
//!   selection.
//!
//! No network is used: the HTTP client is a scripted [`HttpClient`] and the
//! retry timer does not sleep.
//!
//! Env:
//!
//! * `SG_M9JM_DIFF=1`
//! * `M9JM_CORPUS`, `M9JM_OUT`

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use sg_service_frontend::otlp_wire::{
    build_request, encode_json, encode_protobuf, AnyValue, HttpClient, HttpError, HttpRequest,
    HttpResponse, HttpSpanTransport, HttpWireFormat, InstrumentationScope, KeyValue,
    OtlpHttpConfig, RetryPolicy, RetryTimer, SpanKind, SpanLink, Status, StatusCode, WireSpan,
};
use sg_service_frontend::trace_policy::{
    otlp_exporter_config_from_env, wire_span_from_span_data, Attribute, AttributeValue, SpanData,
};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    encode: Vec<EncodeCase>,
    #[serde(default)]
    http: Vec<HttpCase>,
    #[serde(default)]
    config: Vec<ConfigCase>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EncodeCase {
    name: String,
    resource: Option<ResourceSpec>,
    scope: Option<ScopeSpec>,
    #[serde(default)]
    spans: Vec<SpanSpec>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequestSpec {
    resource: Option<ResourceSpec>,
    scope: Option<ScopeSpec>,
    #[serde(default)]
    spans: Vec<SpanSpec>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceSpec {
    #[serde(default)]
    attributes: Vec<AttrSpec>,
    #[serde(default)]
    schema_url: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    schema_url: String,
}

#[derive(Clone, Deserialize)]
struct AttrSpec {
    key: String,
    kind: String,
    value: serde_json::Value,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    time_nanos: u64,
    #[serde(default)]
    attributes: Vec<AttrSpec>,
    #[serde(default)]
    dropped_attributes: u32,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinkSpec {
    trace_id: String,
    span_id: String,
    #[serde(default)]
    remote: bool,
    #[serde(default)]
    attributes: Vec<AttrSpec>,
    #[serde(default)]
    dropped_attributes: u32,
}

#[derive(Clone, Deserialize)]
struct StatusSpec {
    code: String,
    #[serde(default)]
    message: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpanSpec {
    #[serde(default)]
    name: String,
    trace_id: String,
    span_id: String,
    #[serde(default)]
    trace_state: String,
    parent_span_id: Option<String>,
    #[serde(default)]
    parent_remote: bool,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    start_nanos: u64,
    #[serde(default)]
    end_nanos: u64,
    #[serde(default)]
    attributes: Vec<AttrSpec>,
    #[serde(default)]
    dropped_attributes: u32,
    #[serde(default)]
    events: Vec<EventSpec>,
    #[serde(default)]
    dropped_events: u32,
    #[serde(default)]
    links: Vec<LinkSpec>,
    #[serde(default)]
    dropped_links: u32,
    status: StatusSpec,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RetrySpec {
    enabled: bool,
    initial_ms: u64,
    max_ms: u64,
    max_elapsed_ms: u64,
}

#[derive(Clone, Deserialize)]
struct ScriptedResponse {
    status: u16,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpCase {
    name: String,
    #[serde(default)]
    headers: Vec<[String; 2]>,
    retry: RetrySpec,
    timeout_ms: u64,
    #[serde(default)]
    responses: Vec<ScriptedResponse>,
    request: RequestSpec,
}

#[derive(Clone, Deserialize)]
struct ConfigCase {
    name: String,
    endpoint: Option<String>,
    protocol: Option<String>,
}

// ---------------------------------------------------------------------------
// Output (field order must match the Go oracle's structs)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Output {
    encode: Vec<EncodeOut>,
    http: Vec<HttpOut>,
    config: Vec<ConfigOut>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EncodeOut {
    name: String,
    proto_hex: String,
    json: String,
}

#[derive(Serialize)]
struct HttpOut {
    name: String,
    attempts: Vec<AttemptOut>,
    result: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AttemptOut {
    path: String,
    content_type: String,
    user_agent: String,
    custom_headers: Vec<[String; 2]>,
    body_hex: String,
}

#[derive(Serialize)]
struct ConfigOut {
    name: String,
    selection: String,
    error: String,
    endpoint: String,
    protocol: String,
    insecure: bool,
    wire: String,
}

// ---------------------------------------------------------------------------
// Corpus -> OTLP model
// ---------------------------------------------------------------------------

fn attribute_value(spec: &AttrSpec) -> AttributeValue {
    match spec.kind.as_str() {
        "string" => AttributeValue::String(spec.value.as_str().unwrap().to_owned()),
        "int" => AttributeValue::Int64(spec.value.as_i64().unwrap()),
        "bool" => AttributeValue::Bool(spec.value.as_bool().unwrap()),
        "stringSlice" => AttributeValue::StringSlice(
            spec.value
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect(),
        ),
        other => panic!("unknown attribute kind {other}"),
    }
}

fn attribute_key_value(spec: &AttrSpec) -> KeyValue {
    KeyValue {
        key: spec.key.clone(),
        value: match spec.kind.as_str() {
            "string" => AnyValue::String(spec.value.as_str().unwrap().to_owned()),
            "int" => AnyValue::Int(spec.value.as_i64().unwrap()),
            "bool" => AnyValue::Bool(spec.value.as_bool().unwrap()),
            "stringSlice" => AnyValue::Array(
                spec.value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| AnyValue::String(value.as_str().unwrap().to_owned()))
                    .collect(),
            ),
            other => panic!("unknown attribute kind {other}"),
        },
    }
}

fn attributes(specs: &[AttrSpec]) -> Vec<Attribute> {
    specs
        .iter()
        .map(|spec| Attribute {
            key: spec.key.clone(),
            value: attribute_value(spec),
        })
        .collect()
}

/// `tracetransform.Resource`: attributes are sorted by key (resource.NewWithAttributes).
fn resource_from_spec(spec: &ResourceSpec) -> Option<sg_service_frontend::otlp_wire::Resource> {
    let mut attributes: Vec<KeyValue> = spec.attributes.iter().map(attribute_key_value).collect();
    attributes.sort_by(|left, right| left.key.cmp(&right.key));
    Some(sg_service_frontend::otlp_wire::Resource {
        attributes,
        dropped_attributes_count: 0,
        schema_url: spec.schema_url.clone(),
    })
}

/// `tracetransform.InstrumentationScope`: the zero scope is dropped and scope
/// attributes are never populated.
fn scope_from_spec(spec: &ScopeSpec) -> Option<InstrumentationScope> {
    if spec.name.is_empty() && spec.version.is_empty() && spec.schema_url.is_empty() {
        return None;
    }
    Some(InstrumentationScope {
        name: spec.name.clone(),
        version: spec.version.clone(),
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        schema_url: spec.schema_url.clone(),
    })
}

/// `buildSpanFlags`: HAS_IS_REMOTE (0x100) always, IS_REMOTE (0x200) if remote.
fn span_flags(remote: bool) -> u32 {
    0x100 | if remote { 0x200 } else { 0 }
}

fn span_data(spec: &SpanSpec) -> SpanData {
    SpanData {
        name: spec.name.clone(),
        trace_id: spec.trace_id.clone(),
        span_id: spec.span_id.clone(),
        attributes: attributes(&spec.attributes),
        trace_state: spec.trace_state.clone(),
        parent_span_id: spec.parent_span_id.clone(),
        flags: span_flags(spec.parent_remote),
        kind: SpanKind::parse(&spec.kind).unwrap_or(SpanKind::Unspecified),
        start_time_unix_nano: spec.start_nanos,
        end_time_unix_nano: spec.end_nanos,
        status: Status {
            message: spec.status.message.clone(),
            code: StatusCode::parse(&spec.status.code).unwrap(),
        },
        events: spec
            .events
            .iter()
            .map(|event| sg_service_frontend::otlp_wire::SpanEvent {
                time_unix_nano: event.time_nanos,
                name: event.name.clone(),
                attributes: event.attributes.iter().map(attribute_key_value).collect(),
                dropped_attributes_count: event.dropped_attributes,
            })
            .collect(),
        links: spec
            .links
            .iter()
            .map(|link| SpanLink {
                trace_id: sg_service_frontend::otlp_wire::parse_trace_id(&link.trace_id)
                    .unwrap_or([0u8; 16]),
                span_id: sg_service_frontend::otlp_wire::parse_span_id(&link.span_id)
                    .unwrap_or([0u8; 8]),
                trace_state: String::new(),
                attributes: link.attributes.iter().map(attribute_key_value).collect(),
                dropped_attributes_count: link.dropped_attributes,
                flags: span_flags(link.remote),
            })
            .collect(),
        dropped_attributes_count: spec.dropped_attributes,
        dropped_events_count: spec.dropped_events,
        dropped_links_count: spec.dropped_links,
        resource: None,
        scope: None,
    }
}

fn wire_span(spec: &SpanSpec) -> WireSpan {
    wire_span_from_span_data(&span_data(spec))
}

fn request_from_spec(
    spec: &RequestSpec,
) -> sg_service_frontend::otlp_wire::ExportTraceServiceRequest {
    let resource = spec.resource.as_ref().and_then(resource_from_spec);
    let scope = spec.scope.as_ref().and_then(scope_from_spec);
    let spans: Vec<WireSpan> = spec.spans.iter().map(wire_span).collect();
    build_request(resource.as_ref(), scope.as_ref(), &spans)
}

// ---------------------------------------------------------------------------
// Scripted HTTP client / timer
// ---------------------------------------------------------------------------

struct ScriptedClient {
    responses: Mutex<Vec<ScriptedResponse>>,
    requests: Mutex<Vec<HttpRequest>>,
}

impl ScriptedClient {
    fn new(responses: Vec<ScriptedResponse>) -> Self {
        Self {
            responses: Mutex::new(responses),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl HttpClient for ScriptedClient {
    fn execute(&self, request: &HttpRequest) -> Result<HttpResponse, HttpError> {
        let index = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            requests.len() - 1
        };
        let responses = self.responses.lock().unwrap();
        let scripted = if responses.is_empty() {
            ScriptedResponse {
                status: 200,
                error: None,
            }
        } else {
            responses[index.min(responses.len() - 1)].clone()
        };
        if scripted.error.as_deref() == Some("timeout") {
            return Err(HttpError::Timeout("Client.Timeout exceeded".to_owned()));
        }
        Ok(HttpResponse {
            status: scripted.status,
            headers: Vec::new(),
            body: Vec::new(),
        })
    }
}

struct NoSleep;

impl RetryTimer for NoSleep {
    fn sleep(&self, _delay: Duration) {}
}

fn classify(result: &Result<(), sg_service_frontend::otlp_wire::SendError>) -> String {
    use sg_service_frontend::otlp_wire::SendErrorClass;
    match result {
        Ok(()) => "ok".to_owned(),
        Err(error) => match error.class {
            SendErrorClass::RetryExhausted => "retry_exhausted".to_owned(),
            SendErrorClass::Retryable => "retryable".to_owned(),
            SendErrorClass::Status => "status".to_owned(),
            SendErrorClass::Timeout => "timeout".to_owned(),
            SendErrorClass::Transport | SendErrorClass::InvalidEndpoint => "transport".to_owned(),
        },
    }
}

fn header(request: &HttpRequest, name: &str) -> String {
    request
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

fn attempt_out(request: &HttpRequest, custom: &[[String; 2]]) -> AttemptOut {
    let url = url::Url::parse(&request.url).unwrap();
    AttemptOut {
        path: url.path().to_owned(),
        content_type: header(request, "Content-Type"),
        user_agent: header(request, "User-Agent"),
        custom_headers: custom
            .iter()
            .map(|[name, _]| [name.clone(), header(request, name)])
            .collect(),
        body_hex: hex_encode(&request.body),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn run_http(case: &HttpCase) -> HttpOut {
    let request = request_from_spec(&case.request);
    let mut config = OtlpHttpConfig::new("127.0.0.1:4318", true, HttpWireFormat::Protobuf);
    config.headers = case
        .headers
        .iter()
        .map(|[name, value]| (name.clone(), value.clone()))
        .collect();
    config.timeout = Duration::from_millis(case.timeout_ms);
    config.retry = RetryPolicy {
        enabled: case.retry.enabled,
        initial_interval: Duration::from_millis(case.retry.initial_ms),
        max_interval: Duration::from_millis(case.retry.max_ms),
        max_elapsed_time: Duration::from_millis(case.retry.max_elapsed_ms),
    };
    let client = Arc::new(ScriptedClient::new(case.responses.clone()));
    let transport = HttpSpanTransport::with_timer(config, client.clone(), Arc::new(NoSleep));
    let result = transport.send_request_detailed(&request);
    let requests = client.requests.lock().unwrap();
    HttpOut {
        name: case.name.clone(),
        attempts: requests
            .iter()
            .map(|request| attempt_out(request, &case.headers))
            .collect(),
        result: classify(&result),
    }
}

// ---------------------------------------------------------------------------
// Config cases
// ---------------------------------------------------------------------------

fn run_config(case: &ConfigCase) -> ConfigOut {
    let mut env: BTreeMap<&str, String> = BTreeMap::new();
    if let Some(endpoint) = &case.endpoint {
        env.insert("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint.clone());
    }
    if let Some(protocol) = &case.protocol {
        env.insert("OTEL_EXPORTER_OTLP_PROTOCOL", protocol.clone());
    }
    match otlp_exporter_config_from_env(|key| env.get(key).cloned()) {
        Ok(config) => {
            let wire = match config.wire_format() {
                Some(HttpWireFormat::Protobuf) => "protobuf",
                Some(HttpWireFormat::Json) => "json",
                None => "grpc",
            };
            ConfigOut {
                name: case.name.clone(),
                selection: "otlp".to_owned(),
                error: String::new(),
                endpoint: config.endpoint,
                protocol: config.protocol.as_str().to_owned(),
                insecure: config.insecure,
                wire: wire.to_owned(),
            }
        }
        Err(error) => ConfigOut {
            name: case.name.clone(),
            selection: "error".to_owned(),
            error,
            endpoint: String::new(),
            protocol: String::new(),
            insecure: false,
            wire: String::new(),
        },
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[test]
fn otlp_wire_diff() {
    if std::env::var("SG_M9JM_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M9JM_CORPUS").expect("M9JM_CORPUS");
    let out_path = std::env::var("M9JM_OUT").expect("M9JM_OUT");
    let raw = fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let encode = corpus
        .encode
        .iter()
        .map(|case| {
            let request = request_from_spec(&RequestSpec {
                resource: case.resource.clone(),
                scope: case.scope.clone(),
                spans: case.spans.clone(),
            });
            EncodeOut {
                name: case.name.clone(),
                proto_hex: hex_encode(&encode_protobuf(&request)),
                json: encode_json(&request),
            }
        })
        .collect();
    let http = corpus.http.iter().map(run_http).collect();
    let config = corpus.config.iter().map(run_config).collect();

    let output = Output {
        encode,
        http,
        config,
    };
    let text = serde_json::to_string(&output).expect("serialize");
    fs::write(&out_path, text).expect("write output");
    println!("RUST OTLP wire harness wrote {out_path}");
}
