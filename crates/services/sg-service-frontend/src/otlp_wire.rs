#![forbid(unsafe_code)]

//! OTLP/HTTP wire encoding and export for the tracer seam.
//!
//! This ports the observable encoding of the OTel Go SDK v1.27 exporter stack:
//!
//! * [`ExportTraceServiceRequest`] and the resource/scope/span messages mirror
//!   `go.opentelemetry.io/proto/otlp` v1.2.0.
//! * [`encode_protobuf`] reproduces `proto.Marshal` bytes: fields in ascending
//!   field-number order, proto3 defaults omitted, non-nil messages emitted even
//!   when empty, and oneof values emitted even when they hold a default value.
//! * [`encode_json`] reproduces `protojson.Marshal` output: declaration-order
//!   fields, camelCase names, compact `,`/`:` separators, 64-bit integers as
//!   decimal strings, enums as names, bytes as padded standard base64, and the
//!   same string escaping.
//! * [`HttpSpanTransport`] ports `otlptracehttp` request construction (path
//!   `/v1/traces`, `User-Agent`, injected headers, `Content-Type`) and the
//!   retry policy. The HTTP client and retry timer are injectable; the default
//!   transport used by the process remains [`crate::trace_policy::NoopSpanTransport`],
//!   and no test performs network I/O.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;

use crate::trace_policy::{OtlpProtocol, SpanData, SpanTransport};

/// `otlptracehttp/internal/otlpconfig.DefaultTracesPath`.
pub const DEFAULT_TRACES_PATH: &str = "/v1/traces";

/// `otlptrace.Version()` user agent (`otlptracehttp.newRequest`).
pub const OTEL_USER_AGENT: &str = "OTel OTLP Exporter Go/1.27.0";

/// `otlptracehttp.contentTypeProto`.
pub const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// `otlptracehttp` JSON content type (OTLP/HTTP JSON mapping).
pub const CONTENT_TYPE_JSON: &str = "application/json";

/// `otlptracehttp/internal/otlpconfig.DefaultTimeout`.
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Proto model (`go.opentelemetry.io/proto/otlp` v1.2.0)
// ---------------------------------------------------------------------------

/// `opentelemetry.proto.trace.v1.Span.SpanKind`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SpanKind {
    #[default]
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    pub fn as_proto(self) -> u32 {
        match self {
            Self::Unspecified => 0,
            Self::Internal => 1,
            Self::Server => 2,
            Self::Client => 3,
            Self::Producer => 4,
            Self::Consumer => 5,
        }
    }

    /// `protojson` enum value name.
    pub fn as_json(self) -> &'static str {
        match self {
            Self::Unspecified => "SPAN_KIND_UNSPECIFIED",
            Self::Internal => "SPAN_KIND_INTERNAL",
            Self::Server => "SPAN_KIND_SERVER",
            Self::Client => "SPAN_KIND_CLIENT",
            Self::Producer => "SPAN_KIND_PRODUCER",
            Self::Consumer => "SPAN_KIND_CONSUMER",
        }
    }

    /// Parse the lower-case form used by the differential corpus.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "unspecified" => Some(Self::Unspecified),
            "internal" => Some(Self::Internal),
            "server" => Some(Self::Server),
            "client" => Some(Self::Client),
            "producer" => Some(Self::Producer),
            "consumer" => Some(Self::Consumer),
            _ => None,
        }
    }
}

/// `opentelemetry.proto.trace.v1.Status.StatusCode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatusCode {
    #[default]
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    pub fn as_proto(self) -> u32 {
        match self {
            Self::Unset => 0,
            Self::Ok => 1,
            Self::Error => 2,
        }
    }

    pub fn as_json(self) -> &'static str {
        match self {
            Self::Unset => "STATUS_CODE_UNSET",
            Self::Ok => "STATUS_CODE_OK",
            Self::Error => "STATUS_CODE_ERROR",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "unset" => Some(Self::Unset),
            "ok" => Some(Self::Ok),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

/// `opentelemetry.proto.trace.v1.Status`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Status {
    pub message: String,
    pub code: StatusCode,
}

/// One `AnyValue` variant. The ported attribute surfaces only produce these
/// variants (`internal/trace/attributes.go`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnyValue {
    String(String),
    Bool(bool),
    Int(i64),
    Array(Vec<AnyValue>),
}

/// `opentelemetry.proto.common.v1.KeyValue`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: AnyValue,
}

/// `opentelemetry.proto.resource.v1.Resource`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Resource {
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub schema_url: String,
}

/// `opentelemetry.proto.common.v1.InstrumentationScope`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InstrumentationScope {
    pub name: String,
    pub version: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub schema_url: String,
}

/// `opentelemetry.proto.trace.v1.Span.Event`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpanEvent {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

/// `opentelemetry.proto.trace.v1.Span.Link`. The SDK transform never sets the
/// span-link `trace_state`, but the field exists in the wire model.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpanLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_state: String,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
}

/// `opentelemetry.proto.trace.v1.Span`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WireSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_state: String,
    pub parent_span_id: Option<[u8; 8]>,
    pub flags: u32,
    pub name: String,
    pub kind: SpanKind,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub events: Vec<SpanEvent>,
    pub dropped_events_count: u32,
    pub links: Vec<SpanLink>,
    pub dropped_links_count: u32,
    pub status: Option<Status>,
}

/// `opentelemetry.proto.trace.v1.ScopeSpans`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeSpans {
    pub scope: Option<InstrumentationScope>,
    pub spans: Vec<WireSpan>,
    pub schema_url: String,
}

/// `opentelemetry.proto.trace.v1.ResourceSpans`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceSpans {
    pub resource: Option<Resource>,
    pub scope_spans: Vec<ScopeSpans>,
    pub schema_url: String,
}

/// `opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportTraceServiceRequest {
    pub resource_spans: Vec<ResourceSpans>,
}

/// Build the single-resource/single-scope request shape produced by
/// `tracetransform.Spans` for one resource and one instrumentation scope.
pub fn build_request(
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
    spans: &[WireSpan],
) -> ExportTraceServiceRequest {
    if spans.is_empty() {
        return ExportTraceServiceRequest::default();
    }
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: resource.cloned(),
            schema_url: resource.map(|r| r.schema_url.clone()).unwrap_or_default(),
            scope_spans: vec![ScopeSpans {
                scope: scope.cloned(),
                spans: spans.to_vec(),
                schema_url: scope.map(|s| s.schema_url.clone()).unwrap_or_default(),
            }],
        }],
    }
}

/// Parse a 32-character hex trace id (`trace.TraceIDFromHex`).
pub fn parse_trace_id(value: &str) -> Option<[u8; 16]> {
    parse_hex_array(value)
}

/// Parse a 16-character hex span id (`trace.SpanIDFromHex`).
pub fn parse_span_id(value: &str) -> Option<[u8; 8]> {
    parse_hex_array(value)
}

fn parse_hex_array<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let bytes = value.as_bytes();
    let mut out = [0u8; N];
    for (index, slot) in out.iter_mut().enumerate() {
        let high = hex_nibble(bytes[index * 2])?;
        let low = hex_nibble(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Some(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Protobuf binary encoding (`proto.Marshal`)
// ---------------------------------------------------------------------------

const WIRE_VARINT: u32 = 0;
const WIRE_FIXED64: u32 = 1;
const WIRE_LEN: u32 = 2;
const WIRE_FIXED32: u32 = 5;

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn write_tag(out: &mut Vec<u8>, field: u32, wire: u32) {
    write_varint(out, u64::from((field << 3) | wire));
}

fn write_len_field(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    write_tag(out, field, WIRE_LEN);
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn write_string_field(out: &mut Vec<u8>, field: u32, value: &str) {
    if !value.is_empty() {
        write_len_field(out, field, value.as_bytes());
    }
}

fn write_bytes_field(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    if !value.is_empty() {
        write_len_field(out, field, value);
    }
}

fn write_varint_field(out: &mut Vec<u8>, field: u32, value: u64) {
    if value != 0 {
        write_tag(out, field, WIRE_VARINT);
        write_varint(out, value);
    }
}

fn write_fixed64_field(out: &mut Vec<u8>, field: u32, value: u64) {
    if value != 0 {
        write_tag(out, field, WIRE_FIXED64);
        out.extend_from_slice(&value.to_le_bytes());
    }
}

fn write_fixed32_field(out: &mut Vec<u8>, field: u32, value: u32) {
    if value != 0 {
        write_tag(out, field, WIRE_FIXED32);
        out.extend_from_slice(&value.to_le_bytes());
    }
}

fn write_message_field<F>(out: &mut Vec<u8>, field: u32, encode: F)
where
    F: FnOnce(&mut Vec<u8>),
{
    let mut inner = Vec::new();
    encode(&mut inner);
    write_len_field(out, field, &inner);
}

/// `proto.Marshal` for an `AnyValue`. The oneof value is always emitted,
/// including default values (oneof presence).
fn encode_any_value(value: &AnyValue, out: &mut Vec<u8>) {
    match value {
        AnyValue::String(text) => write_len_field(out, 1, text.as_bytes()),
        AnyValue::Bool(flag) => {
            write_tag(out, 2, WIRE_VARINT);
            write_varint(out, u64::from(*flag));
        }
        AnyValue::Int(number) => {
            write_tag(out, 3, WIRE_VARINT);
            write_varint(out, *number as u64);
        }
        AnyValue::Array(values) => write_message_field(out, 5, |inner| {
            for value in values {
                write_message_field(inner, 1, |item| encode_any_value(value, item));
            }
        }),
    }
}

fn encode_key_value(key_value: &KeyValue, out: &mut Vec<u8>) {
    write_string_field(out, 1, &key_value.key);
    write_message_field(out, 2, |inner| encode_any_value(&key_value.value, inner));
}

fn encode_attributes(out: &mut Vec<u8>, field: u32, attributes: &[KeyValue]) {
    for attribute in attributes {
        write_message_field(out, field, |inner| encode_key_value(attribute, inner));
    }
}

fn encode_resource(resource: &Resource, out: &mut Vec<u8>) {
    encode_attributes(out, 1, &resource.attributes);
    write_varint_field(out, 2, u64::from(resource.dropped_attributes_count));
}

fn encode_scope(scope: &InstrumentationScope, out: &mut Vec<u8>) {
    write_string_field(out, 1, &scope.name);
    write_string_field(out, 2, &scope.version);
    encode_attributes(out, 3, &scope.attributes);
    write_varint_field(out, 4, u64::from(scope.dropped_attributes_count));
}

fn encode_status(status: &Status, out: &mut Vec<u8>) {
    write_string_field(out, 2, &status.message);
    write_varint_field(out, 3, u64::from(status.code.as_proto()));
}

fn encode_event(event: &SpanEvent, out: &mut Vec<u8>) {
    write_fixed64_field(out, 1, event.time_unix_nano);
    write_string_field(out, 2, &event.name);
    encode_attributes(out, 3, &event.attributes);
    write_varint_field(out, 4, u64::from(event.dropped_attributes_count));
}

fn encode_link(link: &SpanLink, out: &mut Vec<u8>) {
    write_bytes_field(out, 1, &link.trace_id);
    write_bytes_field(out, 2, &link.span_id);
    write_string_field(out, 3, &link.trace_state);
    encode_attributes(out, 4, &link.attributes);
    write_varint_field(out, 5, u64::from(link.dropped_attributes_count));
    write_fixed32_field(out, 6, link.flags);
}

fn encode_span(span: &WireSpan, out: &mut Vec<u8>) {
    write_bytes_field(out, 1, &span.trace_id);
    write_bytes_field(out, 2, &span.span_id);
    write_string_field(out, 3, &span.trace_state);
    if let Some(parent) = &span.parent_span_id {
        write_bytes_field(out, 4, parent);
    }
    write_string_field(out, 5, &span.name);
    write_varint_field(out, 6, u64::from(span.kind.as_proto()));
    write_fixed64_field(out, 7, span.start_time_unix_nano);
    write_fixed64_field(out, 8, span.end_time_unix_nano);
    encode_attributes(out, 9, &span.attributes);
    write_varint_field(out, 10, u64::from(span.dropped_attributes_count));
    for event in &span.events {
        write_message_field(out, 11, |inner| encode_event(event, inner));
    }
    write_varint_field(out, 12, u64::from(span.dropped_events_count));
    for link in &span.links {
        write_message_field(out, 13, |inner| encode_link(link, inner));
    }
    write_varint_field(out, 14, u64::from(span.dropped_links_count));
    if let Some(status) = &span.status {
        write_message_field(out, 15, |inner| encode_status(status, inner));
    }
    write_fixed32_field(out, 16, span.flags);
}

fn encode_scope_spans(scope_spans: &ScopeSpans, out: &mut Vec<u8>) {
    if let Some(scope) = &scope_spans.scope {
        write_message_field(out, 1, |inner| encode_scope(scope, inner));
    }
    for span in &scope_spans.spans {
        write_message_field(out, 2, |inner| encode_span(span, inner));
    }
    write_string_field(out, 3, &scope_spans.schema_url);
}

fn encode_resource_spans(resource_spans: &ResourceSpans, out: &mut Vec<u8>) {
    if let Some(resource) = &resource_spans.resource {
        write_message_field(out, 1, |inner| encode_resource(resource, inner));
    }
    for scope_spans in &resource_spans.scope_spans {
        write_message_field(out, 2, |inner| encode_scope_spans(scope_spans, inner));
    }
    write_string_field(out, 3, &resource_spans.schema_url);
}

/// Encode an `ExportTraceServiceRequest` exactly like `proto.Marshal`.
pub fn encode_protobuf(request: &ExportTraceServiceRequest) -> Vec<u8> {
    let mut out = Vec::new();
    for resource_spans in &request.resource_spans {
        write_message_field(&mut out, 1, |inner| {
            encode_resource_spans(resource_spans, inner)
        });
    }
    out
}

// ---------------------------------------------------------------------------
// OTLP/JSON encoding (`protojson.Marshal`)
// ---------------------------------------------------------------------------

fn json_escape(out: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            character if (character as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", character as u32);
            }
            character => out.push(character),
        }
    }
}

fn json_string(out: &mut String, value: &str) {
    out.push('"');
    json_escape(out, value);
    out.push('"');
}

fn json_member_name(out: &mut String, first: &mut bool, name: &str) {
    if !*first {
        out.push(',');
    }
    *first = false;
    json_string(out, name);
    out.push(':');
}

fn json_base64(out: &mut String, bytes: &[u8]) {
    out.push('"');
    out.push_str(&base64::engine::general_purpose::STANDARD.encode(bytes));
    out.push('"');
}

fn encode_any_value_json(value: &AnyValue, out: &mut String) {
    out.push('{');
    let mut first = true;
    match value {
        AnyValue::String(text) => {
            json_member_name(out, &mut first, "stringValue");
            json_string(out, text);
        }
        AnyValue::Bool(flag) => {
            json_member_name(out, &mut first, "boolValue");
            out.push_str(if *flag { "true" } else { "false" });
        }
        AnyValue::Int(number) => {
            // protojson renders 64-bit integers as decimal strings.
            json_member_name(out, &mut first, "intValue");
            let _ = write!(out, "\"{number}\"");
        }
        AnyValue::Array(values) => {
            json_member_name(out, &mut first, "arrayValue");
            out.push('{');
            if !values.is_empty() {
                let mut inner_first = true;
                json_member_name(out, &mut inner_first, "values");
                out.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    encode_any_value_json(value, out);
                }
                out.push(']');
            }
            out.push('}');
        }
    }
    out.push('}');
}

fn encode_key_value_json(key_value: &KeyValue, out: &mut String) {
    out.push('{');
    let mut first = true;
    if !key_value.key.is_empty() {
        json_member_name(out, &mut first, "key");
        json_string(out, &key_value.key);
    }
    json_member_name(out, &mut first, "value");
    encode_any_value_json(&key_value.value, out);
    out.push('}');
}

fn encode_attributes_json(attributes: &[KeyValue], out: &mut String) {
    out.push('[');
    for (index, attribute) in attributes.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        encode_key_value_json(attribute, out);
    }
    out.push(']');
}

fn encode_resource_json(resource: &Resource, out: &mut String) {
    out.push('{');
    let mut first = true;
    if !resource.attributes.is_empty() {
        json_member_name(out, &mut first, "attributes");
        encode_attributes_json(&resource.attributes, out);
    }
    if resource.dropped_attributes_count != 0 {
        json_member_name(out, &mut first, "droppedAttributesCount");
        let _ = write!(out, "{}", resource.dropped_attributes_count);
    }
    out.push('}');
}

fn encode_scope_json(scope: &InstrumentationScope, out: &mut String) {
    out.push('{');
    let mut first = true;
    if !scope.name.is_empty() {
        json_member_name(out, &mut first, "name");
        json_string(out, &scope.name);
    }
    if !scope.version.is_empty() {
        json_member_name(out, &mut first, "version");
        json_string(out, &scope.version);
    }
    if !scope.attributes.is_empty() {
        json_member_name(out, &mut first, "attributes");
        encode_attributes_json(&scope.attributes, out);
    }
    if scope.dropped_attributes_count != 0 {
        json_member_name(out, &mut first, "droppedAttributesCount");
        let _ = write!(out, "{}", scope.dropped_attributes_count);
    }
    out.push('}');
}

fn encode_status_json(status: &Status, out: &mut String) {
    out.push('{');
    let mut first = true;
    if !status.message.is_empty() {
        json_member_name(out, &mut first, "message");
        json_string(out, &status.message);
    }
    if status.code != StatusCode::Unset {
        json_member_name(out, &mut first, "code");
        json_string(out, status.code.as_json());
    }
    out.push('}');
}

fn encode_event_json(event: &SpanEvent, out: &mut String) {
    out.push('{');
    let mut first = true;
    if event.time_unix_nano != 0 {
        json_member_name(out, &mut first, "timeUnixNano");
        let _ = write!(out, "\"{}\"", event.time_unix_nano);
    }
    if !event.name.is_empty() {
        json_member_name(out, &mut first, "name");
        json_string(out, &event.name);
    }
    if !event.attributes.is_empty() {
        json_member_name(out, &mut first, "attributes");
        encode_attributes_json(&event.attributes, out);
    }
    if event.dropped_attributes_count != 0 {
        json_member_name(out, &mut first, "droppedAttributesCount");
        let _ = write!(out, "{}", event.dropped_attributes_count);
    }
    out.push('}');
}

fn encode_link_json(link: &SpanLink, out: &mut String) {
    out.push('{');
    let mut first = true;
    json_member_name(out, &mut first, "traceId");
    json_base64(out, &link.trace_id);
    json_member_name(out, &mut first, "spanId");
    json_base64(out, &link.span_id);
    if !link.trace_state.is_empty() {
        json_member_name(out, &mut first, "traceState");
        json_string(out, &link.trace_state);
    }
    if !link.attributes.is_empty() {
        json_member_name(out, &mut first, "attributes");
        encode_attributes_json(&link.attributes, out);
    }
    if link.dropped_attributes_count != 0 {
        json_member_name(out, &mut first, "droppedAttributesCount");
        let _ = write!(out, "{}", link.dropped_attributes_count);
    }
    if link.flags != 0 {
        json_member_name(out, &mut first, "flags");
        let _ = write!(out, "{}", link.flags);
    }
    out.push('}');
}

fn encode_span_json(span: &WireSpan, out: &mut String) {
    out.push('{');
    let mut first = true;
    json_member_name(out, &mut first, "traceId");
    json_base64(out, &span.trace_id);
    json_member_name(out, &mut first, "spanId");
    json_base64(out, &span.span_id);
    if !span.trace_state.is_empty() {
        json_member_name(out, &mut first, "traceState");
        json_string(out, &span.trace_state);
    }
    if let Some(parent) = &span.parent_span_id {
        json_member_name(out, &mut first, "parentSpanId");
        json_base64(out, parent);
    }
    if span.flags != 0 {
        json_member_name(out, &mut first, "flags");
        let _ = write!(out, "{}", span.flags);
    }
    if !span.name.is_empty() {
        json_member_name(out, &mut first, "name");
        json_string(out, &span.name);
    }
    if span.kind != SpanKind::Unspecified {
        json_member_name(out, &mut first, "kind");
        json_string(out, span.kind.as_json());
    }
    if span.start_time_unix_nano != 0 {
        json_member_name(out, &mut first, "startTimeUnixNano");
        let _ = write!(out, "\"{}\"", span.start_time_unix_nano);
    }
    if span.end_time_unix_nano != 0 {
        json_member_name(out, &mut first, "endTimeUnixNano");
        let _ = write!(out, "\"{}\"", span.end_time_unix_nano);
    }
    if !span.attributes.is_empty() {
        json_member_name(out, &mut first, "attributes");
        encode_attributes_json(&span.attributes, out);
    }
    if span.dropped_attributes_count != 0 {
        json_member_name(out, &mut first, "droppedAttributesCount");
        let _ = write!(out, "{}", span.dropped_attributes_count);
    }
    if !span.events.is_empty() {
        json_member_name(out, &mut first, "events");
        out.push('[');
        for (index, event) in span.events.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            encode_event_json(event, out);
        }
        out.push(']');
    }
    if span.dropped_events_count != 0 {
        json_member_name(out, &mut first, "droppedEventsCount");
        let _ = write!(out, "{}", span.dropped_events_count);
    }
    if !span.links.is_empty() {
        json_member_name(out, &mut first, "links");
        out.push('[');
        for (index, link) in span.links.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            encode_link_json(link, out);
        }
        out.push(']');
    }
    if span.dropped_links_count != 0 {
        json_member_name(out, &mut first, "droppedLinksCount");
        let _ = write!(out, "{}", span.dropped_links_count);
    }
    if let Some(status) = &span.status {
        json_member_name(out, &mut first, "status");
        encode_status_json(status, out);
    }
    out.push('}');
}

fn encode_scope_spans_json(scope_spans: &ScopeSpans, out: &mut String) {
    out.push('{');
    let mut first = true;
    if let Some(scope) = &scope_spans.scope {
        json_member_name(out, &mut first, "scope");
        encode_scope_json(scope, out);
    }
    if !scope_spans.spans.is_empty() {
        json_member_name(out, &mut first, "spans");
        out.push('[');
        for (index, span) in scope_spans.spans.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            encode_span_json(span, out);
        }
        out.push(']');
    }
    if !scope_spans.schema_url.is_empty() {
        json_member_name(out, &mut first, "schemaUrl");
        json_string(out, &scope_spans.schema_url);
    }
    out.push('}');
}

fn encode_resource_spans_json(resource_spans: &ResourceSpans, out: &mut String) {
    out.push('{');
    let mut first = true;
    if let Some(resource) = &resource_spans.resource {
        json_member_name(out, &mut first, "resource");
        encode_resource_json(resource, out);
    }
    if !resource_spans.scope_spans.is_empty() {
        json_member_name(out, &mut first, "scopeSpans");
        out.push('[');
        for (index, scope_spans) in resource_spans.scope_spans.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            encode_scope_spans_json(scope_spans, out);
        }
        out.push(']');
    }
    if !resource_spans.schema_url.is_empty() {
        json_member_name(out, &mut first, "schemaUrl");
        json_string(out, &resource_spans.schema_url);
    }
    out.push('}');
}

/// Encode an `ExportTraceServiceRequest` exactly like `protojson.Marshal`.
pub fn encode_json(request: &ExportTraceServiceRequest) -> String {
    let mut out = String::new();
    out.push('{');
    if !request.resource_spans.is_empty() {
        out.push_str("\"resourceSpans\":[");
        for (index, resource_spans) in request.resource_spans.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            encode_resource_spans_json(resource_spans, &mut out);
        }
        out.push(']');
    }
    out.push('}');
    out
}

// ---------------------------------------------------------------------------
// HTTP request construction (`otlptracehttp.newRequest`)
// ---------------------------------------------------------------------------

/// The wire format selected for an OTLP/HTTP export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpWireFormat {
    Protobuf,
    Json,
}

impl HttpWireFormat {
    /// The `Content-Type` the SDK sets (and forces after custom headers).
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Protobuf => CONTENT_TYPE_PROTOBUF,
            Self::Json => CONTENT_TYPE_JSON,
        }
    }

    /// `otlptracehttp` v1.27 always marshals protobuf, even for the
    /// `http/json` protocol (see the overview's errata). The baseline mapping
    /// therefore selects [`Self::Protobuf`].
    pub fn for_protocol(protocol: OtlpProtocol) -> Option<Self> {
        match protocol {
            OtlpProtocol::Grpc => None,
            OtlpProtocol::HttpJson => Some(Self::Protobuf),
        }
    }

    fn encode(self, request: &ExportTraceServiceRequest) -> Vec<u8> {
        match self {
            Self::Protobuf => encode_protobuf(request),
            Self::Json => encode_json(request).into_bytes(),
        }
    }
}

/// Build `{scheme}://{authority}{base}/v1/traces` from a configured endpoint.
///
/// The baseline passes the scheme-stripped endpoint as `url.URL.Host`; an
/// endpoint with a path therefore builds an invalid URL. This helper treats a
/// path suffix as a base path (OTLP spec behavior) instead.
pub fn traces_url(endpoint: &str, insecure: bool) -> Result<String, String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("empty OTLP endpoint".to_owned());
    }
    let (authority, base) = match endpoint.find('/') {
        Some(index) => (&endpoint[..index], &endpoint[index..]),
        None => (endpoint, ""),
    };
    if authority.is_empty() {
        return Err(format!("invalid OTLP endpoint {endpoint:?}"));
    }
    let base = base.trim_end_matches('/');
    let path = if base.is_empty() {
        DEFAULT_TRACES_PATH.to_owned()
    } else {
        format!("{base}{DEFAULT_TRACES_PATH}")
    };
    let scheme = if insecure { "http" } else { "https" };
    Ok(format!("{scheme}://{authority}{path}"))
}

/// One outbound HTTP request built by [`HttpSpanTransport`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub timeout: Duration,
}

/// A scripted/captured HTTP response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Transport-level failures. `Temporary` corresponds to Go's
/// `url.Error.Temporary()` which the SDK retries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HttpError {
    Timeout(String),
    Temporary(String),
    Other(String),
}

/// Injectable HTTP client. Tests script responses; production never uses a
/// real client in this slice (the default process transport stays noop).
pub trait HttpClient: Send + Sync {
    fn execute(&self, request: &HttpRequest) -> Result<HttpResponse, HttpError>;
}

/// Injectable sleep hook so retry tests do not wait.
pub trait RetryTimer: Send + Sync {
    fn sleep(&self, delay: Duration);
}

/// Production timer (only reached when a real HTTP transport is constructed).
pub struct SystemRetryTimer;

impl RetryTimer for SystemRetryTimer {
    fn sleep(&self, delay: Duration) {
        std::thread::sleep(delay);
    }
}

/// `otlptracehttp/internal/retry.DefaultConfig`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub enabled: bool,
    pub initial_interval: Duration,
    pub max_interval: Duration,
    /// Zero means "never stop" (Go's `MaxElapsedTime == 0`).
    pub max_elapsed_time: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_interval: Duration::from_secs(5),
            max_interval: Duration::from_secs(30),
            max_elapsed_time: Duration::from_secs(60),
        }
    }
}

/// `otlptracehttp` signal configuration resolved for the transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtlpHttpConfig {
    /// Scheme-stripped endpoint (`host[:port]`, optionally with a base path).
    pub endpoint: String,
    pub insecure: bool,
    pub format: HttpWireFormat,
    /// Additional headers, injected before the forced `Content-Type`.
    pub headers: Vec<(String, String)>,
    pub timeout: Duration,
    pub retry: RetryPolicy,
}

impl OtlpHttpConfig {
    pub fn new(endpoint: impl Into<String>, insecure: bool, format: HttpWireFormat) -> Self {
        Self {
            endpoint: endpoint.into(),
            insecure,
            format,
            headers: Vec::new(),
            timeout: DEFAULT_HTTP_TIMEOUT,
            retry: RetryPolicy::default(),
        }
    }

    pub fn url(&self) -> Result<String, String> {
        traces_url(&self.endpoint, self.insecure)
    }

    /// Headers in the SDK's `Set` order: user agent, custom headers, then the
    /// forced content type.
    pub fn request_headers(&self) -> Vec<(String, String)> {
        let mut headers = vec![("User-Agent".to_owned(), OTEL_USER_AGENT.to_owned())];
        headers.extend(self.headers.iter().cloned());
        headers.push((
            "Content-Type".to_owned(),
            self.format.content_type().to_owned(),
        ));
        headers
    }
}

/// Error class returned by [`HttpSpanTransport::send_request`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendErrorClass {
    InvalidEndpoint,
    /// A context deadline surfaced without the SDK's retryable wrapper.
    Timeout,
    /// `otlptracehttp`'s `retryableError`: a transient client failure (a
    /// timeout included, since `url.Error.Temporary()` covers them) or a
    /// retryable status. Returned as-is when retries are disabled.
    Retryable,
    /// A non-retryable response status (`failed to send to ...`).
    Status,
    RetryExhausted,
    Transport,
}

/// Rich send error used by the differential harness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendError {
    pub class: SendErrorClass,
    pub message: String,
}

impl SendError {
    fn new(class: SendErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }
}

/// `otlptracehttp` retryable statuses.
fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 502 | 503 | 504)
}

/// `newResponseError`: parse an integer-seconds `Retry-After`.
fn retry_after(headers: &[(String, String)]) -> Duration {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("retry-after") {
            if let Ok(seconds) = value.trim().parse::<u64>() {
                return Duration::from_secs(seconds);
            }
        }
    }
    Duration::ZERO
}

/// OTLP/HTTP transport: encodes finished spans and POSTs them to the collector
/// through the injected client.
pub struct HttpSpanTransport {
    config: OtlpHttpConfig,
    client: Arc<dyn HttpClient>,
    timer: Arc<dyn RetryTimer>,
}

impl HttpSpanTransport {
    pub fn new(config: OtlpHttpConfig, client: Arc<dyn HttpClient>) -> Self {
        Self {
            config,
            client,
            timer: Arc::new(SystemRetryTimer),
        }
    }

    pub fn with_timer(
        config: OtlpHttpConfig,
        client: Arc<dyn HttpClient>,
        timer: Arc<dyn RetryTimer>,
    ) -> Self {
        Self {
            config,
            client,
            timer,
        }
    }

    pub fn config(&self) -> &OtlpHttpConfig {
        &self.config
    }

    /// Encode and send, retrying like `otlptracehttp`'s `RequestFunc`.
    /// The richer error makes the differential harness assert an error class.
    pub fn send_request_detailed(
        &self,
        request: &ExportTraceServiceRequest,
    ) -> Result<(), SendError> {
        let url = self
            .config
            .url()
            .map_err(|message| SendError::new(SendErrorClass::InvalidEndpoint, message))?;
        let body = self.config.format.encode(request);
        let headers = self.config.request_headers();
        let mut backoff = self.config.retry.initial_interval;
        let mut elapsed = Duration::ZERO;
        loop {
            let http_request = HttpRequest {
                method: "POST".to_owned(),
                url: url.clone(),
                headers: headers.clone(),
                body: body.clone(),
                timeout: self.config.timeout,
            };
            let (message, throttle) = match self.client.execute(&http_request) {
                Ok(response) if (200..=299).contains(&response.status) => return Ok(()),
                Ok(response) if is_retryable_status(response.status) => (
                    "retry-able request failure".to_owned(),
                    retry_after(&response.headers),
                ),
                Ok(response) => {
                    return Err(SendError::new(
                        SendErrorClass::Status,
                        format!("failed to send to {url}: {}", response.status),
                    ))
                }
                // `url.Error.Temporary()` is true for client timeouts, so the
                // SDK folds them into the same retryable error as a 429/502/503/504.
                Err(HttpError::Timeout(_)) | Err(HttpError::Temporary(_)) => {
                    ("retry-able request failure".to_owned(), Duration::ZERO)
                }
                Err(HttpError::Other(message)) => {
                    return Err(SendError::new(SendErrorClass::Transport, message))
                }
            };
            if !self.config.retry.enabled {
                return Err(SendError::new(SendErrorClass::Retryable, message));
            }
            let delay = backoff.max(throttle);
            if !self.config.retry.max_elapsed_time.is_zero()
                && elapsed + delay > self.config.retry.max_elapsed_time
            {
                return Err(SendError::new(
                    SendErrorClass::RetryExhausted,
                    format!("max retry time would elapse: {message}"),
                ));
            }
            self.timer.sleep(delay);
            elapsed += delay;
            backoff = next_backoff(backoff, self.config.retry.max_interval);
        }
    }
}

/// Exponential backoff (`Multiplier: 1.5`) without the SDK's randomization so
/// retry sequences are deterministic in tests.
fn next_backoff(current: Duration, max: Duration) -> Duration {
    let next = current.mul_f64(1.5);
    if next > max {
        max
    } else {
        next
    }
}

impl SpanTransport for HttpSpanTransport {
    fn send(&self, spans: &[SpanData]) -> Result<(), String> {
        for span in spans {
            let request = crate::trace_policy::request_from_span_data(span);
            self.send_request_detailed(&request)
                .map_err(|error| error.message)?;
        }
        Ok(())
    }

    fn send_request(&self, request: &ExportTraceServiceRequest) -> Result<(), String> {
        self.send_request_detailed(request)
            .map_err(|error| error.message)
    }
}

// ---------------------------------------------------------------------------
// Batch processor semantics (`sdk/trace.BatchSpanProcessor`)
// ---------------------------------------------------------------------------

/// `sdk/trace.DefaultMaxQueueSize`.
pub const DEFAULT_MAX_QUEUE_SIZE: usize = 2048;
/// `sdk/trace.DefaultMaxExportBatchSize`.
pub const DEFAULT_MAX_EXPORT_BATCH_SIZE: usize = 512;
/// `sdk/trace.DefaultScheduleDelay` (milliseconds).
pub const DEFAULT_SCHEDULE_DELAY_MS: u64 = 5000;
/// `sdk/trace.DefaultExportTimeout` (milliseconds).
pub const DEFAULT_EXPORT_TIMEOUT_MS: u64 = 30000;

/// The `OTEL_BSP_*` configuration read by `NewBatchSpanProcessor`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchConfig {
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
    pub schedule_delay: Duration,
    pub export_timeout: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_queue_size: DEFAULT_MAX_QUEUE_SIZE,
            max_export_batch_size: DEFAULT_MAX_EXPORT_BATCH_SIZE,
            schedule_delay: Duration::from_millis(DEFAULT_SCHEDULE_DELAY_MS),
            export_timeout: Duration::from_millis(DEFAULT_EXPORT_TIMEOUT_MS),
        }
    }
}

impl BatchConfig {
    /// Read `OTEL_BSP_*` like `sdk/internal/env`; invalid values keep defaults.
    /// The batch size is clamped exactly like `NewBatchSpanProcessor`.
    pub fn from_env(env: impl Fn(&str) -> Option<String>) -> Self {
        let mut config = Self::default();
        if let Some(value) = env("OTEL_BSP_MAX_QUEUE_SIZE").and_then(|v| v.parse().ok()) {
            config.max_queue_size = value;
        }
        if let Some(value) = env("OTEL_BSP_MAX_EXPORT_BATCH_SIZE").and_then(|v| v.parse().ok()) {
            config.max_export_batch_size = value;
        }
        if let Some(value) = env("OTEL_BSP_SCHEDULE_DELAY").and_then(|v| v.parse().ok()) {
            config.schedule_delay = Duration::from_millis(value);
        }
        if let Some(value) = env("OTEL_BSP_EXPORT_TIMEOUT").and_then(|v| v.parse().ok()) {
            config.export_timeout = Duration::from_millis(value);
        }
        if config.max_export_batch_size > config.max_queue_size {
            config.max_export_batch_size = if DEFAULT_MAX_EXPORT_BATCH_SIZE > config.max_queue_size
            {
                config.max_queue_size
            } else {
                DEFAULT_MAX_EXPORT_BATCH_SIZE
            };
        }
        config
    }
}

/// A synchronous model of the batch processor's queue: `push` returns a full
/// batch, `flush` drains, and a full queue drops spans like `OnEnd`.
#[derive(Debug)]
pub struct SpanBatcher {
    config: BatchConfig,
    pending: Vec<WireSpan>,
    dropped: u64,
}

impl SpanBatcher {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            pending: Vec::new(),
            dropped: 0,
        }
    }

    pub fn push(&mut self, span: WireSpan) -> Option<Vec<WireSpan>> {
        if self.pending.len() >= self.config.max_queue_size {
            self.dropped += 1;
            return None;
        }
        self.pending.push(span);
        if self.pending.len() >= self.config.max_export_batch_size {
            Some(std::mem::take(&mut self.pending))
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Option<Vec<WireSpan>> {
        if self.pending.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.pending))
        }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> ExportTraceServiceRequest {
        build_request(
            Some(&Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_owned(),
                    value: AnyValue::String("frontend".to_owned()),
                }],
                ..Resource::default()
            }),
            Some(&InstrumentationScope {
                name: "sg".to_owned(),
                version: "1.0".to_owned(),
                ..InstrumentationScope::default()
            }),
            &[WireSpan {
                trace_id: [1; 16],
                span_id: [2; 8],
                name: "span".to_owned(),
                kind: SpanKind::Server,
                start_time_unix_nano: 1,
                status: Some(Status::default()),
                flags: 1,
                ..WireSpan::default()
            }],
        )
    }

    #[test]
    fn protobuf_matches_proto3_wire_shape() {
        let bytes = encode_protobuf(&sample_request());
        // Empty status is still emitted (non-nil message): tag 0x7a, len 0.
        assert!(bytes.windows(2).any(|window| window == [0x7a, 0x00]));
        // Trace/span ids are fixed-size byte fields (16/8 bytes).
        assert!(bytes.windows(2).any(|window| window == [0x0a, 0x10]));
        assert!(bytes.windows(2).any(|window| window == [0x12, 0x08]));
    }

    #[test]
    fn json_matches_protojson_shape() {
        let json = encode_json(&sample_request());
        assert!(json.starts_with("{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"frontend\"}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"sg\",\"version\":\"1.0\"},\"spans\":[{\"traceId\":\"AQEBAQEBAQEBAQEBAQEBAQ==\",\"spanId\":\"AgICAgICAgI=\",\"flags\":1,\"name\":\"span\",\"kind\":\"SPAN_KIND_SERVER\",\"startTimeUnixNano\":\"1\",\"status\":{}}]}]}]}"));
    }

    #[test]
    fn empty_request_encodes_like_empty_messages() {
        assert!(encode_protobuf(&ExportTraceServiceRequest::default()).is_empty());
        assert_eq!(encode_json(&ExportTraceServiceRequest::default()), "{}");
    }

    #[test]
    fn json_escapes_like_protojson() {
        let mut out = String::new();
        json_string(&mut out, "a\"b\\c\nd\te<f>g&h\u{1}i\u{7f}");
        assert_eq!(out, "\"a\\\"b\\\\c\\nd\\te<f>g&h\\u0001i\u{7f}\"");
    }

    #[test]
    fn traces_url_appends_the_traces_path() {
        assert_eq!(
            traces_url("127.0.0.1:4317", true).unwrap(),
            "http://127.0.0.1:4317/v1/traces"
        );
        assert_eq!(
            traces_url("collector:4318", false).unwrap(),
            "https://collector:4318/v1/traces"
        );
        assert_eq!(
            traces_url("collector:4318/base/", false).unwrap(),
            "https://collector:4318/base/v1/traces"
        );
        assert!(traces_url("", false).is_err());
    }

    struct ScriptedClient {
        responses: std::sync::Mutex<Vec<Result<HttpResponse, HttpError>>>,
        requests: std::sync::Mutex<Vec<HttpRequest>>,
    }

    impl ScriptedClient {
        fn new(responses: Vec<Result<HttpResponse, HttpError>>) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses),
                requests: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl HttpClient for ScriptedClient {
        fn execute(&self, request: &HttpRequest) -> Result<HttpResponse, HttpError> {
            self.requests.lock().unwrap().push(request.clone());
            let mut responses = self.responses.lock().unwrap();
            if responses.len() > 1 {
                responses.remove(0)
            } else {
                responses.first().cloned().unwrap_or_else(|| {
                    Ok(HttpResponse {
                        status: 200,
                        headers: Vec::new(),
                        body: Vec::new(),
                    })
                })
            }
        }
    }

    struct NoSleep;

    impl RetryTimer for NoSleep {
        fn sleep(&self, _delay: Duration) {}
    }

    fn response(status: u16) -> HttpResponse {
        HttpResponse {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    #[test]
    fn retries_retryable_statuses_and_forces_headers() {
        let mut config = OtlpHttpConfig::new("127.0.0.1:4318", true, HttpWireFormat::Protobuf);
        config.headers = vec![("X-Test".to_owned(), "1".to_owned())];
        config.retry.initial_interval = Duration::from_millis(1);
        config.retry.max_interval = Duration::from_millis(1);
        let client = Arc::new(ScriptedClient::new(vec![
            Ok(response(503)),
            Ok(response(200)),
        ]));
        let transport = HttpSpanTransport::with_timer(config, client.clone(), Arc::new(NoSleep));
        transport.send_request_detailed(&sample_request()).unwrap();
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, "http://127.0.0.1:4318/v1/traces");
        assert_eq!(requests[0].method, "POST");
        assert!(requests[0]
            .headers
            .contains(&("X-Test".to_owned(), "1".to_owned())));
        assert!(requests[0]
            .headers
            .contains(&("Content-Type".to_owned(), CONTENT_TYPE_PROTOBUF.to_owned())));
        assert!(requests[0]
            .headers
            .contains(&("User-Agent".to_owned(), OTEL_USER_AGENT.to_owned())));
    }

    #[test]
    fn non_retryable_status_and_timeout_do_not_retry() {
        let config = OtlpHttpConfig::new("127.0.0.1:4318", true, HttpWireFormat::Protobuf);
        let client = Arc::new(ScriptedClient::new(vec![Ok(response(400))]));
        let transport =
            HttpSpanTransport::with_timer(config.clone(), client.clone(), Arc::new(NoSleep));
        assert_eq!(
            transport
                .send_request_detailed(&sample_request())
                .unwrap_err()
                .class,
            SendErrorClass::Status
        );
        assert_eq!(client.requests.lock().unwrap().len(), 1);

        // `otlptracehttp` wraps a client timeout in `retryableError`; with
        // retries disabled that error is returned unchanged.
        let client = Arc::new(ScriptedClient::new(vec![Err(HttpError::Timeout(
            "deadline".to_owned(),
        ))]));
        let mut timeout_config = config;
        timeout_config.retry.enabled = false;
        let transport =
            HttpSpanTransport::with_timer(timeout_config, client.clone(), Arc::new(NoSleep));
        assert_eq!(
            transport
                .send_request_detailed(&sample_request())
                .unwrap_err()
                .class,
            SendErrorClass::Retryable
        );
        assert_eq!(client.requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn retry_exhaustion_reports_the_class() {
        let mut config = OtlpHttpConfig::new("127.0.0.1:4318", true, HttpWireFormat::Protobuf);
        config.retry.initial_interval = Duration::from_millis(4);
        config.retry.max_interval = Duration::from_millis(4);
        config.retry.max_elapsed_time = Duration::from_millis(10);
        let client = Arc::new(ScriptedClient::new(vec![Ok(response(503))]));
        let transport = HttpSpanTransport::with_timer(config, client.clone(), Arc::new(NoSleep));
        assert_eq!(
            transport
                .send_request_detailed(&sample_request())
                .unwrap_err()
                .class,
            SendErrorClass::RetryExhausted
        );
        // Delays of 4ms + 4ms fit the 10ms budget; the third 4ms does not.
        assert_eq!(client.requests.lock().unwrap().len(), 3);
    }

    #[test]
    fn batch_config_defaults_and_env_overrides() {
        let config = BatchConfig::from_env(|_| None);
        assert_eq!(config.max_queue_size, 2048);
        assert_eq!(config.max_export_batch_size, 512);
        assert_eq!(config.schedule_delay, Duration::from_millis(5000));
        assert_eq!(config.export_timeout, Duration::from_millis(30000));

        let config = BatchConfig::from_env(|key| match key {
            "OTEL_BSP_MAX_QUEUE_SIZE" => Some("16".to_owned()),
            "OTEL_BSP_MAX_EXPORT_BATCH_SIZE" => Some("64".to_owned()),
            _ => None,
        });
        // The SDK clamps the batch size to the queue size.
        assert_eq!(config.max_export_batch_size, 16);
    }

    #[test]
    fn batcher_splits_and_drops_when_full() {
        let mut batcher = SpanBatcher::new(BatchConfig {
            max_queue_size: 4,
            max_export_batch_size: 2,
            ..BatchConfig::default()
        });
        assert!(batcher.push(WireSpan::default()).is_none());
        let batch = batcher.push(WireSpan::default()).unwrap();
        assert_eq!(batch.len(), 2);
        assert!(batcher.is_empty());

        // A directly-constructed queue smaller than the batch size drops.
        let mut batcher = SpanBatcher::new(BatchConfig {
            max_queue_size: 1,
            max_export_batch_size: 2,
            ..BatchConfig::default()
        });
        assert!(batcher.push(WireSpan::default()).is_none());
        assert!(batcher.push(WireSpan::default()).is_none());
        assert_eq!(batcher.len(), 1);
        assert_eq!(batcher.dropped(), 1);
        assert_eq!(batcher.flush().unwrap().len(), 1);
        assert!(batcher.flush().is_none());
    }
}
