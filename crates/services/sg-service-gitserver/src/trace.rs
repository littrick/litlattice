#![forbid(unsafe_code)]

//! `X-Trace*` response headers.
//!
//! The baseline wraps every plain-HTTP handler in
//! `internal/instrumentation.HTTPMiddleware` (which sets `X-Trace` and
//! `X-Trace-Span` from the request's OpenTelemetry span) and
//! `internal/trace.HTTPMiddleware` (which sets `X-Trace-URL` from the site
//! config; the header is present with an empty value when no trace URL
//! template is configured). The span continues a `traceparent` header when
//! the client sends one.
//!
//! gRPC responses do not carry these headers in the baseline (verified
//! against the Go server), so only the plain-HTTP branch is instrumented.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use http::{HeaderMap, HeaderValue};

/// Trace and span IDs for one request.
pub struct TraceIds {
    trace_id: [u8; 16],
    span_id: [u8; 8],
}

impl TraceIds {
    /// Continue a valid W3C `traceparent` trace, otherwise start a new one.
    /// The span is always new, like `otelhttp` creating a child span.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let trace_id = headers
            .get("traceparent")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_traceparent)
            .unwrap_or_else(random_trace_id);
        Self {
            trace_id,
            span_id: random_span_id(),
        }
    }

    /// Insert `X-Trace`, `X-Trace-Span` and the (empty) `X-Trace-URL`.
    pub fn apply(&self, headers: &mut HeaderMap) {
        headers.insert("x-trace", header_value(&hex(&self.trace_id)));
        headers.insert("x-trace-span", header_value(&hex(&self.span_id)));
        headers.insert("x-trace-url", HeaderValue::from_static(""));
    }
}

fn header_value(value: &str) -> HeaderValue {
    HeaderValue::from_str(value).expect("hex IDs are valid header values")
}

/// Extract the trace ID from a W3C `traceparent` value.
///
/// Follows the OpenTelemetry propagator: two hex version digits (not `ff`),
/// 32 hex trace-ID digits, 16 hex parent-ID digits and two hex flags. All-zero
/// trace and parent IDs are invalid. Version `00` must not carry extra fields.
fn parse_traceparent(value: &str) -> Option<[u8; 16]> {
    let mut parts = value.split('-');
    let version = parts.next()?;
    let trace = parts.next()?;
    let parent = parts.next()?;
    let flags = parts.next()?;
    if version.len() != 2
        || trace.len() != 32
        || parent.len() != 16
        || flags.len() != 2
        || !is_hex(version)
        || !is_hex(trace)
        || !is_hex(parent)
        || !is_hex(flags)
        || version.eq_ignore_ascii_case("ff")
    {
        return None;
    }
    if version == "00" && parts.next().is_some() {
        return None;
    }
    let trace_id = decode_hex_array::<16>(trace)?;
    let parent_id = decode_hex_array::<8>(parent)?;
    if trace_id == [0; 16] || parent_id == [0; 8] {
        return None;
    }
    Some(trace_id)
}

fn is_hex(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn decode_hex_array<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        out[index] = (high * 16 + low) as u8;
    }
    Some(out)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// splitmix64 over a process-local counter: unique enough for per-request IDs
/// without pulling in an RNG dependency.
fn next_u64() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(1);
    let mut z = nanos
        ^ ((u64::from(std::process::id())) << 32)
        ^ sequence.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn random_trace_id() -> [u8; 16] {
    loop {
        let high = next_u64().to_be_bytes();
        let low = next_u64().to_be_bytes();
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&high);
        id[8..].copy_from_slice(&low);
        if id != [0; 16] {
            return id;
        }
    }
}

fn random_span_id() -> [u8; 8] {
    loop {
        let id = next_u64().to_be_bytes();
        if id != [0; 8] {
            return id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", HeaderValue::from_str(value).expect("header"));
        headers
    }

    #[test]
    fn continues_a_valid_traceparent() {
        let ids = TraceIds::from_headers(&headers_with(
            "00-11111111111111111111111111111111-2222222222222222-01",
        ));
        assert_eq!(ids.trace_id, [0x11; 16]);
        assert_eq!(ids.span_id.len(), 8);
    }

    #[test]
    fn rejects_invalid_traceparents() {
        for value in [
            "garbage",
            "00-11111111111111111111111111111111-2222222222222222",
            "00-00000000000000000000000000000000-2222222222222222-01",
            "00-11111111111111111111111111111111-0000000000000000-01",
            "ff-11111111111111111111111111111111-2222222222222222-01",
            "00-11111111111111111111111111111111-2222222222222222-01-extra",
        ] {
            assert!(
                parse_traceparent(value).is_none(),
                "accepted invalid traceparent {value:?}"
            );
        }
    }

    #[test]
    fn starts_a_new_trace_without_a_valid_parent() {
        let first = TraceIds::from_headers(&headers_with("garbage"));
        let second = TraceIds::from_headers(&HeaderMap::new());
        assert_ne!(first.trace_id, [0; 16]);
        assert_ne!(first.trace_id, second.trace_id);
        assert_ne!(first.span_id, [0; 8]);
    }

    #[test]
    fn applies_the_response_headers() {
        let ids = TraceIds::from_headers(&headers_with(
            "00-11111111111111111111111111111111-2222222222222222-01",
        ));
        let mut headers = HeaderMap::new();
        ids.apply(&mut headers);
        assert_eq!(headers["x-trace"], "11111111111111111111111111111111");
        assert_eq!(headers["x-trace-span"].to_str().expect("span").len(), 16);
        assert_eq!(headers["x-trace-url"], "");
    }
}
