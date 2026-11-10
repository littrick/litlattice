//! `encoding/json`-compatible encoding helpers.
//!
//! The streaming search events are compared byte for byte with the Go
//! baseline, so this module reimplements the parts of `encoding/json` we rely
//! on: string escaping with HTML escaping enabled, `omitempty` for Go zero
//! values, and `time.Time`'s RFC3339Nano layout.

use time::{OffsetDateTime, UtcOffset};

/// `streamhttp.maxPayloadSize` (`internal/search/streaming/http/client.go`).
pub const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

/// Append a JSON string literal exactly like `encoding/json` with the default
/// HTML escaping (`<`, `>`, `&` become `<`, `>`, `&`).
pub fn push_quoted(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn push_key(out: &mut String, key: &str) {
    push_quoted(out, key);
    out.push(':');
}

fn separator(out: &mut String) {
    if !out.ends_with('{') && !out.ends_with('[') {
        out.push(',');
    }
}

/// Same as the private `separator`, for callers that write a key by hand.
pub fn separator_public(out: &mut String) {
    separator(out);
}

/// A field that is always emitted.
pub fn field_str(out: &mut String, key: &str, value: &str) {
    separator(out);
    push_key(out, key);
    push_quoted(out, value);
}

pub fn field_int(out: &mut String, key: &str, value: i64) {
    separator(out);
    push_key(out, key);
    out.push_str(&value.to_string());
}

pub fn field_bool(out: &mut String, key: &str, value: bool) {
    separator(out);
    push_key(out, key);
    out.push_str(if value { "true" } else { "false" });
}

/// `string` with `omitempty`: the empty string is omitted.
pub fn opt_str(out: &mut String, key: &str, value: &str) {
    if !value.is_empty() {
        field_str(out, key, value);
    }
}

/// `int` with `omitempty`: zero is omitted.
pub fn opt_int(out: &mut String, key: &str, value: i64) {
    if value != 0 {
        field_int(out, key, value);
    }
}

/// `bool` with `omitempty`: false is omitted.
pub fn opt_bool(out: &mut String, key: &str, value: bool) {
    if value {
        field_bool(out, key, value);
    }
}

/// `[]string` with `omitempty`: empty slices are omitted.
pub fn opt_strings(out: &mut String, key: &str, values: Option<&[String]>) {
    let Some(values) = values else { return };
    if values.is_empty() {
        return;
    }
    separator(out);
    push_key(out, key);
    out.push('[');
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_quoted(out, value);
    }
    out.push(']');
}

/// `time.Time`'s `MarshalJSON`: RFC3339 with nanoseconds (`RFC3339Nano`).
pub fn push_time(out: &mut String, value: OffsetDateTime) {
    let value = value.to_offset(UtcOffset::UTC);
    out.push('"');
    out.push_str(&format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    ));
    let nanos = value.nanosecond();
    if nanos != 0 {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        out.push('.');
        out.push_str(&fraction);
    }
    out.push_str("Z\"");
}

/// `*time.Time` with `omitempty`: nil is omitted.
pub fn opt_time(out: &mut String, key: &str, value: Option<OffsetDateTime>) {
    let Some(value) = value else { return };
    separator(out);
    push_key(out, key);
    push_time(out, value);
}

/// `map[string]*string` with `omitempty`; Go marshals map keys sorted.
pub fn opt_string_map(
    out: &mut String,
    key: &str,
    value: Option<&std::collections::BTreeMap<String, Option<String>>>,
) {
    let Some(value) = value else { return };
    if value.is_empty() {
        return;
    }
    separator(out);
    push_key(out, key);
    out.push('{');
    for (i, (k, v)) in value.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_key(out, k);
        match v {
            Some(v) => push_quoted(out, v),
            None => out.push_str("null"),
        }
    }
    out.push('}');
}

/// Go `%q` for strings (double-quoted Go string literal).
pub fn go_quote(value: &str) -> String {
    let mut out = String::new();
    push_quoted(&mut out, value);
    out
}
