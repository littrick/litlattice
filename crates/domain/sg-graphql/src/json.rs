#![forbid(unsafe_code)]

//! Insertion-ordered JSON DOM and Go `encoding/json`-compatible writers,
//! shared by the Relay introspection JSON (`Schema::to_json`) and the GraphQL
//! response serializer.
//!
//! graph-gophers keeps the selection order of `json.RawMessage` and marshals
//! with `encoding/json`, so neither keys nor values may be reordered, and
//! strings use Go's HTML escaping (`<`, `>`, `&`, U+2028/U+2029).

/// An insertion-ordered JSON value.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    /// A pre-serialized JSON fragment, embedded verbatim (Go
    /// `json.RawMessage`). The caller is responsible for valid, escaped JSON.
    Raw(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    /// Convert a `serde_json` value. Object keys keep `serde_json::Map`'s
    /// iteration order; with `preserve_order` disabled this is the sorted
    /// order Go uses for maps.
    pub fn from_serde(value: &serde_json::Value) -> Result<Json, String> {
        Ok(match value {
            serde_json::Value::Null => Json::Null,
            serde_json::Value::Bool(value) => Json::Bool(*value),
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    Json::Int(value)
                } else if let Some(value) = number.as_u64() {
                    Json::UInt(value)
                } else {
                    Json::Float(number.as_f64().expect("finite serde_json number"))
                }
            }
            serde_json::Value::String(value) => Json::Str(value.clone()),
            serde_json::Value::Array(items) => Json::Arr(
                items
                    .iter()
                    .map(Json::from_serde)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            serde_json::Value::Object(entries) => Json::Obj(
                entries
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), Json::from_serde(value)?)))
                    .collect::<Result<Vec<_>, String>>()?,
            ),
        })
    }

    /// `encoding/json` rejects NaN and infinities.
    pub fn check_finite(&self) -> Result<(), String> {
        match self {
            Json::Float(value) if !value.is_finite() => Err(format!(
                "json: unsupported value: {}",
                if value.is_nan() {
                    "NaN"
                } else if value.is_sign_positive() {
                    "+Inf"
                } else {
                    "-Inf"
                }
            )),
            Json::Arr(items) => items.iter().try_for_each(Json::check_finite),
            Json::Obj(entries) => entries
                .iter()
                .try_for_each(|(_, value)| value.check_finite()),
            _ => Ok(()),
        }
    }

    pub fn obj(entries: Vec<(&str, Json)>) -> Json {
        Json::Obj(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        )
    }

    pub fn opt_str(value: Option<&str>) -> Json {
        match value.filter(|value| !value.is_empty()) {
            Some(value) => Json::Str(value.to_owned()),
            None => Json::Null,
        }
    }

    /// `json.Marshal`: no whitespace, keys in insertion order.
    pub fn to_compact_string(&self) -> String {
        let mut out = String::new();
        write_json(self, &mut out, WriterMode::Compact, 0);
        out
    }

    /// `json.MarshalIndent(v, "", "\t")`.
    pub fn to_indented_string(&self) -> String {
        let mut out = String::new();
        write_json(self, &mut out, WriterMode::Indented, 0);
        out
    }
}

#[derive(Clone, Copy)]
enum WriterMode {
    Compact,
    Indented,
}

/// Serialize like Go's `json.Marshal`/`json.MarshalIndent(v, "", "\t")`.
fn write_json(value: &Json, out: &mut String, mode: WriterMode, depth: usize) {
    let indented = matches!(mode, WriterMode::Indented);
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        Json::Int(value) => out.push_str(&value.to_string()),
        Json::UInt(value) => out.push_str(&value.to_string()),
        Json::Float(value) => write_go_float(*value, out),
        Json::Str(value) => write_go_string(value, out),
        Json::Raw(value) => out.push_str(value),
        Json::Arr(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            if indented {
                out.push_str("[\n");
            } else {
                out.push('[');
            }
            for (index, item) in items.iter().enumerate() {
                if indented {
                    indent(out, depth + 1);
                }
                write_json(item, out, mode, depth + 1);
                if index + 1 < items.len() {
                    out.push(',');
                }
                if indented {
                    out.push('\n');
                }
            }
            if indented {
                indent(out, depth);
            }
            out.push(']');
        }
        Json::Obj(entries) => {
            if entries.is_empty() {
                out.push_str("{}");
                return;
            }
            if indented {
                out.push_str("{\n");
            } else {
                out.push('{');
            }
            for (index, (key, item)) in entries.iter().enumerate() {
                if indented {
                    indent(out, depth + 1);
                }
                write_go_string(key, out);
                out.push(':');
                if indented {
                    out.push(' ');
                }
                write_json(item, out, mode, depth + 1);
                if index + 1 < entries.len() {
                    out.push(',');
                }
                if indented {
                    out.push('\n');
                }
            }
            if indented {
                indent(out, depth);
            }
            out.push('}');
        }
    }
}

fn indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push('\t');
    }
}

/// Go `encoding/json` float formatting (`encode.go:floatEncoder`):
/// `'f'` for `1e-6 <= |v| < 1e21` (or zero), `'e'` otherwise, shortest
/// round-trip digits, and Go's exponent spelling (`1e+21`, `1e-7`).
fn write_go_float(value: f64, out: &mut String) {
    debug_assert!(value.is_finite());
    let abs = value.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let formatted = format!("{value:e}");
        match formatted.split_once('e') {
            Some((mantissa, exponent)) if !exponent.starts_with('-') => {
                out.push_str(mantissa);
                out.push_str("e+");
                out.push_str(exponent);
            }
            _ => out.push_str(&formatted),
        }
    } else {
        out.push_str(&format!("{value}"));
    }
}

/// Go `encoding/json` string escaping with HTML escaping enabled.
pub fn write_go_string(value: &str, out: &mut String) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_and_indented_match_encoding_json() {
        let value = Json::obj(vec![
            ("a", Json::Str("x<y".to_owned())),
            ("b", Json::Arr(vec![Json::Bool(true), Json::Null])),
            ("c", Json::obj(vec![])),
        ]);
        assert_eq!(
            value.to_compact_string(),
            r#"{"a":"x\u003cy","b":[true,null],"c":{}}"#
        );
        assert_eq!(
            value.to_indented_string(),
            "{\n\t\"a\": \"x\\u003cy\",\n\t\"b\": [\n\t\ttrue,\n\t\tnull\n\t],\n\t\"c\": {}\n}"
        );
    }

    #[test]
    fn floats_match_encoding_json() {
        // Expected values from `json.Marshal(float64)` (Go 1.x).
        let cases = [
            (0.0, "0"),
            (1.5, "1.5"),
            (-2.25, "-2.25"),
            (1e-6, "0.000001"),
            (9.999e-7, "9.999e-7"),
            (1e-7, "1e-7"),
            (1.23e-7, "1.23e-7"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1.23e21, "1.23e+21"),
            (1e300, "1e+300"),
            (-1e-300, "-1e-300"),
            (1.0 / 3.0, "0.3333333333333333"),
        ];
        for (value, expected) in cases {
            assert_eq!(Json::Float(value).to_compact_string(), expected);
        }
    }

    #[test]
    fn raw_and_serde_conversion() {
        let serde = serde_json::json!({"b": 2, "a": [true, "x<y"], "c": 18446744073709551615u64});
        let json = Json::from_serde(&serde).unwrap();
        assert_eq!(
            json.to_compact_string(),
            r#"{"a":[true,"x\u003cy"],"b":2,"c":18446744073709551615}"#
        );
        assert_eq!(
            Json::obj(vec![("data", Json::Raw("{\"x\":1}".to_owned()))]).to_compact_string(),
            r#"{"data":{"x":1}}"#
        );
        assert!(
            Json::Float(f64::NAN).check_finite().is_err(),
            "NaN must be rejected"
        );
    }
}
