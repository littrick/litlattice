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
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
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
        Json::Str(value) => write_go_string(value, out),
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
}
