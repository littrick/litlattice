#![forbid(unsafe_code)]

//! `internal/search/job/printer` (`json.go`, `sexp.go`, `mermaid.go`,
//! `encoder.go`).
//!
//! The printer walks a [`SearchJob`] via `Name`/`Children`/`Attributes`.
//! `Attributes` is already normalized by [`crate::search_job`] to
//! `(key, Emit text)` pairs; [`typed_attributes`] restores the OpenTelemetry
//! value type (bool / int64 / string / string slice) from the attribute key so
//! the JSON printer can reproduce `attribute.Value.AsInterface`, while SEXP
//! and Mermaid reproduce `attribute.Value.Emit` exactly.
//!
//! See `docs/overviews/m9_8ht_parse_search_query_jobtree_overview.md`.

use std::collections::BTreeMap;

use crate::search_job::{SearchJob, Verbosity};

// ---------------------------------------------------------------------------
// Attribute values
// ---------------------------------------------------------------------------

/// The OpenTelemetry `attribute.Value` types used by search jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AttrValue {
    Str(String),
    Bool(bool),
    Int(i64),
    StringSlice(Vec<String>),
}

impl AttrValue {
    /// `attribute.Value.AsInterface` serialized to JSON.
    fn json_value(&self) -> Json {
        match self {
            AttrValue::Str(value) => Json::Str(value.clone()),
            AttrValue::Bool(value) => Json::Bool(*value),
            AttrValue::Int(value) => Json::Int(*value),
            AttrValue::StringSlice(values) => {
                Json::Array(values.iter().map(|v| Json::Str(v.clone())).collect())
            }
        }
    }

    /// `attribute.Value.Emit` (`attribute/value.go:226-258`).
    fn emit(&self) -> String {
        match self {
            AttrValue::Str(value) => value.clone(),
            AttrValue::Bool(value) => {
                if *value {
                    "true".to_owned()
                } else {
                    "false".to_owned()
                }
            }
            AttrValue::Int(value) => value.to_string(),
            AttrValue::StringSlice(values) => go_json_string_slice(values),
        }
    }
}

#[derive(Debug, Clone)]
struct PrintAttribute {
    key: String,
    value: AttrValue,
}

/// Attribute keys whose OTel value is an int64.
const INT_KEYS: &[&str] = &["limit", "numRepos", "fileMatchLimit"];

/// Attribute keys whose OTel value is a bool.
const BOOL_KEYS: &[&str] = &[
    "ensureUnique",
    "indexed",
    "isCaseSensitive",
    "isStructural",
    "pathPatternsAreCaseSensitive",
    "patternMatchesPath",
    "diff",
    "caseSensitiveRepoFilters",
    "forkSet",
    "noForks",
    "onlyForks",
    "onlyCloned",
    "archivedSet",
    "noArchived",
    "onlyArchived",
    "negated",
];

/// Attribute keys whose OTel value is a `[]string`.
const STRING_SLICE_KEYS: &[&str] = &[
    "minusRepoFilters",
    "descriptionPatterns",
    "select",
    "includePatterns",
    "includeLangs",
    "excludeLangs",
    "originalPatterns",
    "filterPatterns",
    "includeOwners",
    "excludeOwners",
    "includeContributors",
    "excludeContributors",
    "languages",
    "repoNamePatterns",
];

/// The last dot-separated segment of an attribute key (`repoOpts.limit` ->
/// `limit`, `repoOpts.commitAfter.negated` -> `negated`).
fn key_tail(key: &str) -> &str {
    key.rsplit('.').next().unwrap_or(key)
}

/// Restore the value types of the normalized `(key, Emit text)` pairs.
fn typed_attributes(attributes: Vec<(String, String)>) -> Vec<PrintAttribute> {
    attributes
        .into_iter()
        .map(|(key, raw)| {
            let value = if BOOL_KEYS.contains(&key_tail(&key)) {
                match raw.as_str() {
                    "true" => AttrValue::Bool(true),
                    "false" => AttrValue::Bool(false),
                    _ => AttrValue::Str(raw),
                }
            } else if INT_KEYS.contains(&key_tail(&key)) {
                match raw.parse::<i64>() {
                    Ok(value) => AttrValue::Int(value),
                    Err(_) => AttrValue::Str(raw),
                }
            } else if STRING_SLICE_KEYS.contains(&key_tail(&key)) {
                match serde_json::from_str::<Vec<String>>(&raw) {
                    Ok(values) => AttrValue::StringSlice(values),
                    Err(_) => AttrValue::Str(raw),
                }
            } else {
                AttrValue::Str(raw)
            };
            PrintAttribute { key, value }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tree
// ---------------------------------------------------------------------------

struct PrintNode {
    name: String,
    attributes: Vec<PrintAttribute>,
    children: Vec<PrintNode>,
}

fn build<T: SearchJob + ?Sized>(job: &T, verbosity: Verbosity) -> PrintNode {
    PrintNode {
        name: job.name().to_owned(),
        attributes: typed_attributes(job.attributes(verbosity)),
        children: job
            .children()
            .into_iter()
            .map(|child| build(child, verbosity))
            .collect(),
    }
}

/// `printer.trimmedUpperName` (`encoder.go:7-9`).
fn trimmed_upper_name(name: &str) -> String {
    name.strip_suffix("Job").unwrap_or(name).to_uppercase()
}

// ---------------------------------------------------------------------------
// JSON (`printer/json.go`)
// ---------------------------------------------------------------------------

/// `printer.JSONVerbose` (`json.go:17-24`).
pub fn json_verbose<T: SearchJob + ?Sized>(job: &T, verbosity: Verbosity) -> String {
    let node = build(job, verbosity);
    let value = node_to_json(&node);
    let mut out = String::new();
    write_json(&value, 0, &mut out);
    out
}

/// `printer.JSON` (`json.go:13-15`).
pub fn json<T: SearchJob + ?Sized>(job: &T) -> String {
    json_verbose(job, Verbosity::None)
}

/// `node.MarshalJSON` (`json.go:53-60`).
fn node_to_json(node: &PrintNode) -> Json {
    if node.attributes.is_empty() && node.children.is_empty() {
        return Json::Str(node.name.clone());
    }
    let mut object = BTreeMap::new();
    object.insert(node.name.clone(), Json::Object(node_params(node)));
    Json::Object(object)
}

/// `node.params` (`json.go:32-51`), including the duplicate-child-key quirk.
fn node_params(node: &PrintNode) -> BTreeMap<String, Json> {
    let mut map = BTreeMap::new();
    for attribute in &node.attributes {
        map.insert(attribute.key.clone(), attribute.value.json_value());
    }
    let mut seen: BTreeMap<String, i64> = BTreeMap::new();
    for child in &node.children {
        let mut key = child.name.clone();
        if let Some(&seen_count) = seen.get(&key) {
            if seen_count == 1 {
                let previous = map.remove(&key).unwrap_or(Json::Null);
                map.insert(format!("{key}.0"), previous);
            }
            key = format!("{key}.{seen_count}");
        }
        map.insert(key.clone(), Json::Object(node_params(child)));
        *seen.entry(key).or_insert(0) += 1;
    }
    map
}

/// A JSON value with Go's `encoding/json` semantics (sorted object keys,
/// integer numbers, HTML-escaped strings).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Str(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}

fn write_json(value: &Json, indent: usize, out: &mut String) {
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Int(number) => out.push_str(&number.to_string()),
        Json::Str(text) => push_go_quoted(text, out),
        Json::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push_str("[\n");
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(",\n");
                }
                push_indent(indent + 1, out);
                write_json(item, indent + 1, out);
            }
            out.push('\n');
            push_indent(indent, out);
            out.push(']');
        }
        Json::Object(fields) => {
            if fields.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push_str("{\n");
            for (index, (key, item)) in fields.iter().enumerate() {
                if index > 0 {
                    out.push_str(",\n");
                }
                push_indent(indent + 1, out);
                push_go_quoted(key, out);
                out.push_str(": ");
                write_json(item, indent + 1, out);
            }
            out.push('\n');
            push_indent(indent, out);
            out.push('}');
        }
    }
}

fn push_indent(indent: usize, out: &mut String) {
    for _ in 0..indent {
        out.push_str("  ");
    }
}

/// `encoding/json`'s default (HTML-escaping) string encoder.
fn push_go_quoted(text: &str, out: &mut String) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn go_json_string_slice(values: &[String]) -> String {
    let mut out = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_go_quoted(value, &mut out);
    }
    out.push(']');
    out
}

// ---------------------------------------------------------------------------
// SEXP (`printer/sexp.go`)
// ---------------------------------------------------------------------------

/// `printer.SexpVerbose` (`sexp.go:21-27`).
pub fn sexp_verbose<T: SearchJob + ?Sized>(job: &T, verbosity: Verbosity, pretty: bool) -> String {
    let node = build(job, verbosity);
    let (separator, indent) = if pretty { ("\n", "  ") } else { (" ", "") };
    let mut out = String::new();
    write_sexp(&node, separator, indent, 0, &mut out);
    out
}

/// `printer.Sexp` (`sexp.go:11-13`).
pub fn sexp<T: SearchJob + ?Sized>(job: &T) -> String {
    sexp_verbose(job, Verbosity::None, false)
}

/// `printer.SexpPretty` (`sexp.go:16-18`).
pub fn sexp_pretty<T: SearchJob + ?Sized>(job: &T) -> String {
    sexp_verbose(job, Verbosity::Basic, true)
}

/// `printer.SexpFormat` (`sexp.go:29-66`).
fn write_sexp(node: &PrintNode, separator: &str, indent: &str, depth: usize, out: &mut String) {
    if node.attributes.is_empty() && node.children.is_empty() {
        out.push_str(&trimmed_upper_name(&node.name));
        return;
    }

    out.push('(');
    out.push_str(&trimmed_upper_name(&node.name));
    let depth = depth + 1;
    for attribute in &node.attributes {
        write_separator(separator, indent, depth, out);
        out.push('(');
        out.push_str(&attribute.key);
        out.push_str(" . ");
        out.push_str(&attribute.value.emit());
        out.push(')');
    }
    for child in &node.children {
        write_separator(separator, indent, depth, out);
        write_sexp(child, separator, indent, depth, out);
    }
    out.push(')');
}

/// `printer.writeSep` (`sexp.go:68-76`, shared with Mermaid).
fn write_separator(separator: &str, indent: &str, depth: usize, out: &mut String) {
    out.push_str(separator);
    if indent.is_empty() {
        return;
    }
    for _ in 0..depth {
        out.push_str(indent);
    }
}

// ---------------------------------------------------------------------------
// Mermaid (`printer/mermaid.go`)
// ---------------------------------------------------------------------------

/// `printer.MermaidVerbose` (`mermaid.go:17-39`).
pub fn mermaid_verbose<T: SearchJob + ?Sized>(job: &T, verbosity: Verbosity) -> String {
    let node = build(job, verbosity);
    let mut out = String::from("flowchart TB\n");
    let mut id: i64 = 0;
    write_mermaid(&node, 0, &mut id, &mut out);
    out
}

/// `printer.Mermaid` (`mermaid.go:13-15`).
pub fn mermaid<T: SearchJob + ?Sized>(job: &T) -> String {
    mermaid_verbose(job, Verbosity::None)
}

fn write_mermaid(node: &PrintNode, depth: usize, id: &mut i64, out: &mut String) {
    let source_id = *id;
    let depth = depth + 1;
    write_mermaid_node(node, depth, id, out);
    for child in &node.children {
        write_mermaid_edge(depth, source_id, *id, out);
        write_mermaid(child, depth, id, out);
    }
}

/// `printer.writeEdge` (`mermaid.go:48-53`).
fn write_mermaid_edge(depth: usize, source: i64, destination: i64, out: &mut String) {
    out.push_str(&source.to_string());
    out.push_str("---");
    out.push_str(&destination.to_string());
    write_separator("\n", "  ", depth, out);
}

/// `printer.writeNode` (`mermaid.go:55-68`), `DefaultStyle`.
fn write_mermaid_node(node: &PrintNode, depth: usize, id: &mut i64, out: &mut String) {
    out.push_str(&id.to_string());
    out.push('[');
    out.push_str(&build_label(node));
    out.push(']');
    write_separator("\n", "  ", depth, out);
    *id += 1;
}

/// `printer.buildLabel` (`mermaid.go:70-80`).
fn build_label(node: &PrintNode) -> String {
    let mut out = String::from("\"");
    out.push_str(&trimmed_upper_name(&node.name));
    for attribute in &node.attributes {
        out.push_str(" <br> ");
        out.push_str(&mermaid_escape(&attribute.key));
        out.push_str(": ");
        out.push_str(&mermaid_escape(&attribute.value.emit()));
    }
    out.push('"');
    out
}

/// `printer.mermaidEscaper` (`mermaid.go:91-98`).
fn mermaid_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '"' => out.push_str("#quot;"),
            '\'' => out.push_str("#apos;"),
            '&' => out.push_str("#amp;"),
            '<' => out.push_str("#lt;"),
            '>' => out.push_str("#gt;"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_job::JobError;
    use crate::search_stream::progress::SearchEvent;

    /// A minimal [`SearchJob`] used to exercise the printer without the full
    /// job tree.
    struct Node {
        name: &'static str,
        attributes: Vec<(String, String)>,
        children: Vec<Node>,
    }

    impl SearchJob for Node {
        fn run(
            &mut self,
            _ctx: &crate::search_job::JobContext,
            _clients: &crate::search_job::RuntimeClients<'_>,
            _stream: &mut dyn FnMut(SearchEvent),
        ) -> Result<(), JobError> {
            Ok(())
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn children(&self) -> Vec<&dyn SearchJob> {
            self.children
                .iter()
                .map(|child| child as &dyn SearchJob)
                .collect()
        }

        fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
            if verbosity == Verbosity::None {
                Vec::new()
            } else {
                self.attributes.clone()
            }
        }
    }

    fn leaf(name: &'static str) -> Node {
        Node {
            name,
            attributes: Vec::new(),
            children: Vec::new(),
        }
    }

    fn attr(key: &str, value: &str) -> (String, String) {
        (key.to_owned(), value.to_owned())
    }

    #[test]
    fn json_leaf_is_just_the_name() {
        let node = leaf("NoopJob");
        assert_eq!(json(&node), "\"NoopJob\"");
    }

    #[test]
    fn json_uses_typed_attributes_and_sorted_keys() {
        let node = Node {
            name: "SearcherTextSearchJob",
            attributes: vec![attr("indexed", "false")],
            children: Vec::new(),
        };
        assert_eq!(
            json_verbose(&node, Verbosity::Basic),
            "{\n  \"SearcherTextSearchJob\": {\n    \"indexed\": false\n  }\n}"
        );
    }

    #[test]
    fn json_reproduces_duplicate_child_key_rename_quirk() {
        // Three same-named children: the second renames the first to `.0`,
        // and the third overwrites `.1` (baseline `node.params`).
        let node = Node {
            name: "ParentJob",
            attributes: Vec::new(),
            children: vec![leaf("ChildJob"), leaf("ChildJob"), leaf("ChildJob")],
        };
        assert_eq!(
            json_verbose(&node, Verbosity::Basic),
            "{\n  \"ParentJob\": {\n    \"ChildJob.0\": null,\n    \"ChildJob.1\": {}\n  }\n}"
        );
    }

    #[test]
    fn sexp_pretty_and_compact() {
        let node = Node {
            name: "LogJob",
            attributes: Vec::new(),
            children: vec![leaf("NoopJob")],
        };
        assert_eq!(sexp_verbose(&node, Verbosity::Basic, true), "(LOG\n  NOOP)");
        assert_eq!(sexp(&node), "(LOG NOOP)");
    }

    #[test]
    fn sexp_emits_int_and_slice_values() {
        let node = Node {
            name: "LimitJob",
            attributes: vec![attr("limit", "10")],
            children: vec![leaf("SelectJob")],
        };
        assert_eq!(
            sexp_verbose(&node, Verbosity::Basic, false),
            "(LIMIT (limit . 10) SELECT)"
        );
    }

    #[test]
    fn mermaid_flowchart() {
        let node = Node {
            name: "LogJob",
            attributes: Vec::new(),
            children: vec![leaf("NoopJob")],
        };
        // `writeNode` always writes a trailing separator at its own depth.
        assert_eq!(
            mermaid_verbose(&node, Verbosity::Basic),
            "flowchart TB\n0[\"LOG\"]\n  0---1\n  1[\"NOOP\"]\n    "
        );
    }

    #[test]
    fn mermaid_escapes_labels() {
        let node = Node {
            name: "AlertJob",
            attributes: vec![attr("query", "a<b&c")],
            children: Vec::new(),
        };
        assert_eq!(
            mermaid_verbose(&node, Verbosity::Basic),
            "flowchart TB\n0[\"ALERT <br> query: a#lt;b#amp;c\"]\n  "
        );
    }

    #[test]
    fn emit_and_json_values_match_otel() {
        assert_eq!(AttrValue::Bool(true).emit(), "true");
        assert_eq!(AttrValue::Int(-3).emit(), "-3");
        assert_eq!(
            AttrValue::StringSlice(vec!["a<b".to_owned(), "c".to_owned()]).emit(),
            "[\"a\\u003cb\",\"c\"]"
        );
        assert_eq!(
            AttrValue::StringSlice(vec!["a".to_owned()]).json_value(),
            Json::Array(vec![Json::Str("a".to_owned())])
        );
    }
}
