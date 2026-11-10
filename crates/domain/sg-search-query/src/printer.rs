#![forbid(unsafe_code)]

//! `internal/search/query/printer.go`: `StringHuman` and the helpers it uses
//! to turn a parsed tree back into a query string.

use sg_jsonx::go_json_string;

use crate::fields::{FIELD_AFTER, FIELD_BEFORE, FIELD_REPO, FIELD_REV};
use crate::labels::Labels;
use crate::node::{go_quote, Node, OperatorKind};
use crate::parser::{delimit, scan_balanced_pattern};
use crate::range::Range;
use crate::transformer::partition_search_pattern;

/// `stringHumanPattern`.
fn string_human_pattern(nodes: &[Node]) -> String {
    let mut result: Vec<String> = Vec::new();
    for node in nodes {
        match node {
            Node::Pattern(n) => {
                let mut v = n.value.clone();
                if n.annotation.labels.is_set(Labels::QUOTED) {
                    v = go_quote(&v);
                }
                if n.annotation.labels.is_set(Labels::REGEXP) {
                    v = delimit(&v, '/');
                }
                let (_, _, ok) = scan_balanced_pattern(v.as_bytes());
                if !ok
                    && !n.annotation.labels.is_set(Labels::IS_CONTENT)
                    && n.annotation.labels.is_set(Labels::LITERAL)
                {
                    v = format!("content:{}", go_quote(&v));
                    if n.negated {
                        v = format!("-{v}");
                    }
                } else if n.annotation.labels.is_set(Labels::IS_CONTENT) {
                    v = format!("content:{v}");
                    if n.negated {
                        v = format!("-{v}");
                    }
                } else if n.negated {
                    v = format!("(NOT {v})");
                }
                result.push(v);
            }
            Node::Operator(n) => {
                let nested: Vec<String> = n
                    .operands
                    .iter()
                    .map(|operand| string_human_pattern(std::slice::from_ref(operand)))
                    .collect();
                let separator = match n.kind {
                    OperatorKind::Or => " OR ",
                    OperatorKind::And => " AND ",
                    OperatorKind::Concat => "",
                };
                result.push(format!("({})", nested.join(separator)));
            }
            // `stringHumanPattern` has no Parameter case; parameters are
            // silently dropped here, like the baseline.
            Node::Parameter(_) => {}
        }
    }
    result.join("")
}

/// `stringHumanParameters`.
fn string_human_parameters(parameters: &[crate::node::Parameter]) -> String {
    let mut result: Vec<String> = Vec::new();
    for p in parameters {
        let mut v = p.value.clone();
        if p.annotation.labels.is_set(Labels::QUOTED) {
            v = go_quote(&v);
        }
        let mut field = p.field.clone();
        if p.annotation.labels.is_set(Labels::IS_ALIAS) {
            // Preserve the alias for fields that have a single alias.
            field = match p.field.as_str() {
                FIELD_REPO => "r".to_owned(),
                FIELD_AFTER => "since".to_owned(),
                FIELD_BEFORE => "until".to_owned(),
                FIELD_REV => "revision".to_owned(),
                _ => field,
            };
        }
        let rendered = format!("{field}:{v}");
        if p.negated {
            result.push(format!("-{rendered}"));
        } else {
            result.push(rendered);
        }
    }
    result.join(" ")
}

/// `StringHuman`: renders a parsed tree back into a query string.
pub fn string_human(nodes: &[Node]) -> String {
    match partition_search_pattern(nodes) {
        Ok((parameters, pattern)) => {
            let Some(pattern) = pattern else {
                return string_human_parameters(&parameters);
            };
            if parameters.is_empty() {
                return string_human_pattern(std::slice::from_ref(&pattern));
            }
            format!(
                "{} {}",
                string_human_parameters(&parameters),
                string_human_pattern(std::slice::from_ref(&pattern))
            )
        }
        Err(_) => {
            // We couldn't partition at this level, so recurse on operators
            // until we can.
            let mut v: Vec<String> = Vec::new();
            for node in nodes {
                if let Node::Operator(term) = node {
                    let s: Vec<String> = term
                        .operands
                        .iter()
                        .map(|operand| string_human(std::slice::from_ref(operand)))
                        .collect();
                    match term.kind {
                        OperatorKind::Or => v.push(format!("({})", s.join(" OR "))),
                        OperatorKind::And => v.push(format!("({})", s.join(" AND "))),
                        OperatorKind::Concat => {}
                    }
                }
            }
            v.join("")
        }
    }
}

/// `nodeToJSON` (`printer.go:117-198`): the `encoding/json` shape of one node.
///
/// Go marshals anonymous structs, so the key order is the declaration order
/// (`field`/`value`/`negated`/`labels`/`range` for parameters, and
/// `value`/`negated`/`labels`/`range` for patterns), not the sorted order of
/// a map.
fn node_to_json(node: &Node, out: &mut String) {
    match node {
        Node::Operator(n) => {
            out.push('{');
            out.push_str(&go_json_string(n.kind.as_str()));
            out.push(':');
            // Go's `var jsons []any` is nil with no operands, which
            // `encoding/json` encodes as `null`.
            if n.operands.is_empty() {
                out.push_str("null");
            } else {
                out.push('[');
                for (index, operand) in n.operands.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    node_to_json(operand, out);
                }
                out.push(']');
            }
            out.push('}');
        }
        Node::Parameter(p) => {
            out.push_str(r#"{"field":"#);
            out.push_str(&go_json_string(&p.field));
            out.push_str(r#","value":"#);
            out.push_str(&go_json_string(&p.value));
            out.push_str(r#","negated":"#);
            out.push_str(go_bool(p.negated));
            out.push_str(r#","labels":"#);
            write_json_labels(p.annotation.labels, out);
            out.push_str(r#","range":"#);
            write_json_range(p.annotation.range, out);
            out.push('}');
        }
        Node::Pattern(p) => {
            out.push_str(r#"{"value":"#);
            out.push_str(&go_json_string(&p.value));
            out.push_str(r#","negated":"#);
            out.push_str(go_bool(p.negated));
            out.push_str(r#","labels":"#);
            write_json_labels(p.annotation.labels, out);
            out.push_str(r#","range":"#);
            write_json_range(p.annotation.range, out);
            out.push('}');
        }
    }
}

fn go_bool(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

/// `labels.String()`: `["None"]` when unset, else the sorted set names.
fn write_json_labels(labels: Labels, out: &mut String) {
    out.push('[');
    for (index, name) in labels.names().iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&go_json_string(name));
    }
    out.push(']');
}

/// `Range`'s JSON shape (`range.go:4-10`).
fn write_json_range(range: Range, out: &mut String) {
    out.push_str(r#"{"start":{"line":"#);
    out.push_str(&range.start.line.to_string());
    out.push_str(r#","column":"#);
    out.push_str(&range.start.column.to_string());
    out.push_str(r#"},"end":{"line":"#);
    out.push_str(&range.end.line.to_string());
    out.push_str(r#","column":"#);
    out.push_str(&range.end.column.to_string());
    out.push_str("}}");
}

/// `ToJSON` (`printer.go:202-211`): `json.Marshal(nodesToJSON(q))`.
///
/// `nodesToJSON` declares `var jsons []any`, so an empty query marshals as
/// `null`, not `[]`.
pub fn to_json(nodes: &[Node]) -> String {
    if nodes.is_empty() {
        return "null".to_owned();
    }
    let mut out = String::new();
    out.push('[');
    for (index, node) in nodes.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        node_to_json(node, &mut out);
    }
    out.push(']');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Annotation, Pattern};
    use crate::range::Range;

    fn pattern(value: &str, labels: Labels) -> Node {
        Node::Pattern(Pattern {
            value: value.to_owned(),
            negated: false,
            annotation: Annotation {
                labels,
                range: Range::new(0, 0),
            },
        })
    }

    #[test]
    fn quotes_quoted_patterns() {
        assert_eq!(string_human(&[pattern("a b", Labels::QUOTED)]), "\"a b\"");
    }

    #[test]
    fn prefixes_content_for_unbalanced_literal() {
        // "a(" scans as unbalanced, so the literal ends up as content:"a(".
        assert_eq!(
            string_human(&[pattern("a(", Labels::LITERAL)]),
            "content:\"a(\""
        );
    }

    #[test]
    fn preserves_single_alias_fields() {
        let param = Node::Parameter(crate::node::Parameter {
            field: "repo".to_owned(),
            value: "foo".to_owned(),
            negated: true,
            annotation: Annotation {
                labels: Labels::IS_ALIAS,
                range: Range::new(0, 0),
            },
        });
        assert_eq!(string_human(&[param]), "-r:foo");
    }

    #[test]
    fn joins_and_operators() {
        let nodes = crate::node::new_operator(
            vec![pattern("a", Labels::LITERAL), pattern("b", Labels::LITERAL)],
            OperatorKind::And,
        );
        assert_eq!(string_human(&nodes), "(a AND b)");
    }

    fn parameter(field: &str, value: &str, labels: Labels) -> Node {
        Node::Parameter(crate::node::Parameter {
            field: field.to_owned(),
            value: value.to_owned(),
            negated: false,
            annotation: Annotation {
                labels,
                range: Range::new(0, 0),
            },
        })
    }

    #[test]
    fn json_parameter_uses_declaration_order() {
        let node = Node::Parameter(crate::node::Parameter {
            field: "repo".to_owned(),
            value: "a".to_owned(),
            negated: true,
            annotation: Annotation {
                labels: Labels::LITERAL | Labels::STANDARD,
                range: Range::new(1, 4),
            },
        });
        assert_eq!(
            to_json(&[node]),
            r#"[{"field":"repo","value":"a","negated":true,"labels":["Literal","Standard"],"range":{"start":{"line":0,"column":1},"end":{"line":0,"column":4}}}]"#
        );
    }

    #[test]
    fn json_pattern_uses_none_label() {
        let node = Node::Pattern(Pattern {
            value: "a&b".to_owned(),
            negated: false,
            annotation: Annotation {
                labels: Labels::NONE,
                range: Range::new(0, 3),
            },
        });
        assert_eq!(
            to_json(&[node]),
            r#"[{"value":"a\u0026b","negated":false,"labels":["None"],"range":{"start":{"line":0,"column":0},"end":{"line":0,"column":3}}}]"#
        );
    }

    #[test]
    fn json_empty_query_is_null() {
        assert_eq!(to_json(&[]), "null");
    }

    #[test]
    fn json_operator_nests_operands_in_order() {
        let nodes = vec![Node::Operator(crate::node::Operator {
            kind: OperatorKind::Or,
            operands: vec![
                pattern("a", Labels::LITERAL),
                parameter("repo", "r", Labels::STANDARD),
            ],
            annotation: Annotation::default(),
        })];
        assert_eq!(
            to_json(&nodes),
            r#"[{"or":[{"value":"a","negated":false,"labels":["Literal"],"range":{"start":{"line":0,"column":0},"end":{"line":0,"column":0}}},{"field":"repo","value":"r","negated":false,"labels":["Standard"],"range":{"start":{"line":0,"column":0},"end":{"line":0,"column":0}}}]}]"#
        );
    }

    #[test]
    fn json_empty_operator_operands_are_null() {
        let nodes = vec![Node::Operator(crate::node::Operator {
            kind: OperatorKind::And,
            operands: Vec::new(),
            annotation: Annotation::default(),
        })];
        assert_eq!(to_json(&nodes), r#"[{"and":null}]"#);
    }
}
