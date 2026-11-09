#![forbid(unsafe_code)]

//! `internal/search/query/printer.go`: `StringHuman` and the helpers it uses
//! to turn a parsed tree back into a query string.

use crate::fields::{FIELD_AFTER, FIELD_BEFORE, FIELD_REPO, FIELD_REV};
use crate::labels::Labels;
use crate::node::{go_quote, Node, OperatorKind};
use crate::parser::{delimit, scan_balanced_pattern};
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
}
