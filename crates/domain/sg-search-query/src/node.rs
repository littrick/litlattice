#![forbid(unsafe_code)]

//! The query AST (`internal/search/query/parser.go` node types) and the
//! reduction/partition helpers that build it.
//!
//! A query is a tree of [`Node`]s: [`Pattern`] leaves (search fragments),
//! [`Parameter`] leaves (`field:value` filters) and [`Operator`] nonterminals
//! (`and`, `or`, `concat`).

use crate::labels::Labels;
use crate::range::Range;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Annotation {
    pub labels: Labels,
    pub range: Range,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    pub value: String,
    pub negated: bool,
    pub annotation: Annotation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameter {
    pub field: String,
    pub value: String,
    pub negated: bool,
    pub annotation: Annotation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorKind {
    Or,
    And,
    Concat,
}

impl OperatorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OperatorKind::Or => "or",
            OperatorKind::And => "and",
            OperatorKind::Concat => "concat",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operator {
    pub kind: OperatorKind,
    pub operands: Vec<Node>,
    pub annotation: Annotation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Pattern(Pattern),
    Parameter(Parameter),
    Operator(Operator),
}

impl Node {
    pub fn pattern(value: impl Into<String>, labels: Labels, range: Range) -> Node {
        Node::Pattern(Pattern {
            value: value.into(),
            negated: false,
            annotation: Annotation { labels, range },
        })
    }
}

/// `strconv.Quote` for the ASCII inputs the parser produces.
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{b}' => out.push_str("\\v"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

impl std::fmt::Display for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.negated {
            write!(f, "(not {})", go_quote(&self.value))
        } else {
            write!(f, "{}", go_quote(&self.value))
        }
    }
}

impl std::fmt::Display for Parameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = if self.field.is_empty() {
            self.value.clone()
        } else if self.negated {
            format!("-{}:{}", self.field, self.value)
        } else {
            format!("{}:{}", self.field, self.value)
        };
        write!(f, "{}", go_quote(&v))
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let result: Vec<String> = self.operands.iter().map(|o| o.to_string()).collect();
        write!(f, "({} {})", self.kind.as_str(), result.join(" "))
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Node::Pattern(p) => p.fmt(f),
            Node::Parameter(p) => p.fmt(f),
            Node::Operator(o) => o.fmt(f),
        }
    }
}

/// `toString`.
pub fn to_string(nodes: &[Node]) -> String {
    nodes
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn to_nodes(parameters: &[Parameter]) -> Vec<Node> {
    parameters.iter().cloned().map(Node::Parameter).collect()
}

// ---------------------------------------------------------------------------
// Reduction
// ---------------------------------------------------------------------------

fn reduce(mut left: Vec<Node>, right: Vec<Node>, kind: OperatorKind) -> (Vec<Node>, bool) {
    if let Node::Parameter(param) = &left[0] {
        if param.value.is_empty() {
            return (right, true);
        }
    }

    match &right[0] {
        Node::Operator(term) => {
            if term.kind == kind {
                left.extend(term.operands.iter().cloned());
                if right.len() > 1 {
                    left.extend(right[1..].iter().cloned());
                }
                return (left, true);
            }
        }
        Node::Parameter(term) => {
            if term.value.is_empty() {
                if right.len() > 1 {
                    left.extend(right[1..].iter().cloned());
                    return (left, true);
                }
                return (left, true);
            }
            if let Node::Operator(op) = &left[0] {
                if op.kind == kind {
                    let mut operands = op.operands.clone();
                    operands.extend(right.iter().cloned());
                    return (operands, true);
                }
            }
        }
        Node::Pattern(term) => {
            if term.value.is_empty() {
                if right.len() > 1 {
                    left.extend(right[1..].iter().cloned());
                    return (left, true);
                }
                return (left, true);
            }
            if let Node::Operator(op) = &left[0] {
                if op.kind == kind {
                    let mut operands = op.operands.clone();
                    operands.extend(right.iter().cloned());
                    return (operands, true);
                }
            }
        }
    }

    if right.len() > 1 {
        let (reduced, changed) = reduce(vec![right[0].clone()], right[1..].to_vec(), kind);
        if changed {
            left.extend(reduced);
            return (left, true);
        }
    }

    left.extend(right);
    (left, false)
}

/// `NewOperator`: constructs a node of `kind` from `nodes`, reducing where
/// possible. Returns an empty list for an empty input and the input itself for
/// a single node, matching the baseline.
pub fn new_operator(nodes: Vec<Node>, kind: OperatorKind) -> Vec<Node> {
    if nodes.is_empty() {
        return Vec::new();
    }
    if nodes.len() == 1 {
        return nodes;
    }
    let (reduced, changed) = reduce(vec![nodes[0].clone()], nodes[1..].to_vec(), kind);
    if changed {
        return new_operator(reduced, kind);
    }
    vec![Node::Operator(Operator {
        kind,
        operands: reduced,
        annotation: Annotation::default(),
    })]
}

// ---------------------------------------------------------------------------
// Tree queries
// ---------------------------------------------------------------------------

/// `Exists`: early-exiting traversal.
pub fn exists(nodes: &[Node], f: &mut dyn FnMut(&Node) -> bool) -> bool {
    for node in nodes {
        if f(node) {
            return true;
        }
        if let Node::Operator(op) = node {
            if exists(&op.operands, f) {
                return true;
            }
        }
    }
    false
}

/// `isPatternExpression`: no `Parameter` leaf anywhere.
pub fn is_pattern_expression(nodes: &[Node]) -> bool {
    !exists(nodes, &mut |node| matches!(node, Node::Parameter(_)))
}

/// `containsPattern`: any descendant is a pattern.
pub fn contains_pattern(node: &Node) -> bool {
    let slice = std::slice::from_ref(node);
    exists(slice, &mut |node| matches!(node, Node::Pattern(_)))
}

/// `partitionParameters`: separates patterns (ordered) from parameters.
pub fn partition_parameters(nodes: Vec<Node>) -> Vec<Node> {
    let mut patterns = Vec::new();
    let mut unordered_params = Vec::new();
    for node in nodes {
        match node {
            Node::Pattern(_) => patterns.push(node),
            Node::Parameter(_) => unordered_params.push(node),
            Node::Operator(ref op) => {
                if contains_pattern(&Node::Operator(op.clone())) {
                    patterns.push(node);
                } else {
                    unordered_params.push(node);
                }
            }
        }
    }
    if patterns.len() > 1 {
        let mut ordered_patterns = new_operator(patterns, OperatorKind::Concat);
        unordered_params.append(&mut ordered_patterns);
        return new_operator(unordered_params, OperatorKind::And);
    }
    unordered_params.append(&mut patterns);
    new_operator(unordered_params, OperatorKind::And)
}

// ---------------------------------------------------------------------------
// Visitors / mappers
// ---------------------------------------------------------------------------

pub fn visit_parameter(nodes: &[Node], f: &mut dyn FnMut(&str, &str, bool, &Annotation)) {
    for node in nodes {
        match node {
            Node::Operator(op) => visit_parameter(&op.operands, f),
            Node::Parameter(p) => f(&p.field, &p.value, p.negated, &p.annotation),
            Node::Pattern(_) => {}
        }
    }
}

pub fn visit_pattern(nodes: &[Node], f: &mut dyn FnMut(&str, bool, &Annotation)) {
    for node in nodes {
        match node {
            Node::Operator(op) => visit_pattern(&op.operands, f),
            Node::Pattern(p) => f(&p.value, p.negated, &p.annotation),
            Node::Parameter(_) => {}
        }
    }
}

pub fn visit_field(nodes: &[Node], field: &str, f: &mut dyn FnMut(&str, bool, &Annotation)) {
    visit_parameter(nodes, &mut |got_field, value, negated, annotation| {
        if got_field == field {
            f(value, negated, annotation);
        }
    });
}

/// `MapPattern`: replaces every pattern leaf.
pub fn map_pattern(
    nodes: &[Node],
    f: &mut dyn FnMut(&str, bool, &Annotation) -> Node,
) -> Vec<Node> {
    nodes
        .iter()
        .map(|node| match node {
            Node::Operator(op) => Node::Operator(Operator {
                kind: op.kind,
                operands: map_pattern(&op.operands, f),
                annotation: op.annotation.clone(),
            }),
            Node::Pattern(p) => f(&p.value, p.negated, &p.annotation),
            Node::Parameter(p) => Node::Parameter(p.clone()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_string_matches_baseline() {
        let nodes = new_operator(
            vec![
                Node::pattern("a", Labels::LITERAL, Range::new(0, 1)),
                Node::pattern("b", Labels::LITERAL, Range::new(2, 3)),
            ],
            OperatorKind::And,
        );
        assert_eq!(to_string(&nodes), "(and \"a\" \"b\")");
    }

    #[test]
    fn partition_concats_multiple_patterns() {
        let nodes = vec![
            Node::pattern("a", Labels::LITERAL, Range::new(0, 1)),
            Node::Parameter(Parameter {
                field: "repo".into(),
                value: "r".into(),
                negated: false,
                annotation: Annotation::default(),
            }),
            Node::pattern("b", Labels::LITERAL, Range::new(3, 4)),
        ];
        assert_eq!(
            to_string(&partition_parameters(nodes)),
            "(and \"repo:r\" (concat \"a\" \"b\"))"
        );
    }

    #[test]
    fn go_quote_escapes_like_strconv() {
        assert_eq!(go_quote("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
    }
}
