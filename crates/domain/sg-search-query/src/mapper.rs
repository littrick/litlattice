#![forbid(unsafe_code)]

//! `internal/search/query/mapper.go`: the recursive mappers the pipeline
//! transforms use. `BaseMapper.MapNodes` rebuilds operators with
//! `NewOperator`, so mapping can reduce or remove nodes; returning `None` from
//! a callback removes the node.

use crate::node::{new_operator, Annotation, Node, Operator, Parameter, Pattern};

/// `BaseMapper.MapNodes` (with `BaseMapper.MapOperator` applied to operators).
pub fn map_nodes(
    nodes: &[Node],
    parameter: &mut dyn FnMut(&str, &str, bool, &Annotation) -> Option<Node>,
    pattern: &mut dyn FnMut(&str, bool, &Annotation) -> Option<Node>,
) -> Vec<Node> {
    let mut mapped: Vec<Node> = Vec::new();
    for node in nodes {
        match node {
            Node::Pattern(p) => {
                if let Some(result) = pattern(&p.value, p.negated, &p.annotation) {
                    mapped.push(result);
                }
            }
            Node::Parameter(p) => {
                if let Some(result) = parameter(&p.field, &p.value, p.negated, &p.annotation) {
                    mapped.push(result);
                }
            }
            Node::Operator(op) => {
                let operands = map_nodes(&op.operands, parameter, pattern);
                mapped.extend(new_operator(operands, op.kind));
            }
        }
    }
    mapped
}

fn identity_parameter(
    field: &str,
    value: &str,
    negated: bool,
    annotation: &Annotation,
) -> Option<Node> {
    Some(Node::Parameter(Parameter {
        field: field.to_owned(),
        value: value.to_owned(),
        negated,
        annotation: annotation.clone(),
    }))
}

fn identity_pattern(value: &str, negated: bool, annotation: &Annotation) -> Option<Node> {
    Some(Node::Pattern(Pattern {
        value: value.to_owned(),
        negated,
        annotation: annotation.clone(),
    }))
}

/// `MapParameter`.
pub fn map_parameter(
    nodes: &[Node],
    callback: &mut dyn FnMut(&str, &str, bool, &Annotation) -> Option<Node>,
) -> Vec<Node> {
    map_nodes(nodes, callback, &mut identity_pattern)
}

/// `MapPattern`.
pub fn map_pattern(
    nodes: &[Node],
    callback: &mut dyn FnMut(&str, bool, &Annotation) -> Option<Node>,
) -> Vec<Node> {
    map_nodes(nodes, &mut identity_parameter, callback)
}

/// `MapField`.
pub fn map_field(
    nodes: &[Node],
    field: &str,
    callback: &mut dyn FnMut(&str, bool, &Annotation) -> Option<Node>,
) -> Vec<Node> {
    map_parameter(nodes, &mut |got_field, value, negated, annotation| {
        if got_field == field {
            callback(value, negated, annotation)
        } else {
            identity_parameter(got_field, value, negated, annotation)
        }
    })
}

/// Rebuilds an `Operator` after mapping its operands, like `BaseMapper`.
#[allow(dead_code)]
pub fn operator(kind: crate::node::OperatorKind, operands: Vec<Node>) -> Vec<Node> {
    new_operator(operands, kind)
}

#[allow(dead_code)]
fn _assert_operator_used(_: &Operator) {}
