#![forbid(unsafe_code)]

//! The transformer helpers `Parse` needs: `Hoist`, `PartitionSearchPattern`,
//! `processTopLevel` and `naturallyOrdered`
//! (`internal/search/query/transformer.go`, `validate.go`, `mapper.go`).

use crate::labels::Labels;
use crate::node::{
    exists, is_pattern_expression, map_pattern, new_operator, to_nodes, visit_parameter, Node,
    Operator, OperatorKind, Parameter,
};

/// `PartitionSearchPattern`: split into scope parameters and a single pattern
/// expression, rejecting nested/non-top-level parameter expressions.
pub fn partition_search_pattern(nodes: &[Node]) -> Result<(Vec<Parameter>, Option<Node>), String> {
    let nodes: Vec<Node> = if nodes.len() == 1 {
        process_top_level(nodes)?
    } else {
        nodes.to_vec()
    };

    let mut patterns: Vec<Node> = Vec::new();
    let mut parameters: Vec<Parameter> = Vec::new();
    for node in nodes {
        if is_pattern_expression(std::slice::from_ref(&node)) {
            patterns.push(node);
        } else if let Node::Parameter(param) = node {
            parameters.push(param);
        } else {
            return Err("cannot evaluate: unable to partition pure search pattern".to_owned());
        }
    }

    let pattern = if patterns.len() > 1 {
        Some(Node::Operator(Operator {
            kind: OperatorKind::And,
            operands: patterns,
            annotation: Default::default(),
        }))
    } else if patterns.len() == 1 {
        Some(patterns.remove(0))
    } else {
        None
    };

    Ok((parameters, pattern))
}

/// `processTopLevel`.
fn process_top_level(nodes: &[Node]) -> Result<Vec<Node>, String> {
    let Some(first) = nodes.first() else {
        return Ok(nodes.to_vec());
    };
    if let Node::Operator(term) = first {
        let single = [Node::Operator(term.clone())];
        let pattern_expression = is_pattern_expression(&single);
        if matches!(term.kind, OperatorKind::And | OperatorKind::Or) && pattern_expression {
            return Ok(nodes.to_vec());
        } else if term.kind == OperatorKind::And {
            return Ok(term.operands.clone());
        } else if term.kind == OperatorKind::Concat {
            return Ok(nodes.to_vec());
        }
        return Err("cannot evaluate: unable to partition pure search pattern".to_owned());
    }
    Ok(nodes.to_vec())
}

/// `naturallyOrdered`: reading left to right (or right to left), parameters
/// only appear after patterns.
fn naturally_ordered(node: &Node, reverse: bool) -> bool {
    let mut rightmost_parameter_pos = 0usize;
    let mut rightmost_pattern_pos = 0usize;
    let mut leftmost_parameter_pos = 1usize << 30;
    let mut leftmost_pattern_pos = 1usize << 30;

    let nodes = std::slice::from_ref(node);
    visit_parameter(nodes, &mut |_, _, _, annotation| {
        let column = annotation.range.start.column;
        if column > rightmost_parameter_pos {
            rightmost_parameter_pos = column;
        }
        if column < leftmost_parameter_pos {
            leftmost_parameter_pos = column;
        }
    });
    crate::node::visit_pattern(nodes, &mut |_, _, annotation| {
        let column = annotation.range.start.column;
        if column > rightmost_pattern_pos {
            rightmost_pattern_pos = column;
        }
        if column < leftmost_pattern_pos {
            leftmost_pattern_pos = column;
        }
    });

    if reverse {
        leftmost_parameter_pos > rightmost_pattern_pos
    } else {
        rightmost_parameter_pos < leftmost_pattern_pos
    }
}

/// `Hoist`: rewrite `repo:foo a or b` into `repo:foo (a or b)` when the query
/// starts with parameters, followed by patterns, followed by parameters.
pub fn hoist(nodes: &[Node]) -> Result<Vec<Node>, String> {
    if nodes.len() != 1 {
        return Err("heuristic requires one top-level expression".to_owned());
    }
    let expression = match &nodes[0] {
        Node::Operator(op) if op.kind != OperatorKind::Concat => op,
        _ => return Err("heuristic requires top-level and- or or-expression".to_owned()),
    };

    let n = expression.operands.len();
    let mut pattern: Vec<Node> = Vec::new();
    let mut scope_parameters: Vec<Parameter> = Vec::new();

    for (i, node) in expression.operands.iter().enumerate() {
        if i == 0 {
            // Handles "(and parameter expression)".
            if let Node::Parameter(param) = node {
                if expression.kind == OperatorKind::And {
                    let hoisted = hoist(&new_operator(
                        expression.operands[1..].to_vec(),
                        expression.kind,
                    ))?;
                    let mut result = vec![Node::Parameter(param.clone())];
                    result.extend(hoisted);
                    return Ok(result);
                }
            }
            let (scope_part, pattern_part) =
                partition_search_pattern(std::slice::from_ref(node))
                    .map_err(|_| "could not partition first expression".to_owned())?;
            let Some(pattern_part) = pattern_part else {
                return Err("could not partition first expression".to_owned());
            };
            if !naturally_ordered(node, false) {
                return Err("unnatural order: patterns not followed by parameter".to_owned());
            }
            pattern.push(pattern_part);
            scope_parameters.extend(scope_part);
            continue;
        }
        if i == n - 1 {
            let (scope_part, pattern_part) =
                partition_search_pattern(std::slice::from_ref(node))
                    .map_err(|_| "could not partition first expression".to_owned())?;
            let Some(pattern_part) = pattern_part else {
                return Err("could not partition first expression".to_owned());
            };
            if !naturally_ordered(node, true) {
                return Err("unnatural order: patterns not followed by parameter".to_owned());
            }
            pattern.push(pattern_part);
            scope_parameters.extend(scope_part);
            continue;
        }
        if !is_pattern_expression(std::slice::from_ref(node)) {
            return Err(format!(
                "inner expression {} is not a pure pattern expression",
                node
            ));
        }
        pattern.push(node.clone());
    }

    let pattern = map_pattern(&pattern, &mut |value, negated, annotation| {
        let mut annotation = annotation.clone();
        annotation.labels.set(Labels::HEURISTIC_HOISTED);
        Node::Pattern(crate::node::Pattern {
            value: value.to_owned(),
            negated,
            annotation,
        })
    });

    let mut result = to_nodes(&scope_parameters);
    result.extend(new_operator(pattern, expression.kind));
    Ok(result)
}

/// `validatePureLiteralPattern`.
pub fn validate_pure_literal_pattern(nodes: &[Node], balanced: bool) -> Result<(), String> {
    let impure = exists(nodes, &mut |node| {
        if let Node::Operator(operator) = node {
            if operator.kind == OperatorKind::Concat {
                return operator.operands.iter().any(|operand| {
                    matches!(
                        operand,
                        Node::Operator(op) if op.kind == OperatorKind::Or || op.kind == OperatorKind::And
                    )
                });
            }
        }
        false
    });
    if impure {
        if !balanced {
            return Err("this literal search query contains unbalanced parentheses. I tried to guess what you meant, but wasn't able to. Maybe you missed a parenthesis? Otherwise, try using the content: filter if the pattern is unbalanced".to_owned());
        }
        return Err("i'm having trouble understanding that query. The combination of parentheses is the problem. Try using the content: filter to quote patterns that contain parentheses".to_owned());
    }
    Ok(())
}
