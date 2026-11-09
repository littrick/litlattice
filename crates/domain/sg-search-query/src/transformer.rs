#![forbid(unsafe_code)]

//! The transformer helpers `Parse` needs: `Hoist`, `PartitionSearchPattern`,
//! `processTopLevel` and `naturallyOrdered`
//! (`internal/search/query/transformer.go`, `validate.go`, `mapper.go`).

use crate::basic::{to_parameters, Basic, Parameters};
use crate::fields::ALIASES;
use crate::labels::Labels;
use crate::mapper::{map_field, map_parameter, map_pattern};
use crate::node::{
    exists, is_pattern_expression, new_operator, to_nodes, visit_field, visit_parameter, Node,
    Operator, OperatorKind, Parameter, Pattern,
};
use crate::parser::SearchType;

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
        Some(Node::Pattern(crate::node::Pattern {
            value: value.to_owned(),
            negated,
            annotation,
        }))
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

// ---------------------------------------------------------------------------
// Pipeline transforms (`transformer.go`)
// ---------------------------------------------------------------------------

/// `SubstituteAliases`: canonicalize field aliases and turn `content:` into a
/// pattern node.
pub fn substitute_aliases(nodes: &[Node], search_type: SearchType) -> Vec<Node> {
    map_parameter(nodes, &mut |field, value, negated, annotation| {
        if field == "content" {
            let mut annotation = annotation.clone();
            if search_type == SearchType::Regex && !annotation.labels.is_set(Labels::QUOTED) {
                annotation.labels.set(Labels::REGEXP);
            } else {
                annotation.labels.set(Labels::LITERAL);
            }
            annotation.labels.set(Labels::IS_CONTENT);
            return Some(Node::Pattern(Pattern {
                value: value.to_owned(),
                negated,
                annotation,
            }));
        }
        let mut annotation = annotation.clone();
        let mut field = field.to_owned();
        if let Some((_, canonical)) = ALIASES.iter().find(|(alias, _)| *alias == field) {
            annotation.labels.set(Labels::IS_ALIAS);
            field = (*canonical).to_owned();
        }
        Some(Node::Parameter(Parameter {
            field,
            value: value.to_owned(),
            negated,
            annotation,
        }))
    })
}

/// `LowercaseFieldNames`.
pub fn lowercase_field_names(nodes: &[Node]) -> Vec<Node> {
    map_parameter(nodes, &mut |field, value, negated, annotation| {
        Some(Node::Parameter(Parameter {
            field: field.to_lowercase(),
            value: value.to_owned(),
            negated,
            annotation: annotation.clone(),
        }))
    })
}

/// `CountAllLimit`: the limit `count:all` is substituted with.
pub const COUNT_ALL_LIMIT: i64 = 99_999_999;

/// `SubstituteCountAll`: replace `count:all` with `count:99999999`.
pub fn substitute_count_all(nodes: &[Node]) -> Vec<Node> {
    map_parameter(nodes, &mut |field, value, negated, annotation| {
        let value = if field == crate::fields::FIELD_COUNT && value.to_lowercase() == "all" {
            COUNT_ALL_LIMIT.to_string()
        } else {
            value.to_owned()
        };
        Some(Node::Parameter(Parameter {
            field: field.to_owned(),
            value,
            negated,
            annotation: annotation.clone(),
        }))
    })
}

/// `BuildPlan`.
pub fn build_plan(query: &[Node]) -> Vec<Basic> {
    distribute(Vec::new(), query)
}

/// `distribute`.
fn distribute(prefixes: Vec<Basic>, nodes: &[Node]) -> Vec<Basic> {
    let mut prefixes = prefixes;
    for node in nodes {
        match node {
            Node::Operator(op) => {
                if is_pattern_expression(&op.operands) {
                    prefixes = product(
                        prefixes,
                        Basic::new(Parameters::default(), Some(node.clone())),
                    );
                    continue;
                }
                match op.kind {
                    OperatorKind::Or => {
                        let mut result: Vec<Basic> = Vec::new();
                        for operand in &op.operands {
                            let new_basics = distribute(Vec::new(), std::slice::from_ref(operand));
                            for new_basic in new_basics {
                                result.extend(product(prefixes.clone(), new_basic));
                            }
                        }
                        prefixes = result;
                    }
                    OperatorKind::And | OperatorKind::Concat => {
                        prefixes = distribute(prefixes, &op.operands);
                    }
                }
            }
            Node::Parameter(p) => {
                prefixes = product(prefixes, Basic::new(Parameters::new(vec![p.clone()]), None));
            }
            Node::Pattern(p) => {
                prefixes = product(
                    prefixes,
                    Basic::new(Parameters::default(), Some(Node::Pattern(p.clone()))),
                );
            }
        }
    }
    prefixes
}

/// `product`: conjunction between `to_merge` and each input basic query.
fn product(basics: Vec<Basic>, to_merge: Basic) -> Vec<Basic> {
    if basics.is_empty() {
        return vec![to_merge];
    }
    basics
        .into_iter()
        .map(|basic| conjunction(basic, to_merge.clone()))
        .collect()
}

/// `conjunction`.
fn conjunction(left: Basic, right: Basic) -> Basic {
    let pattern = match (left.pattern, right.pattern) {
        (None, right) => right,
        (left, None) => left,
        (Some(left_pattern), Some(right_pattern)) => {
            new_operator(vec![left_pattern, right_pattern], OperatorKind::And)
                .into_iter()
                .next()
        }
    };
    let mut parameters = left.parameters.0;
    parameters.extend(right.parameters.0);
    Basic::new(Parameters::new(parameters), pattern)
}

/// `fuzzyRegexp`: interpolate patterns with `.*?` and combine into one regexp.
pub fn fuzzy_regexp(patterns: &[Pattern]) -> Vec<Node> {
    if patterns.len() == 1 {
        return vec![Node::Pattern(patterns[0].clone())];
    }
    let values: Vec<String> = patterns.iter().map(|p| p.regexp_pattern()).collect();
    vec![Node::Pattern(Pattern {
        annotation: crate::node::Annotation {
            labels: Labels::REGEXP,
            ..Default::default()
        },
        value: format!("(?:{})", values.join(").*?(?:")),
        negated: false,
    })]
}

/// `standard`: concatenate adjacent literals, AND regexps with them.
pub fn standard(patterns: &[Pattern]) -> Vec<Node> {
    if patterns.len() == 1 {
        return vec![Node::Pattern(patterns[0].clone())];
    }
    let mut literals: Vec<Pattern> = Vec::new();
    let mut result: Vec<Node> = Vec::new();
    for pattern in patterns {
        if pattern.annotation.labels.is_set(Labels::REGEXP) {
            if !literals.is_empty() {
                result.extend(space(&literals));
            }
            result.push(Node::Pattern(pattern.clone()));
            literals = Vec::new();
            continue;
        }
        literals.push(pattern.clone());
    }
    if !literals.is_empty() {
        result.extend(space(&literals));
    }
    result
}

/// `space`: concatenate patterns with a space, preserving the first pattern's
/// labels.
pub fn space(patterns: &[Pattern]) -> Vec<Node> {
    if patterns.len() == 1 {
        return vec![Node::Pattern(patterns[0].clone())];
    }
    let values: Vec<&str> = patterns.iter().map(|p| p.value.as_str()).collect();
    vec![Node::Pattern(Pattern {
        annotation: patterns[0].annotation.clone(),
        value: values.join(" "),
        negated: false,
    })]
}

/// `and`: `NewOperator(patterns, And)`.
pub fn and(patterns: Vec<Node>) -> Vec<Node> {
    new_operator(patterns, OperatorKind::And)
}

fn is_pattern(node: &Node) -> bool {
    matches!(node, Node::Pattern(p) if !p.negated)
}

/// `substituteConcat`: concatenate contiguous patterns rooted by a concat
/// operator; negated patterns split the run.
pub fn substitute_concat(nodes: &[Node], callback: &dyn Fn(&[Pattern]) -> Vec<Node>) -> Vec<Node> {
    let mut new_nodes: Vec<Node> = Vec::new();
    for node in nodes {
        match node {
            Node::Parameter(_) | Node::Pattern(_) => new_nodes.push(node.clone()),
            Node::Operator(op) => {
                if op.kind == OperatorKind::Concat {
                    let mut ps: Vec<Pattern> = Vec::new();
                    let mut previous = op.operands[0].clone();
                    if let Node::Pattern(p) = &previous {
                        ps.push(p.clone());
                    }
                    for next in &op.operands[1..] {
                        if is_pattern(next) && is_pattern(&previous) {
                            if let Node::Pattern(p) = next {
                                ps.push(p.clone());
                            }
                            previous = next.clone();
                            continue;
                        }
                        if !ps.is_empty() {
                            new_nodes.extend(callback(&ps));
                            ps = Vec::new();
                        }
                        new_nodes.extend(substitute_concat(std::slice::from_ref(next), callback));
                    }
                    if !ps.is_empty() {
                        new_nodes.extend(callback(&ps));
                    }
                } else {
                    new_nodes.extend(new_operator(
                        substitute_concat(&op.operands, callback),
                        op.kind,
                    ));
                }
            }
        }
    }
    new_nodes
}

/// `substituteConcatForKeyword`: like `substituteConcat`, but allows `or`/`and`
/// operators nested inside `concat`.
pub fn substitute_concat_for_keyword(
    nodes: &[Node],
    callback: &dyn Fn(Vec<Node>) -> Vec<Node>,
) -> Vec<Node> {
    let mut new_nodes: Vec<Node> = Vec::new();
    for node in nodes {
        match node {
            Node::Parameter(_) | Node::Pattern(_) => new_nodes.push(node.clone()),
            Node::Operator(op) => {
                if op.kind == OperatorKind::Concat {
                    let mut ps: Vec<Node> = Vec::new();
                    for child in &op.operands {
                        if is_pattern(child) {
                            ps.push(child.clone());
                        } else {
                            ps.extend(substitute_concat_for_keyword(
                                std::slice::from_ref(child),
                                callback,
                            ));
                        }
                    }
                    if !ps.is_empty() {
                        new_nodes.extend(callback(ps));
                    }
                } else {
                    new_nodes.extend(new_operator(
                        substitute_concat_for_keyword(&op.operands, callback),
                        op.kind,
                    ));
                }
            }
        }
    }
    new_nodes
}

/// `escapeParens` (`escapeParensHeuristic`'s core).
///
/// Baseline quirk: `for i := range len(s)` shadows the outer `i`, so the
/// pre-scan that looks for the first special character has no effect and the
/// rewrite always starts at offset 0 (the prefix is preserved). We start at 0
/// to match the observed behaviour.
pub fn escape_parens(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    if bytes.is_empty() {
        return s.to_owned();
    }
    let mut result: Vec<u8> = Vec::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                if i + 1 < bytes.len() {
                    result.push(b'\\');
                    result.push(bytes[i + 1]);
                    i += 2;
                    continue;
                }
                i += 1;
                result.push(b'\\');
            }
            b'(' => {
                if i + 1 == bytes.len() {
                    result.push(b'\\');
                    result.push(b'(');
                    i += 1;
                    continue;
                }
                if i + 1 < bytes.len() && bytes[i + 1] == b')' {
                    result.extend_from_slice(b"\\(\\)");
                    i += 2;
                    continue;
                }
                result.push(b'(');
                i += 1;
            }
            b => {
                result.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(result).expect("escapeParens preserves utf8")
}

/// `escapeParensHeuristic`.
pub fn escape_parens_heuristic(nodes: &[Node]) -> Vec<Node> {
    map_pattern(nodes, &mut |value, negated, annotation| {
        let value = if annotation.labels.is_set(Labels::QUOTED) {
            value.to_owned()
        } else {
            escape_parens(value)
        };
        Some(Node::Pattern(Pattern {
            value,
            negated,
            annotation: annotation.clone(),
        }))
    })
}

/// `ConcatRevFilters`: remove `rev:` and attach it to positive `repo:` fields
/// as `repo@rev`. The `rev:at.time(...)` predicate branch is not ported.
pub fn concat_rev_filters(basic: Basic) -> Basic {
    let parameters = basic.parameters.to_nodes();
    let mut revision = String::new();
    visit_field(
        &parameters,
        crate::fields::FIELD_REV,
        &mut |value, _negated, annotation| {
            if !annotation.labels.is_set(Labels::IS_PREDICATE) {
                revision = value.to_owned();
            }
        },
    );
    if revision.is_empty() {
        return basic;
    }

    let nodes = map_field(&parameters, crate::fields::FIELD_REV, &mut |_, _, _| None);
    let modified = map_field(
        &nodes,
        crate::fields::FIELD_REPO,
        &mut |value, negated, annotation| {
            let value = if !negated && !annotation.labels.is_set(Labels::IS_PREDICATE) {
                format!("{value}@{revision}")
            } else {
                value.to_owned()
            };
            Some(Node::Parameter(Parameter {
                field: crate::fields::FIELD_REPO.to_owned(),
                value,
                negated,
                annotation: annotation.clone(),
            }))
        },
    );
    Basic::new(
        Parameters::new(to_parameters(&modified)),
        basic.pattern.clone(),
    )
}

/// `labelStructural`: convert `Literal` labels to `Structural`.
pub fn label_structural(nodes: &[Node]) -> Vec<Node> {
    map_pattern(nodes, &mut |value, negated, annotation| {
        let mut annotation = annotation.clone();
        annotation.labels.unset(Labels::LITERAL);
        annotation.labels.set(Labels::STRUCTURAL);
        Some(Node::Pattern(Pattern {
            value: value.to_owned(),
            negated,
            annotation,
        }))
    })
}

/// `ellipsesForHoles`: replace `...` with `:[_]`.
pub fn ellipses_for_holes(nodes: &[Node]) -> Vec<Node> {
    map_pattern(nodes, &mut |value, negated, annotation| {
        Some(Node::Pattern(Pattern {
            value: value.replace("...", ":[_]"),
            negated,
            annotation: annotation.clone(),
        }))
    })
}

/// `ToBasicQuery`.
pub fn to_basic_query(nodes: &[Node]) -> Result<Basic, String> {
    let (parameters, pattern) = partition_search_pattern(nodes)?;
    Ok(Basic::new(Parameters::new(parameters), pattern))
}
