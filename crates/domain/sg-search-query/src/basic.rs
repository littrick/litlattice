#![forbid(unsafe_code)]

//! `internal/search/query/types.go`: `Q` helpers, `Parameters`, `Basic` and
//! `Plan` (the disjoint-query plan `Pipeline` produces).
//!
//! The typed-predicate accessors (`RepoHasFileContent`, `RepoHasKVPs`, …)
//! need the predicate argument parser and are not ported yet; only the
//! parameter/plan surface used by `Pipeline` and query construction exists.

use crate::node::{new_operator, visit_field, Annotation, Node, OperatorKind, Parameter};
use crate::transformer::hoist;

/// `Parameters`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Parameters(pub Vec<Parameter>);

impl std::ops::Deref for Parameters {
    type Target = Vec<Parameter>;
    fn deref(&self) -> &Vec<Parameter> {
        &self.0
    }
}

impl Parameters {
    pub fn new(parameters: Vec<Parameter>) -> Parameters {
        Parameters(parameters)
    }

    /// `Parameters.Exists`: whether a parameter with this field exists.
    pub fn exists(&self, field: &str) -> bool {
        let mut found = false;
        visit_field(&self.to_nodes(), field, &mut |_, _, _| found = true);
        found
    }

    /// `Parameters.FindValue`: the first non-negated-or-not value of `field`.
    pub fn find_value(&self, field: &str) -> String {
        let mut found = String::new();
        self.find_parameter(field, &mut |value, _, _| found = value.to_owned());
        found
    }

    /// `Parameters.FindParameter`: calls `f` on the first parameter of `field`.
    pub fn find_parameter(&self, field: &str, f: &mut dyn FnMut(&str, bool, &Annotation)) {
        for parameter in &self.0 {
            if parameter.field == field {
                f(&parameter.value, parameter.negated, &parameter.annotation);
                break;
            }
        }
    }

    pub fn to_nodes(&self) -> Vec<Node> {
        self.0.iter().cloned().map(Node::Parameter).collect()
    }

    /// `Parameters.boolValue` / `Parameters.IsCaseSensitive`: the last visited
    /// `case:` value decides; a parse error yields `false` (the baseline
    /// discards the error, which validation should already have caught).
    pub fn is_case_sensitive(&self) -> bool {
        let mut result = false;
        visit_field(
            &self.to_nodes(),
            crate::fields::FIELD_CASE,
            &mut |value, _negated, _annotation| {
                result = crate::validate::parse_bool(value).unwrap_or(false);
            },
        );
        result
    }

    /// `Parameters.yesNoOnlyValue`: the last value of a yes/no/only field wins
    /// (`Yes`/`No`/`Only`), an unparsable value panics like the baseline
    /// (validation should already have rejected it).
    pub fn yes_no_only_value(&self, field: &str) -> Option<crate::validate::YesNoOnly> {
        let mut result = None;
        visit_field(
            &self.to_nodes(),
            field,
            &mut |value, _negated, _annotation| {
                let yno = crate::validate::parse_yes_no_only(value);
                if yno == crate::validate::YesNoOnly::Invalid {
                    panic!(
                        "Invalid value {} for field {}",
                        crate::node::go_quote(value),
                        crate::node::go_quote(field)
                    );
                }
                result = Some(yno);
            },
        );
        result
    }

    /// `Parameters.Index`: `index:` value, defaulting to `Yes`.
    pub fn index(&self) -> crate::validate::YesNoOnly {
        self.yes_no_only_value(crate::fields::FIELD_INDEX)
            .unwrap_or(crate::validate::YesNoOnly::Yes)
    }

    /// `Parameters.Fork`.
    pub fn fork(&self) -> Option<crate::validate::YesNoOnly> {
        self.yes_no_only_value(crate::fields::FIELD_FORK)
    }

    /// `Parameters.Archived`.
    pub fn archived(&self) -> Option<crate::validate::YesNoOnly> {
        self.yes_no_only_value(crate::fields::FIELD_ARCHIVED)
    }
}

/// `toNodes`.
pub fn to_nodes(parameters: &[Parameter]) -> Vec<Node> {
    parameters.iter().cloned().map(Node::Parameter).collect()
}

/// `toParameters`: the input must contain only parameter nodes.
pub fn to_parameters(nodes: &[Node]) -> Vec<Parameter> {
    nodes
        .iter()
        .map(|node| match node {
            Node::Parameter(p) => p.clone(),
            _ => panic!("toParameters: node is not a parameter"),
        })
        .collect()
}

/// String helpers on `Q` that the pipeline/printing tests rely on.
pub fn q_string(nodes: &[Node]) -> String {
    crate::node::to_string(nodes)
}

/// `Q.Exists`.
pub fn q_exists(nodes: &[Node], field: &str) -> bool {
    let mut found = false;
    visit_field(nodes, field, &mut |_, _, _| found = true);
    found
}

/// `Q.StringValue`: the last non-negated value of `field`.
pub fn q_string_value(nodes: &[Node], field: &str) -> String {
    let mut value = String::new();
    visit_field(nodes, field, &mut |visited, negated, _| {
        if !negated {
            value = visited.to_owned();
        }
    });
    value
}

/// `Basic`: a leaf expression of one pattern expression plus scoping
/// parameters.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Basic {
    pub parameters: Parameters,
    pub pattern: Option<Node>,
}

impl Basic {
    pub fn new(parameters: Parameters, pattern: Option<Node>) -> Basic {
        Basic {
            parameters,
            pattern,
        }
    }

    /// `Basic.ToParseTree`: parameters followed by the pattern, then `Hoist`.
    pub fn to_parse_tree(&self) -> Vec<Node> {
        let mut nodes = self.parameters.to_nodes();
        let Some(pattern) = &self.pattern else {
            return nodes;
        };
        nodes.push(pattern.clone());
        match hoist(&nodes) {
            Ok(hoisted) => hoisted,
            Err(_) => nodes,
        }
    }

    /// `Basic.MapParameters`.
    pub fn map_parameters(&self, parameters: Parameters) -> Basic {
        Basic {
            parameters,
            pattern: self.pattern.clone(),
        }
    }

    /// `Basic.MapPattern`.
    pub fn map_pattern(&self, pattern: Node) -> Basic {
        Basic {
            parameters: self.parameters.clone(),
            pattern: Some(pattern),
        }
    }

    /// `Basic.PatternString`: the pattern value when it is a `Pattern` atom,
    /// quoted with `regexp.QuoteMeta` when the atom is a literal.
    pub fn pattern_string(&self) -> String {
        match &self.pattern {
            Some(Node::Pattern(p)) => {
                if self.is_literal() {
                    crate::node::regexp_quote_meta(&p.value)
                } else {
                    p.value.clone()
                }
            }
            _ => String::new(),
        }
    }

    /// `Basic.IsLiteral`.
    pub fn is_literal(&self) -> bool {
        has_pattern_label(self, crate::labels::Labels::LITERAL)
    }

    /// `Basic.IsRegexp`.
    pub fn is_regexp(&self) -> bool {
        has_pattern_label(self, crate::labels::Labels::REGEXP)
    }

    /// `Basic.IsStructural`.
    pub fn is_structural(&self) -> bool {
        has_pattern_label(self, crate::labels::Labels::STRUCTURAL)
    }

    /// `Basic.IsEmptyPattern`.
    pub fn is_empty_pattern(&self) -> bool {
        match &self.pattern {
            None => true,
            Some(Node::Pattern(p)) => p.value.is_empty(),
            Some(_) => false,
        }
    }
}

fn has_pattern_label(basic: &Basic, label: crate::labels::Labels) -> bool {
    match &basic.pattern {
        Some(Node::Pattern(p)) => p.annotation.labels.is_set(label),
        _ => false,
    }
}

/// `Plan`: a set of disjoint basic queries whose results are unioned.
pub type Plan = Vec<Basic>;

/// `Plan.ToQ`: an Or-expression over the plan's basic queries.
pub fn plan_to_q(plan: &Plan) -> Vec<Node> {
    let mut nodes: Vec<Node> = Vec::new();
    for basic in plan {
        nodes.extend(new_operator(basic.to_parse_tree(), OperatorKind::And));
    }
    new_operator(nodes, OperatorKind::Or)
}

/// `MapPlan`.
pub fn map_plan(plan: Plan, pass: &mut dyn FnMut(Basic) -> Basic) -> Plan {
    plan.into_iter().map(pass).collect()
}
