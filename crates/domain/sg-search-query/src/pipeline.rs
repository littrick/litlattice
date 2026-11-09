#![forbid(unsafe_code)]

//! `internal/search/query/query.go`: the step/pipeline abstraction that turns
//! an input string into a [`Plan`].
//!
//! `Pipeline` validates the produced plan and then runs `ConcatRevFilters` on
//! each basic query, mirroring the baseline.

use crate::basic::{map_plan, Plan};
use crate::mapper::map_field;
use crate::node::Node;
use crate::parser::SearchType;
use crate::transformer::build_plan;
use crate::transformer::{
    and, concat_rev_filters, ellipses_for_holes, escape_parens_heuristic, fuzzy_regexp,
    label_structural, lowercase_field_names, space, standard, substitute_aliases,
    substitute_concat, substitute_concat_for_keyword, substitute_count_all,
};
use crate::validate::validate_plan;

/// A step transforms nodes and may fail.
pub type Step = Box<dyn Fn(&[Node]) -> Result<Vec<Node>, String>>;

/// `Sequence`: compose steps in order.
pub fn sequence(steps: Vec<Step>) -> Step {
    Box::new(move |nodes| {
        let mut nodes = nodes.to_vec();
        for step in &steps {
            nodes = step(&nodes)?;
        }
        Ok(nodes)
    })
}

/// `identity`.
pub fn identity() -> Step {
    Box::new(|nodes| Ok(nodes.to_vec()))
}

/// `With`: `step` when `enabled`, otherwise the identity step.
pub fn with_step(enabled: bool, step: Step) -> Step {
    if enabled {
        step
    } else {
        identity()
    }
}

/// `SubstituteSearchContexts`. Errors are returned one at a time (the baseline
/// aggregates them with `errors.Append`).
pub fn substitute_search_contexts(
    lookup_query_string: impl Fn(&str) -> Result<String, String> + 'static,
) -> Step {
    Box::new(move |nodes| {
        let mut error: Option<String> = None;
        let substituted = map_field(
            nodes,
            crate::fields::FIELD_CONTEXT,
            &mut |value, negated, annotation| {
                if error.is_some() {
                    return None;
                }
                let query_string = match lookup_query_string(value) {
                    Ok(qs) => qs,
                    Err(e) => {
                        error = Some(e);
                        return None;
                    }
                };
                if query_string.is_empty() {
                    return Some(Node::Parameter(crate::node::Parameter {
                        value: value.to_owned(),
                        field: crate::fields::FIELD_CONTEXT.to_owned(),
                        negated,
                        annotation: annotation.clone(),
                    }));
                }
                match crate::parser::parse(&query_string, SearchType::Regex) {
                    Ok(query) => Some(Node::Operator(crate::node::Operator {
                        kind: crate::node::OperatorKind::And,
                        operands: query,
                        annotation: Default::default(),
                    })),
                    Err(e) => {
                        error = Some(e.to_string());
                        None
                    }
                }
            },
        );
        match error {
            Some(e) => Err(e),
            None => Ok(substituted),
        }
    })
}

/// `For`: normalization followed by the search-type-specific pattern
/// processing.
pub fn for_search_type(search_type: SearchType) -> Step {
    Box::new(move |nodes| {
        let mut nodes = lowercase_field_names(nodes);
        nodes = substitute_aliases(&nodes, search_type);
        nodes = substitute_count_all(&nodes);
        let processed = match search_type {
            SearchType::Standard | SearchType::CodyContext => substitute_concat(&nodes, &standard),
            SearchType::Literal => substitute_concat(&nodes, &space),
            SearchType::Regex => {
                let nodes = escape_parens_heuristic(&nodes);
                substitute_concat(&nodes, &fuzzy_regexp)
            }
            SearchType::Structural => {
                let nodes = label_structural(&nodes);
                let nodes = ellipses_for_holes(&nodes);
                substitute_concat(&nodes, &space)
            }
            SearchType::Keyword => substitute_concat_for_keyword(&nodes, &and),
        };
        Ok(processed)
    })
}

/// `Init`: parse the input and run the search-type normalization.
pub fn init(input: String, search_type: SearchType) -> Step {
    let parser: Step = Box::new(move |_nodes| {
        crate::parser::parse(&input, search_type).map_err(|e| e.to_string())
    });
    sequence(vec![parser, for_search_type(search_type)])
}

/// `InitLiteral`.
pub fn init_literal(input: String) -> Step {
    init(input, SearchType::Literal)
}

/// `InitRegexp`.
pub fn init_regexp(input: String) -> Step {
    init(input, SearchType::Regex)
}

/// `InitStructural`.
pub fn init_structural(input: String) -> Step {
    init(input, SearchType::Structural)
}

/// `Run`.
pub fn run(step: Step) -> Result<Vec<Node>, String> {
    step(&[])
}

/// `Pipeline`: run the steps, build and validate the plan, then fold `rev:`
/// into `repo:`.
pub fn pipeline(steps: Vec<Step>) -> Result<Plan, String> {
    let nodes = sequence(steps)(&[])?;
    let plan = build_plan(&nodes);
    validate_plan(&plan)?;
    Ok(map_plan(plan, &mut concat_rev_filters))
}

/// `Pipeline(Init(input, searchType))`.
pub fn pipeline_query(input: &str, search_type: SearchType) -> Result<Plan, String> {
    pipeline(vec![init(input.to_owned(), search_type)])
}

/// `ParseRegexp`: `Run(Init(in, SearchTypeRegex))`, i.e. parsing without
/// pipeline validation.
pub fn parse_regexp(input: &str) -> Result<Vec<Node>, String> {
    run(init_regexp(input.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::plan_to_q;
    use crate::node::to_string;

    #[test]
    fn literal_pipeline_joins_contiguous_patterns() {
        let plan = pipeline_query("repo:foo a b", SearchType::Literal).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(to_string(&plan_to_q(&plan)), "(and \"repo:foo\" \"a b\")");
    }

    #[test]
    fn count_all_is_substituted() {
        let plan = pipeline_query("count:all a", SearchType::Literal).unwrap();
        assert!(plan[0].parameters.exists("count"));
        assert_eq!(plan[0].parameters.find_value("count"), "99999999");
    }

    #[test]
    fn rev_is_folded_into_repo() {
        let plan = pipeline_query("repo:foo rev:v1 a", SearchType::Literal).unwrap();
        assert_eq!(to_string(&plan_to_q(&plan)), "(and \"repo:foo@v1\" \"a\")");
    }

    #[test]
    fn or_distributes_into_a_plan() {
        let plan = pipeline_query("(repo:a or repo:b) c", SearchType::Literal).unwrap();
        assert_eq!(plan.len(), 2);
    }

    #[test]
    fn unknown_field_is_rejected() {
        // `g` is a recognized scan field but has no alias and no validator
        // case, so it reaches `unrecognized field`.
        let err = pipeline_query("g:foo a", SearchType::Literal).unwrap_err();
        assert_eq!(err, "unrecognized field \"g\"");
    }
}
