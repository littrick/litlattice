#![forbid(unsafe_code)]

//! `internal/compute`: the compute query language and its parser (`Parse`).
//!
//! Compute queries reuse the search-query AST: a query is a single pattern
//! (the compute match pattern) plus the remaining search parameters. A
//! pattern that came from `content:` may instead be a compute predicate
//! (`replace(...)`, `output(...)`); otherwise the query is a `MatchOnly`
//! command. Only parsing is ported here: `Command.Run` needs a gitserver
//! client and match results, which belong to a later slice.
//!
//! The baseline compiles match patterns with `grafana/regexp` (Go/RE2
//! syntax); this port validates them with `regex-syntax`. Validity agrees for
//! the supported subset, but compile-error text differs; see docs §85.

use sg_search_query::basic::{plan_to_q, Basic, Plan};
use sg_search_query::fields::{FIELD_CONTENT, FIELD_SELECT, FIELD_TYPE};
use sg_search_query::labels::Labels;
use sg_search_query::mapper::map_pattern;
use sg_search_query::node::{
    visit_field, visit_pattern, Annotation, Node, Operator, OperatorKind, Pattern,
};
use sg_search_query::parser::{scan_balanced_parens, scan_predicate_name, SearchType};
use sg_search_query::predicate::{parse_as_predicate, registry_table};
use sg_search_query::{parse_regexp, pipeline_query, query_to_string, string_human};

/// `ComputePredicateRegistry`: the predicate names compute recognizes on
/// `content:`. Each maps to `EmptyPredicate`; the arguments are interpreted by
/// [`parse_content_predicate`].
pub const COMPUTE_PREDICATE_REGISTRY: &[(&str, &[&str])] = &[(
    FIELD_CONTENT,
    &[
        "replace",
        "replace.regexp",
        "replace.structural",
        "output",
        "output.regexp",
        "output.structural",
        "output.extra",
    ],
)];

/// `Query`: a compute command plus the search parameters it runs with.
#[derive(Debug, Clone)]
pub struct Query {
    pub command: Command,
    pub parameters: Vec<Node>,
}

impl Query {
    /// `Query.String`.
    pub fn string(&self) -> String {
        if self.parameters.is_empty() {
            format!("Command: `{}`", self.command.string())
        } else {
            format!(
                "Command: `{}`, Parameters: `{}`",
                self.command.string(),
                string_human(&self.parameters)
            )
        }
    }

    /// `Query.ToSearchQuery`: `(and <parameters> <search pattern>)`.
    pub fn to_search_query(&self) -> String {
        let mut operands = self.parameters.clone();
        operands.push(Node::Pattern(Pattern {
            value: self.command.to_search_pattern().to_owned(),
            negated: false,
            annotation: Annotation::default(),
        }));
        let expression = [Node::Operator(Operator {
            kind: OperatorKind::And,
            operands,
            annotation: Annotation::default(),
        })];
        string_human(&expression)
    }
}

/// `MatchPattern`: a raw regexp or comby template. The baseline holds a
/// compiled `*regexp.Regexp`; only its source string is observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchPattern {
    /// `Regexp` (Go/RE2 syntax, validated with `regex-syntax` here).
    Regexp(String),
    /// `Comby` structural template (not validated).
    Comby(String),
}

impl MatchPattern {
    /// The underlying pattern source.
    pub fn value(&self) -> &str {
        match self {
            MatchPattern::Regexp(value) | MatchPattern::Comby(value) => value,
        }
    }
}

/// `Command`: how compute processes a match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// `MatchOnly`.
    MatchOnly {
        search_pattern: MatchPattern,
        /// `(?i:<pattern>)` when the query is not case sensitive.
        compute_pattern: MatchPattern,
    },
    /// `Replace`.
    Replace {
        search_pattern: MatchPattern,
        replace_pattern: String,
    },
    /// `Output`.
    Output {
        search_pattern: MatchPattern,
        output_pattern: String,
        separator: String,
        type_value: String,
        selector: String,
        kind: String,
    },
}

impl Command {
    /// `Command.ToSearchPattern`.
    pub fn to_search_pattern(&self) -> &str {
        match self {
            Command::MatchOnly { search_pattern, .. }
            | Command::Replace { search_pattern, .. }
            | Command::Output { search_pattern, .. } => search_pattern.value(),
        }
    }

    /// `Command.String`.
    pub fn string(&self) -> String {
        match self {
            Command::MatchOnly {
                search_pattern,
                compute_pattern,
            } => format!(
                "Match only search pattern: {}, compute pattern: {}",
                search_pattern.value(),
                compute_pattern.value()
            ),
            Command::Replace {
                search_pattern,
                replace_pattern,
            } => format!(
                "Replace in place: ({}) -> ({})",
                search_pattern.value(),
                replace_pattern
            ),
            Command::Output {
                search_pattern,
                output_pattern,
                separator,
                ..
            } => format!(
                "Output with separator: ({}) -> ({}) separator: {}",
                search_pattern.value(),
                output_pattern,
                separator
            ),
        }
    }
}

/// `extractPattern`: require exactly one non-negated pattern.
fn extract_pattern(basic: &Basic) -> Result<Pattern, String> {
    let Some(node) = &basic.pattern else {
        return Err("compute endpoint expects nonempty pattern".to_owned());
    };
    let mut result: Option<Pattern> = None;
    let mut error: Option<String> = None;
    let mut seen = false;
    visit_pattern(
        std::slice::from_ref(node),
        &mut |value, negated, annotation| {
            if error.is_some() {
                return;
            }
            if negated {
                error = Some("compute endpoint expects a nonnegated pattern".to_owned());
                return;
            }
            if seen {
                error = Some(
                    "compute endpoint only supports one search pattern currently ('and' or 'or' operators are not supported yet)"
                        .to_owned(),
                );
                return;
            }
            result = Some(Pattern {
                value: value.to_owned(),
                negated,
                annotation: annotation.clone(),
            });
            seen = true;
        },
    );
    if let Some(error) = error {
        return Err(error);
    }
    result.ok_or_else(|| "compute endpoint expects nonempty pattern".to_owned())
}

/// `toRegexpPattern`: validate and keep the source string. The baseline error
/// text comes from Go's regexp compiler and is not byte-compatible here.
fn to_regexp_pattern(value: &str) -> Result<MatchPattern, String> {
    regex_syntax::ast::parse::Parser::new()
        .parse(value)
        .map_err(|err| format!("compute endpoint: {err}"))?;
    Ok(MatchPattern::Regexp(value.to_owned()))
}

/// `ScanPredicate("content", buf, ComputePredicateRegistry)`.
fn scan_content_predicate(buf: &[u8]) -> Option<String> {
    let table = registry_table(FIELD_CONTENT, COMPUTE_PREDICATE_REGISTRY)?;
    let (name, name_advance) = scan_predicate_name(buf, table)?;
    let buf = &buf[name_advance..];
    if buf.first() != Some(&b'(') {
        return None;
    }
    let (params, _) = scan_balanced_parens(buf)?;
    Some(format!("{name}{params}"))
}

/// `parseContentPredicate`: a `content:` pattern holding a compute predicate.
fn parse_content_predicate(pattern: &Pattern) -> Option<(String, String)> {
    if !pattern.annotation.labels.is_set(Labels::IS_CONTENT) {
        // Not set via `content:`, so it cannot be a compute command.
        return None;
    }
    let value = scan_content_predicate(pattern.value.as_bytes())?;
    parse_as_predicate(&value)
}

/// `arrowSyntax.Split(args, 2)` for `\s*->\s*` (Go's ASCII `\s`).
fn parse_arrow_syntax(args: &str) -> Result<(String, String), String> {
    const NO_ARROW: &str = "invalid arrow statement, no left and right hand sides of `->`";
    let Some(index) = args.find("->") else {
        return Err(NO_ARROW.to_owned());
    };
    let bytes = args.as_bytes();
    let is_space = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\x0c' | b'\r');
    let mut start = index;
    while start > 0 && is_space(bytes[start - 1]) {
        start -= 1;
    }
    let mut end = index + 2;
    while end < bytes.len() && is_space(bytes[end]) {
        end += 1;
    }
    Ok((args[..start].to_owned(), args[end..].to_owned()))
}

fn parse_replace(basic: &Basic) -> Result<Option<Command>, String> {
    let pattern = extract_pattern(basic)?;
    let Some((name, args)) = parse_content_predicate(&pattern) else {
        return Ok(None);
    };
    let (left, right) = parse_arrow_syntax(&args)?;

    let search_pattern = match name.as_str() {
        "replace" | "replace.regexp" => {
            to_regexp_pattern(&left).map_err(|err| format!("replace command: {err}"))?
        }
        // Structural search doesn't validate a match pattern.
        "replace.structural" => MatchPattern::Comby(left),
        _ => return Ok(None),
    };

    Ok(Some(Command::Replace {
        search_pattern,
        replace_pattern: right,
    }))
}

fn parse_output(basic: &Basic) -> Result<Option<Command>, String> {
    let pattern = extract_pattern(basic)?;
    let Some((name, args)) = parse_content_predicate(&pattern) else {
        return Ok(None);
    };
    let (left, right) = parse_arrow_syntax(&args)?;

    let search_pattern = match name.as_str() {
        "output" | "output.regexp" | "output.extra" => {
            to_regexp_pattern(&left).map_err(|err| format!("output command: {err}"))?
        }
        "output.structural" => MatchPattern::Comby(left),
        _ => return Ok(None),
    };

    let tree = basic.to_parse_tree();
    let mut type_value = String::new();
    visit_field(&tree, FIELD_TYPE, &mut |value, _, _| {
        type_value = value.to_owned();
    });
    let mut selector = String::new();
    visit_field(&tree, FIELD_SELECT, &mut |value, _, _| {
        selector = value.to_owned();
    });

    // The default separator is newline and cannot be changed currently.
    Ok(Some(Command::Output {
        search_pattern,
        output_pattern: right,
        separator: "\n".to_owned(),
        type_value,
        selector,
        kind: name,
    }))
}

fn parse_match_only(basic: &Basic) -> Result<Option<Command>, String> {
    let pattern = extract_pattern(basic)?;

    let search_pattern = to_regexp_pattern(&pattern.value)?;
    let compute_pattern = if basic.parameters.is_case_sensitive() {
        search_pattern.clone()
    } else {
        to_regexp_pattern(&format!("(?i:{})", pattern.value))?
    };

    Ok(Some(Command::MatchOnly {
        search_pattern,
        compute_pattern,
    }))
}

/// `first(parseReplace, parseOutput, parseMatchOnly)`.
fn parse_command(basic: &Basic) -> Result<Command, String> {
    for parser in [parse_replace, parse_output, parse_match_only] {
        if let Some(command) = parser(basic)? {
            return Ok(command);
        }
    }
    Err(format!(
        "could not parse valid compute command from query {}",
        basic_string(basic)
    ))
}

/// `Basic.String`: `Q.String` of the parameters, a space, then the pattern.
fn basic_string(basic: &Basic) -> String {
    let parameters = query_to_string(&basic.parameters.to_nodes());
    match &basic.pattern {
        Some(pattern) => format!(
            "{parameters} {}",
            query_to_string(std::slice::from_ref(pattern))
        ),
        None => parameters,
    }
}

fn to_compute_query(plan: &Plan) -> Result<Query, String> {
    let Some(first) = plan.first() else {
        return Err("compute endpoint can't do anything with empty query".to_owned());
    };

    let command = parse_command(first)?;
    let parameters = map_pattern(&plan_to_q(plan), &mut |_, _, _| None);
    Ok(Query {
        command,
        parameters,
    })
}

/// `compute.Parse`.
pub fn parse(query: &str) -> Result<Query, String> {
    let parse_tree = parse_regexp(query)?;

    let mut seen_patterns = 0;
    visit_pattern(&parse_tree, &mut |_, _, _| seen_patterns += 1);
    if seen_patterns > 1 {
        return Err(
            "compute endpoint cannot currently support expressions in patterns containing 'and', 'or', 'not' (or negation) right now!"
                .to_owned(),
        );
    }

    let plan = pipeline_query(query, SearchType::Regex)?;
    to_compute_query(&plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_string(input: &str) -> String {
        match parse(input) {
            Ok(parsed) => parsed.string(),
            Err(err) => err,
        }
    }

    fn search_query(input: &str) -> String {
        match parse(input) {
            Ok(parsed) => parsed.to_search_query(),
            Err(err) => err,
        }
    }

    // Expectations from `internal/compute/query_test.go` (autogold).
    #[test]
    fn parse_matches_baseline_examples() {
        assert_eq!(
            query_string("not a(foo)"),
            "compute endpoint expects a nonnegated pattern"
        );
        assert_eq!(
            query_string("content:'foo'"),
            "Command: `Match only search pattern: foo, compute pattern: (?i:foo)`"
        );
        assert_eq!(
            query_string("milk case:yes"),
            "Command: `Match only search pattern: milk, compute pattern: milk`, Parameters: `case:yes`"
        );
        assert_eq!(
            query_string("repo:cool"),
            "compute endpoint expects nonempty pattern"
        );
        assert_eq!(
            query_string("a or b"),
            "compute endpoint cannot currently support expressions in patterns containing 'and', 'or', 'not' (or negation) right now!"
        );
        assert_eq!(
            query_string("content:replace(sourcegraph -> smorgasboard)"),
            "Command: `Replace in place: (sourcegraph) -> (smorgasboard)`"
        );
        assert_eq!(
            query_string("content:replace(a -> b -> c)"),
            "Command: `Replace in place: (a) -> (b -> c)`"
        );
        assert_eq!(
            query_string("content:replace(a->b)"),
            "Command: `Replace in place: (a) -> (b)`"
        );
        assert_eq!(
            query_string("content:replace(->b)"),
            "Command: `Replace in place: () -> (b)`"
        );
    }

    #[test]
    fn to_search_query_matches_baseline_examples() {
        assert_eq!(
            search_query("repo:foo file:bar carolado"),
            "(repo:foo file:bar AND carolado)"
        );
        assert_eq!(
            search_query("content:replace(colarado -> colorodo) repo:foo file:bar"),
            "(repo:foo file:bar AND colarado)"
        );
        assert_eq!(
            search_query(
                "content:replace(colarado -> colorodo) repo:foo file:bar (lang:go or lang:text)"
            ),
            "((repo:foo file:bar lang:go OR repo:foo file:bar lang:text) AND colarado)"
        );
    }
}
