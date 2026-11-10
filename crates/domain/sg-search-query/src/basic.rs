#![forbid(unsafe_code)]

//! `internal/search/query/types.go`: `Q` helpers, `Parameters`, `Basic` and
//! `Plan` (the disjoint-query plan `Pipeline` produces).
//!
//! The typed-predicate accessors `RepoHasFileContent` (and its `repo:has.*`
//! argument parsing) are ported; the remaining typed-predicate accessors
//! (`RepoHasKVPs`, …) are not.

use crate::labels::Labels;
use crate::node::{new_operator, visit_field, Annotation, Node, OperatorKind, Parameter};
use crate::transformer::hoist;

/// `query.RepoHasFileContentArgs`: one `repo:contains.*`/`repohasfile:`
/// filter. At least one of `path`/`content` is non-empty.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepoHasFileContentArgs {
    pub path: String,
    pub content: String,
    pub negated: bool,
}

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

    /// `Parameters.IncludeExcludeValues`: partition values of `field` into
    /// positive (include) and negated (exclude) values, skipping predicates.
    pub fn include_exclude_values(&self, field: &str) -> (Vec<String>, Vec<String>) {
        include_exclude_values(self, field)
    }

    /// `Parameters.RepoHasFileContent`: collect `repohasfile:`, typed
    /// `repo:contains.path`/`contains.content`/`contains.file`, and the
    /// deprecated `repo:contains` predicates in the baseline's grouping order.
    pub fn repo_has_file_content(&self) -> Result<Vec<RepoHasFileContentArgs>, String> {
        repo_has_file_content(self)
    }
}

/// `Parameters.IncludeExcludeValues`.
pub fn include_exclude_values(parameters: &Parameters, field: &str) -> (Vec<String>, Vec<String>) {
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    visit_field(
        &parameters.to_nodes(),
        field,
        &mut |value, negated, annotation| {
            if annotation.labels.is_set(Labels::IS_PREDICATE) {
                return;
            }
            if negated {
                exclude.push(value.to_owned());
            } else {
                include.push(value.to_owned());
            }
        },
    );
    (include, exclude)
}

/// The canonical `Predicate.Name()` for the `repo:` predicates this module
/// models, or `None` for any other predicate (which these accessors ignore).
fn canonical_repo_predicate(name: &str) -> Option<&'static str> {
    match name {
        "contains.path" | "has.path" => Some("contains.path"),
        "contains.content" | "has.content" => Some("contains.content"),
        "contains.file" | "has.file" => Some("contains.file"),
        "contains" => Some("contains"),
        _ => None,
    }
}

/// `syntax.Parse(pattern, syntax.Perl)` validation used by the predicate
/// `Unmarshal` methods. The baseline panics on failure (the query was
/// validated earlier); callers propagate the error instead.
fn validate_predicate_regexp(pattern: &str) -> Result<(), String> {
    regex_syntax::ast::parse::Parser::new()
        .parse(pattern)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// `Parameters.RepoHasFileContent`.
pub fn repo_has_file_content(
    parameters: &Parameters,
) -> Result<Vec<RepoHasFileContentArgs>, String> {
    let nodes = parameters.to_nodes();
    let mut result = Vec::new();

    visit_field(
        &nodes,
        crate::fields::FIELD_REPO_HAS_FILE,
        &mut |value, negated, _annotation| {
            result.push(RepoHasFileContentArgs {
                path: value.to_owned(),
                content: String::new(),
                negated,
            });
        },
    );

    // `VisitTypedPredicate` walks all `repo:` parameters in order, skipping
    // predicates whose canonical `Name()` differs from the requested one.
    let visit_typed = |canonical: &str,
                       f: &mut dyn FnMut(&str, bool) -> Result<(), String>|
     -> Result<(), String> {
        let mut error = None;
        visit_field(
            &nodes,
            crate::fields::FIELD_REPO,
            &mut |value, negated, annotation| {
                if error.is_some() || !annotation.labels.is_set(Labels::IS_PREDICATE) {
                    return;
                }
                let Some((name, args)) = crate::predicate::parse_as_predicate(value) else {
                    return;
                };
                if canonical_repo_predicate(&name) != Some(canonical) {
                    return;
                }
                if let Err(err) = f(&args, negated) {
                    error = Some(err);
                }
            },
        );
        match error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    };

    visit_typed("contains.path", &mut |args, negated| {
        validate_predicate_regexp(args)?;
        if args.is_empty() {
            return Err("contains.path argument should not be empty".to_owned());
        }
        result.push(RepoHasFileContentArgs {
            path: args.to_owned(),
            content: String::new(),
            negated,
        });
        Ok(())
    })?;

    visit_typed("contains.content", &mut |args, negated| {
        validate_predicate_regexp(args)?;
        if args.is_empty() {
            return Err("contains.content argument should not be empty".to_owned());
        }
        result.push(RepoHasFileContentArgs {
            path: String::new(),
            content: args.to_owned(),
            negated,
        });
        Ok(())
    })?;

    visit_typed("contains.file", &mut |args, negated| {
        let (path, content) = parse_contains_file_args(args)?;
        result.push(RepoHasFileContentArgs {
            path,
            content,
            negated,
        });
        Ok(())
    })?;

    visit_typed("contains", &mut |args, negated| {
        let (file, content) = parse_contains_args(args)?;
        result.push(RepoHasFileContentArgs {
            path: file,
            content,
            negated,
        });
        Ok(())
    })?;

    Ok(result)
}

/// `RepoContainsFilePredicate.Unmarshal` argument parsing (`path:`/`content:`
/// or the deprecated single-pattern form).
fn parse_contains_file_args(params: &str) -> Result<(String, String), String> {
    let nodes = crate::parser::parse(params, crate::parser::SearchType::Regex)
        .map_err(|err| err.to_string())?;
    let mut path = String::new();
    let mut content = String::new();
    match parse_contains_file_nodes(&nodes, &mut path, &mut content) {
        Ok(()) => {}
        Err(err) => {
            // Deprecated syntax `repo:contains.file(name.go)`: only fall back
            // when the parse yielded a single pattern node.
            if nodes.len() != 1 {
                return Err(err);
            }
            let Node::Pattern(pattern) = &nodes[0] else {
                return Err(err);
            };
            validate_predicate_regexp(&pattern.value)?;
            path = pattern.value.clone();
        }
    }
    if path.is_empty() && content.is_empty() {
        return Err("one of path or content must be set".to_owned());
    }
    Ok((path, content))
}

fn parse_contains_file_nodes(
    nodes: &[Node],
    path: &mut String,
    content: &mut String,
) -> Result<(), String> {
    for node in nodes {
        match node {
            Node::Parameter(parameter) => {
                if parameter.negated {
                    return Err("predicates do not currently support negated values".to_owned());
                }
                match parameter.field.to_lowercase().as_str() {
                    "path" => {
                        if !path.is_empty() {
                            return Err("cannot specify path multiple times".to_owned());
                        }
                        validate_predicate_regexp(&parameter.value).map_err(|err| {
                            format!("`contains.file` predicate has invalid `path` argument: {err}")
                        })?;
                        *path = parameter.value.clone();
                    }
                    "content" => {
                        if !content.is_empty() {
                            return Err("cannot specify content multiple times".to_owned());
                        }
                        validate_predicate_regexp(&parameter.value).map_err(|err| {
                            format!(
                                "`contains.file` predicate has invalid `content` argument: {err}"
                            )
                        })?;
                        *content = parameter.value.clone();
                    }
                    field => return Err(format!("unsupported option {field:?}")),
                }
            }
            Node::Pattern(pattern) => {
                return Err(format!(
                    "prepend 'file:' or 'content:' to \"{}\" to search repositories containing files or content respectively.",
                    pattern.value
                ));
            }
            Node::Operator(operator) => {
                if operator.kind == OperatorKind::Or {
                    return Err("predicates do not currently support 'or' queries".to_owned());
                }
                parse_contains_file_nodes(&operator.operands, path, content)?;
            }
        }
    }
    Ok(())
}

/// Deprecated `RepoContainsPredicate.Unmarshal` (`repo:contains(file:a content:b)`).
fn parse_contains_args(params: &str) -> Result<(String, String), String> {
    let nodes = crate::parser::parse(params, crate::parser::SearchType::Regex)
        .map_err(|err| err.to_string())?;
    let mut file = String::new();
    let mut content = String::new();
    parse_contains_nodes(&nodes, &mut file, &mut content)?;
    if file.is_empty() && content.is_empty() {
        return Err("one of file or content must be set".to_owned());
    }
    Ok((file, content))
}

fn parse_contains_nodes(
    nodes: &[Node],
    file: &mut String,
    content: &mut String,
) -> Result<(), String> {
    for node in nodes {
        match node {
            Node::Parameter(parameter) => {
                if parameter.negated {
                    return Err(
                        "the repo:contains() predicate does not currently support negated values"
                            .to_owned(),
                    );
                }
                match parameter.field.to_lowercase().as_str() {
                    "file" => {
                        if !file.is_empty() {
                            return Err("cannot specify file multiple times".to_owned());
                        }
                        validate_predicate_regexp(&parameter.value).map_err(|err| {
                            format!(
                                "the repo:contains() predicate has invalid `file` argument: {err}"
                            )
                        })?;
                        *file = parameter.value.clone();
                    }
                    "content" => {
                        if !content.is_empty() {
                            return Err("cannot specify content multiple times".to_owned());
                        }
                        validate_predicate_regexp(&parameter.value).map_err(|err| {
                            format!(
                                "the repo:contains() predicate has invalid `content` argument: {err}"
                            )
                        })?;
                        *content = parameter.value.clone();
                    }
                    field => return Err(format!("unsupported option {field:?}")),
                }
            }
            Node::Pattern(pattern) => {
                return Err(format!(
                    "unsupported node type Pattern: \"{}\"",
                    pattern.value
                ));
            }
            Node::Operator(operator) => {
                if operator.kind == OperatorKind::Or {
                    return Err("predicates do not currently support 'or' queries".to_owned());
                }
                parse_contains_nodes(&operator.operands, file, content)?;
            }
        }
    }
    Ok(())
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

    /// `Basic.IsCaseSensitive` (embedded `Parameters.IsCaseSensitive`).
    pub fn is_case_sensitive(&self) -> bool {
        self.parameters.is_case_sensitive()
    }

    /// `Basic.IncludeExcludeValues` (embedded `Parameters.IncludeExcludeValues`).
    pub fn include_exclude_values(&self, field: &str) -> (Vec<String>, Vec<String>) {
        self.parameters.include_exclude_values(field)
    }

    /// `Basic.RepoHasFileContent` (embedded `Parameters.RepoHasFileContent`).
    pub fn repo_has_file_content(&self) -> Result<Vec<RepoHasFileContentArgs>, String> {
        self.parameters.repo_has_file_content()
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
