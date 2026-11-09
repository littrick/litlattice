#![forbid(unsafe_code)]

//! `internal/search/query/validate.go`: plan/parameter validation.
//!
//! Ported: field uniqueness/negation/boolean/number/duration/yes-no checks,
//! structural labels, repo-rev pairing, commit parameters, ref-free
//! type:`symbol` pairing, and pure-pattern checks.
//!
//! Not ported (accepted by this port where the baseline would reject):
//! regexp compilation (`repo:`/`file:`/`author:`… values), language-name
//! lookup, `select:` path validation, git date parsing (`before:`/`after:`),
//! predicate argument unmarshalling, and `refGlobs`/`index:only` checks. Those
//! need regexp/git-date/predicate subsystems; see docs §83.3.

use crate::basic::Plan;
use crate::fields;
use crate::node::{exists, go_quote, visit_parameter, visit_pattern, Node, Parameter};
use std::collections::HashSet;

/// `parseBool`: `strconv.ParseBool` plus y/yes/n/no.
pub fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "y" | "yes" => return Ok(true),
        "n" | "no" => return Ok(false),
        _ => {}
    }
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("invalid boolean {}", go_quote(s))),
    }
}

/// `YesNoOnly`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YesNoOnly {
    Yes,
    No,
    Only,
    Invalid,
}

/// `parseYesNoOnly`.
pub fn parse_yes_no_only(s: &str) -> YesNoOnly {
    match s {
        "y" | "Y" | "yes" | "YES" | "Yes" => YesNoOnly::Yes,
        "n" | "N" | "no" | "NO" | "No" => YesNoOnly::No,
        "o" | "only" | "ONLY" | "Only" => YesNoOnly::Only,
        _ => match parse_bool(s) {
            Ok(true) => YesNoOnly::Yes,
            Ok(false) => YesNoOnly::No,
            Err(_) => YesNoOnly::Invalid,
        },
    }
}

fn is_singular(field: &str, seen: &mut HashSet<String>) -> Result<(), String> {
    if seen.contains(field) {
        return Err(format!(
            "field {} may not be used more than once",
            go_quote(field)
        ));
    }
    Ok(())
}

fn is_not_negated(field: &str, negated: bool) -> Result<(), String> {
    if negated {
        return Err(format!(
            "field {} does not support negation",
            go_quote(field)
        ));
    }
    Ok(())
}

fn is_boolean(value: &str) -> Result<(), String> {
    parse_bool(value).map(|_| ())
}

fn is_number(field: &str, value: &str) -> Result<(), String> {
    match value.parse::<i64>() {
        Ok(count) => {
            if count > i32::MAX as i64 || count < i32::MIN as i64 {
                return Err(format!(
                    "field {field} has a value that is out of range, try making it smaller"
                ));
            }
            if count <= 0 {
                return Err(format!("field {field} requires a positive number"));
            }
            Ok(())
        }
        Err(_) => Err(format!(
            "field {field} has value {value}, {value} is not a number"
        )),
    }
}

/// A `time.ParseDuration`-compatible validator (no value is returned).
fn is_duration(value: &str) -> Result<(), String> {
    let invalid = || {
        "invalid value for field 'timeout' (examples: \"timeout:2s\", \"timeout:200ms\")".to_owned()
    };
    let mut chars = value.chars().peekable();
    if matches!(chars.peek(), Some('+') | Some('-')) {
        chars.next();
    }
    if value == "0" || value == "+0" || value == "-0" {
        return Ok(());
    }
    loop {
        let mut digits = String::new();
        let mut saw_digit = false;
        while let Some(c) = chars.peek() {
            if c.is_ascii_digit() {
                saw_digit = true;
                digits.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        if chars.peek() == Some(&'.') {
            digits.push('.');
            chars.next();
            while let Some(c) = chars.peek() {
                if c.is_ascii_digit() {
                    saw_digit = true;
                    digits.push(*c);
                    chars.next();
                } else {
                    break;
                }
            }
        }
        if !saw_digit {
            return Err(invalid());
        }
        let mut unit = String::new();
        while let Some(c) = chars.peek() {
            if c.is_ascii_alphabetic() || *c == 'µ' {
                unit.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        if unit.is_empty() {
            return Err(invalid());
        }
        match unit.as_str() {
            "ns" | "us" | "µs" | "ms" | "s" | "m" | "h" => {}
            _ => return Err(invalid()),
        }
        if chars.peek().is_none() {
            break;
        }
    }
    Ok(())
}

fn is_yes_no_only(field: &str, value: &str) -> Result<(), String> {
    if parse_yes_no_only(value) == YesNoOnly::Invalid {
        return Err(format!(
            "invalid value {} for field {}. Valid values are: yes, only, no",
            go_quote(value),
            go_quote(field)
        ));
    }
    Ok(())
}

fn is_unrecognized_field(field: &str) -> Result<(), String> {
    Err(format!("unrecognized field {}", go_quote(field)))
}

/// `validateField`.
pub fn validate_field(
    field: &str,
    value: &str,
    negated: bool,
    seen: &mut HashSet<String>,
) -> Result<(), String> {
    match field {
        fields::FIELD_DEFAULT => Ok(()),
        fields::FIELD_CASE => {
            is_singular(field, seen)?;
            is_boolean(value)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_REPO => {
            // isValidRepoRegexp: not ported.
            Ok(())
        }
        fields::FIELD_CONTEXT => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_FILE => {
            // isValidRegexp: not ported.
            Ok(())
        }
        fields::FIELD_LANG => {
            // isLanguage: not ported.
            Ok(())
        }
        fields::FIELD_TYPE => is_not_negated(field, negated),
        fields::FIELD_PATTERN_TYPE | fields::FIELD_CONTENT | fields::FIELD_VISIBILITY => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_REPO_HAS_FILE => {
            // isValidRegexp: not ported.
            Ok(())
        }
        fields::FIELD_REPO_HAS_COMMIT_AFTER => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_BEFORE | fields::FIELD_AFTER => {
            is_not_negated(field, negated) // isValidGitDate: not ported.
        }
        fields::FIELD_AUTHOR | fields::FIELD_COMMITTER | fields::FIELD_MESSAGE => {
            // isValidRegexp: not ported.
            Ok(())
        }
        fields::FIELD_INDEX | fields::FIELD_FORK | fields::FIELD_ARCHIVED => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)?;
            is_yes_no_only(field, value)
        }
        fields::FIELD_COUNT => {
            is_singular(field, seen)?;
            is_number(field, value)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_COMBY_RULE => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_TIMEOUT => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)?;
            is_duration(value)
        }
        fields::FIELD_REV => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_SELECT => {
            is_singular(field, seen)?;
            is_not_negated(field, negated) // isValidSelect: not ported.
        }
        _ => is_unrecognized_field(field),
    }
}

/// `validateParameters`.
pub fn validate_parameters(nodes: &[Node]) -> Result<(), String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut error: Option<String> = None;
    visit_parameter(nodes, &mut |field, value, negated, annotation| {
        if error.is_some() {
            return;
        }
        let result = if annotation
            .labels
            .is_set(crate::labels::Labels::IS_PREDICATE)
        {
            // validatePredicate: argument unmarshalling not ported.
            Ok(())
        } else {
            validate_field(field, value, negated, &mut seen)
        };
        seen.insert(field.to_owned());
        if let Err(e) = result {
            error = Some(e);
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `validatePattern`.
pub fn validate_pattern(nodes: &[Node]) -> Result<(), String> {
    let mut error: Option<String> = None;
    visit_pattern(nodes, &mut |_value, negated, annotation| {
        if error.is_some() {
            return;
        }
        // Regexp compilation is not ported.
        if annotation.labels.is_set(crate::labels::Labels::STRUCTURAL) && negated {
            error = Some(
                "the query contains a negated search pattern. Structural search does not support negated search patterns at the moment"
                    .to_owned(),
            );
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `validateRepoRevPair`.
pub fn validate_repo_rev_pair(nodes: &[Node]) -> Result<(), String> {
    let mut seen_repo_with_commit = false;
    let mut seen_repo = false;
    let mut seen_empty_repo = false;
    crate::node::visit_field(nodes, fields::FIELD_REPO, &mut |value, negated, _| {
        seen_repo = true;
        seen_empty_repo = value.is_empty();
        if !negated && value.contains('@') {
            seen_repo_with_commit = true;
        }
    });
    let rev_specified = exists(
        nodes,
        &mut |node| matches!(node, Node::Parameter(Parameter { field, .. }) if field == fields::FIELD_REV),
    );
    if seen_repo_with_commit && rev_specified {
        return Err("invalid syntax. You specified both @ and rev: for a repo: filter and I don't know how to interpret this. Remove either @ or rev: and try again".to_owned());
    }
    if !seen_repo && rev_specified {
        return Err(
            "invalid syntax. The query contains `rev:` without `repo:`. Add a `repo:` filter and try again"
                .to_owned(),
        );
    }
    if seen_empty_repo && rev_specified {
        return Err("invalid syntax. The query contains `rev:` but `repo:` is empty. Add a non-empty `repo:` filter and try again".to_owned());
    }
    Ok(())
}

/// `validateCommitParameters`.
pub fn validate_commit_parameters(nodes: &[Node]) -> Result<(), String> {
    let mut seen_commit_param = String::new();
    let mut type_commit_exists = false;
    visit_parameter(nodes, &mut |field, value, _negated, _| {
        if matches!(
            field,
            fields::FIELD_AUTHOR
                | fields::FIELD_BEFORE
                | fields::FIELD_AFTER
                | fields::FIELD_MESSAGE
        ) {
            seen_commit_param = field.to_owned();
        }
        if field == fields::FIELD_TYPE && (value == "commit" || value == "diff") {
            type_commit_exists = true;
        }
    });
    if !seen_commit_param.is_empty() && !type_commit_exists {
        return Err(format!(
            "your query contains the field '{seen_commit_param}', which requires type:commit or type:diff in the query"
        ));
    }
    Ok(())
}

/// `validateTypeStructural`.
pub fn validate_type_structural(nodes: &[Node]) -> Result<(), String> {
    let mut seen_structural = false;
    let mut seen_type = false;
    let mut type_diff = false;
    let invalid = exists(nodes, &mut |node| {
        if let Node::Pattern(p) = node {
            if p.annotation
                .labels
                .is_set(crate::labels::Labels::STRUCTURAL)
            {
                seen_structural = true;
            }
        }
        if let Node::Parameter(p) = node {
            if p.field == fields::FIELD_TYPE {
                seen_type = true;
                type_diff = p.value == "diff";
            }
        }
        seen_structural && seen_type
    });
    if invalid {
        let mut basic = "this structural search query specifies `type:` and is not supported. Structural search syntax only applies to searching file contents".to_owned();
        if type_diff {
            basic.push_str(" and is not currently supported for diff searches");
        }
        return Err(basic);
    }
    Ok(())
}

/// `validateRepoHasFile`.
pub fn validate_repo_has_file(nodes: &[Node]) -> Result<(), String> {
    let mut seen_repo_has_file = false;
    let mut seen_type_symbol = false;
    visit_parameter(nodes, &mut |field, value, _negated, _| {
        if field == fields::FIELD_REPO_HAS_FILE {
            seen_repo_has_file = true;
        }
        if field == fields::FIELD_TYPE && value.eq_ignore_ascii_case("symbol") {
            seen_type_symbol = true;
        }
    });
    if seen_repo_has_file && seen_type_symbol {
        return Err("repohasfile is not compatible for type:symbol. Subscribe to https://github.com/sourcegraph/sourcegraph/issues/4610 for updates".to_owned());
    }
    Ok(())
}

/// `validate`.
pub fn validate(nodes: &[Node]) -> Result<(), String> {
    validate_parameters(nodes)?;
    validate_pattern(nodes)?;
    validate_repo_rev_pair(nodes)?;
    validate_repo_has_file(nodes)?;
    validate_commit_parameters(nodes)?;
    validate_type_structural(nodes)?;
    // validateRefGlobs: needs ParseRepositoryRevisions/ContainsRefGlobs.
    Ok(())
}

/// `ValidatePlan`.
pub fn validate_plan(plan: &Plan) -> Result<(), String> {
    for basic in plan {
        validate(&basic.to_parse_tree())?;
    }
    Ok(())
}
