#![forbid(unsafe_code)]

//! `internal/search/query/predicate.go`: predicate registry and argument
//! unmarshalling.
//!
//! `ScanPredicate` labels `field:name(...)` values `IsPredicate`; during
//! validation each predicate's `Unmarshal` runs with the baseline's exact
//! acceptance rules and error text (`validatePredicate` wraps failures as
//! `invalid predicate value: %s`).

use crate::fields::{
    resolve_field_alias, FIELD_FILE, FIELD_REPO, FIELD_REV,
};
use crate::node::{go_quote, Node, OperatorKind};
use crate::parser::SearchType;

/// `DefaultPredicateRegistry`, as the predicate names accepted on each field.
pub const DEFAULT_PREDICATE_REGISTRY: &[(&str, &[&str])] = &[
    (
        FIELD_REPO,
        &[
            "contains.file",
            "has.file",
            "contains.path",
            "has.path",
            "contains.content",
            "has.content",
            "contains.commit.after",
            "has.commit.after",
            "has.description",
            "has.meta",
            "has.topic",
            // Deprecated predicates:
            "has.tag",
            "has",
            "has.key",
            "contains",
        ],
    ),
    (
        FIELD_FILE,
        &[
            "contains.content",
            "has.content",
            "has.owner",
            "has.contributor",
        ],
    ),
    (FIELD_REV, &["at.time"]),
];

/// The predicate table for `field`, after resolving an alias.
pub fn predicate_table(field: &str) -> Option<&'static [&'static str]> {
    let canonical = resolve_field_alias(field);
    DEFAULT_PREDICATE_REGISTRY
        .iter()
        .find(|(name, _)| *name == canonical)
        .map(|(_, table)| *table)
}

/// The predicate table for `field` inside an arbitrary registry (used by
/// compute's `ComputePredicateRegistry`).
pub fn registry_table(
    field: &str,
    registry: &[(&'static str, &'static [&'static str])],
) -> Option<&'static [&'static str]> {
    let canonical = resolve_field_alias(field);
    registry
        .iter()
        .find(|(name, _)| *name == canonical)
        .map(|(_, table)| *table)
}

/// `ParseAsPredicate`: split a validated `name(params)` value. The baseline
/// panics when the value does not match `^([a-z.]+)\((?s:(.*))\)$`; this port
/// returns `None` instead. `(.*)` is greedy, so `params` extends to the last
/// `)`.
pub fn parse_as_predicate(value: &str) -> Option<(String, String)> {
    let bytes = value.as_bytes();
    let name_len = bytes
        .iter()
        .take_while(|b| b.is_ascii_lowercase() || **b == b'.')
        .count();
    if name_len == 0 || bytes.get(name_len) != Some(&b'(') || bytes.last() != Some(&b')') {
        return None;
    }
    // `name(` needs at least the closing paren; `name_len + 2 <= len`.
    if value.len() < name_len + 2 {
        return None;
    }
    Some((
        value[..name_len].to_owned(),
        value[name_len + 1..value.len() - 1].to_owned(),
    ))
}

/* Unmarshal implementations */

fn parse_params(params: &str) -> Result<Vec<Node>, String> {
    crate::parser::parse(params, SearchType::Regex).map_err(|e| e.to_string())
}

fn negated_predicate(name: &str) -> String {
    format!(
        "search predicate {} does not support negation",
        go_quote(name)
    )
}

fn parse_regexp(value: &str) -> Result<(), String> {
    crate::go_regexp::compile(value)
}

fn contains_file_node(
    node: &Node,
    path: &mut String,
    content: &mut String,
) -> Result<(), String> {
    match node {
        Node::Parameter(p) => {
            if p.negated {
                return Err("predicates do not currently support negated values".to_owned());
            }
            match p.field.to_lowercase().as_str() {
                "path" => {
                    if !path.is_empty() {
                        return Err("cannot specify path multiple times".to_owned());
                    }
                    parse_regexp(&p.value).map_err(|e| {
                        format!("`contains.file` predicate has invalid `path` argument: {e}")
                    })?;
                    *path = p.value.clone();
                }
                "content" => {
                    if !content.is_empty() {
                        return Err("cannot specify content multiple times".to_owned());
                    }
                    parse_regexp(&p.value).map_err(|e| {
                        format!("`contains.file` predicate has invalid `content` argument: {e}")
                    })?;
                    *content = p.value.clone();
                }
                _ => return Err(format!("unsupported option {}", go_quote(&p.field))),
            }
        }
        Node::Pattern(p) => {
            return Err(format!(
                "prepend 'file:' or 'content:' to \"{}\" to search repositories containing files or content respectively.",
                p.value
            ))
        }
        Node::Operator(op) => {
            if op.kind == OperatorKind::Or {
                return Err("predicates do not currently support 'or' queries".to_owned());
            }
            for operand in &op.operands {
                contains_file_node(operand, path, content)?;
            }
        }
    }
    Ok(())
}

/// `RepoContainsFilePredicate.Unmarshal`, including the deprecated
/// `repo:contains.file(name.go)` fallback.
fn unmarshal_contains_file(params: &str) -> Result<(), String> {
    let nodes = parse_params(params)?;
    let mut path = String::new();
    let mut content = String::new();
    if let Err(err) = nodes
        .iter()
        .try_for_each(|node| contains_file_node(node, &mut path, &mut content))
    {
        if nodes.len() != 1 {
            return Err(err);
        }
        let Node::Pattern(pattern) = &nodes[0] else {
            return Err(err);
        };
        parse_regexp(&pattern.value)?;
        path = pattern.value.clone();
    }
    if path.is_empty() && content.is_empty() {
        return Err("one of path or content must be set".to_owned());
    }
    Ok(())
}

fn unmarshal_contains_path(params: &str) -> Result<(), String> {
    parse_regexp(params).map_err(|e| format!("contains.path argument: {e}"))?;
    if params.is_empty() {
        return Err("contains.path argument should not be empty".to_owned());
    }
    Ok(())
}

fn unmarshal_repo_contains_content(params: &str) -> Result<(), String> {
    parse_regexp(params).map_err(|e| format!("contains.content argument: {e}"))?;
    if params.is_empty() {
        return Err("contains.content argument should not be empty".to_owned());
    }
    Ok(())
}

fn unmarshal_has_description(params: &str, negated: bool) -> Result<(), String> {
    if negated {
        return Err(negated_predicate("repo:has.description"));
    }
    parse_regexp(params)
        .map_err(|e| format!("invalid repo:has.description() argument: {e}"))?;
    if params.is_empty() {
        return Err("empty repo:has.description() predicate parameter".to_owned());
    }
    Ok(())
}

fn unmarshal_has_tag(params: &str) -> Result<(), String> {
    if params.is_empty() {
        return Err("tag must be non-empty".to_owned());
    }
    Ok(())
}

/// `RepoHasMetaPredicate.Unmarshal`'s `scanLiteral`.
fn scan_meta_literal(data: &str) -> Result<(String, bool, usize), String> {
    let bytes = data.as_bytes();
    match bytes.first() {
        Some(b'"') => {
            let (s, advance) = crate::parser::scan_delimited(bytes, true, '"')?;
            return Ok((s, false, advance));
        }
        Some(b'\'') => {
            let (s, advance) = crate::parser::scan_delimited(bytes, true, '\'')?;
            return Ok((s, false, advance));
        }
        Some(b'/') => {
            let (s, advance) = crate::parser::scan_delimited(bytes, false, '/')?;
            return Ok((s, true, advance));
        }
        _ => {}
    }
    match data.find(':') {
        Some(loc) => Ok((data[..loc].to_owned(), false, loc)),
        None => Ok((data.to_owned(), false, data.len())),
    }
}

fn unmarshal_has_meta(params: &str) -> Result<(), String> {
    let params = params.trim_matches(|c| c == ' ' || c == '\t');
    let (key, _is_regexp, advance) = scan_meta_literal(params)?;
    if key.is_empty() {
        return Err("key cannot be empty".to_owned());
    }
    let params = &params[advance..];
    if let Some(params) = params.strip_prefix(':') {
        let (_value, _is_regexp, advance) = scan_meta_literal(params)?;
        if !params[advance..].is_empty() {
            return Err("unexpected extra content".to_owned());
        }
    }
    Ok(())
}

/// `RepoHasKVPPredicate.Unmarshal`'s `scanLiteral` (quotes or `:`).
fn scan_kvp_literal(data: &str) -> Result<(String, usize), String> {
    let bytes = data.as_bytes();
    match bytes.first() {
        Some(b'"') => {
            let (s, advance) = crate::parser::scan_delimited(bytes, true, '"')?;
            return Ok((s, advance));
        }
        Some(b'\'') => {
            let (s, advance) = crate::parser::scan_delimited(bytes, true, '\'')?;
            return Ok((s, advance));
        }
        _ => {}
    }
    match data.find(':') {
        Some(loc) => Ok((data[..loc].to_owned(), loc)),
        None => Ok((data.to_owned(), data.len())),
    }
}

fn unmarshal_has_kvp(params: &str) -> Result<(), String> {
    let params = params.trim_matches(|c| c == ' ' || c == '\t');
    let (key, advance) = scan_kvp_literal(params)?;
    let params = &params[advance..];
    let Some(params) = params.strip_prefix(':') else {
        return Err("expected params of the form key:value".to_owned());
    };
    let (_value, advance) = scan_kvp_literal(params)?;
    if !params[advance..].is_empty() {
        return Err("unexpected extra content".to_owned());
    }
    if key.is_empty() {
        return Err("key cannot be empty".to_owned());
    }
    Ok(())
}

fn unmarshal_has_key(params: &str) -> Result<(), String> {
    if params.is_empty() {
        return Err("key must be non-empty".to_owned());
    }
    Ok(())
}

fn unmarshal_has_topic(params: &str) -> Result<(), String> {
    if params.is_empty() {
        return Err("topic must be non-empty".to_owned());
    }
    Ok(())
}

fn contains_node(
    node: &Node,
    file: &mut String,
    content: &mut String,
) -> Result<(), String> {
    match node {
        Node::Parameter(p) => {
            if p.negated {
                return Err(
                    "the repo:contains() predicate does not currently support negated values"
                        .to_owned(),
                );
            }
            match p.field.to_lowercase().as_str() {
                "file" => {
                    if !file.is_empty() {
                        return Err("cannot specify file multiple times".to_owned());
                    }
                    parse_regexp(&p.value).map_err(|e| {
                        format!("the repo:contains() predicate has invalid `file` argument: {e}")
                    })?;
                    *file = p.value.clone();
                }
                "content" => {
                    if !content.is_empty() {
                        return Err("cannot specify content multiple times".to_owned());
                    }
                    parse_regexp(&p.value).map_err(|e| {
                        format!(
                            "the repo:contains() predicate has invalid `content` argument: {e}"
                        )
                    })?;
                    *content = p.value.clone();
                }
                _ => return Err(format!("unsupported option {}", go_quote(&p.field))),
            }
        }
        Node::Pattern(p) => {
            return Err(format!(
                "prepend 'file:' or 'content:' to \"{}\" to search repositories containing files or content respectively.",
                p.value
            ))
        }
        Node::Operator(op) => {
            if op.kind == OperatorKind::Or {
                return Err("predicates do not currently support 'or' queries".to_owned());
            }
            for operand in &op.operands {
                contains_node(operand, file, content)?;
            }
        }
    }
    Ok(())
}

fn unmarshal_repo_contains(params: &str) -> Result<(), String> {
    let nodes = parse_params(params)?;
    let mut file = String::new();
    let mut content = String::new();
    for node in &nodes {
        contains_node(node, &mut file, &mut content)?;
    }
    if file.is_empty() && content.is_empty() {
        return Err("one of file or content must be set".to_owned());
    }
    Ok(())
}

fn unmarshal_file_contains_content(params: &str, negated: bool) -> Result<(), String> {
    if negated {
        return Err(negated_predicate("file:contains.content"));
    }
    parse_regexp(params)
        .map_err(|e| format!("file:contains.content argument: {e}"))?;
    if params.is_empty() {
        return Err("file:contains.content argument should not be empty".to_owned());
    }
    Ok(())
}

fn unmarshal_has_contributor(params: &str) -> Result<(), String> {
    parse_regexp(params).map_err(|e| {
        format!("the file:has.contributor() predicate has invalid argument: {e}")
    })
}

fn unmarshal_at_time(params: &str) -> Result<(), String> {
    let elems: Vec<&str> = params.split(',').collect();
    match elems.len() {
        1 => crate::validate::is_valid_git_date(elems[0].trim()),
        2 => crate::validate::is_valid_git_date(elems[0].trim()),
        _ => Err("unexpected number of arguments to rev:at.time()".to_owned()),
    }
}

/// `validatePredicate`: parse the value, look the predicate up in the default
/// registry, and run its `Unmarshal`, wrapping any failure.
pub fn validate_predicate(field: &str, value: &str, negated: bool) -> Result<(), String> {
    // `ParseAsPredicate` is guaranteed to succeed for parser-labelled
    // predicates; return an error instead of panicking if that invariant is
    // ever broken.
    let Some((name, params)) = parse_as_predicate(value) else {
        return Err("invalid predicate value: malformed predicate".to_owned());
    };
    let field = resolve_field_alias(field);
    let result = match (field, name.as_str()) {
        (FIELD_REPO, "contains.file" | "has.file") => unmarshal_contains_file(&params),
        (FIELD_REPO, "contains.path" | "has.path") => unmarshal_contains_path(&params),
        (FIELD_REPO, "contains.content" | "has.content") => {
            unmarshal_repo_contains_content(&params)
        }
        (FIELD_REPO, "contains.commit.after" | "has.commit.after") => Ok(()),
        (FIELD_REPO, "has.description") => unmarshal_has_description(&params, negated),
        (FIELD_REPO, "has.meta") => unmarshal_has_meta(&params),
        (FIELD_REPO, "has.tag") => unmarshal_has_tag(&params),
        (FIELD_REPO, "has") => unmarshal_has_kvp(&params),
        (FIELD_REPO, "has.key") => unmarshal_has_key(&params),
        (FIELD_REPO, "has.topic") => unmarshal_has_topic(&params),
        (FIELD_REPO, "contains") => unmarshal_repo_contains(&params),
        (FIELD_FILE, "contains.content" | "has.content") => {
            unmarshal_file_contains_content(&params, negated)
        }
        (FIELD_FILE, "has.owner") => Ok(()),
        (FIELD_FILE, "has.contributor") => unmarshal_has_contributor(&params),
        (FIELD_REV, "at.time") => unmarshal_at_time(&params),
        _ => {
            return Err(format!(
                "invalid predicate value: unsupported predicate {} on field {}",
                go_quote(&name),
                go_quote(field)
            ))
        }
    };
    result.map_err(|e| format!("invalid predicate value: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_as_predicate_matches_baseline_shape() {
        assert_eq!(
            parse_as_predicate("replace(a -> b)"),
            Some(("replace".to_owned(), "a -> b".to_owned()))
        );
        assert_eq!(
            parse_as_predicate("output.extra(x -> y)"),
            Some(("output.extra".to_owned(), "x -> y".to_owned()))
        );
        assert_eq!(
            parse_as_predicate("replace(foo(bar))"),
            Some(("replace".to_owned(), "foo(bar)".to_owned()))
        );
        assert_eq!(
            parse_as_predicate("replace()"),
            Some(("replace".to_owned(), String::new()))
        );
        assert_eq!(parse_as_predicate("replace(a -> b"), None);
        assert_eq!(parse_as_predicate("(a)"), None);
        assert_eq!(parse_as_predicate("Replace(a)"), None);
        assert_eq!(parse_as_predicate("replace"), None);
    }
}
