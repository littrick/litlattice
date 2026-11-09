#![forbid(unsafe_code)]

//! `internal/search/query/predicate.go`: the recognized predicate names.
//!
//! Parsing only needs the name table: `ScanPredicate` accepts `field:name(...)`
//! as a value and labels it `IsPredicate`; the predicates' arguments are
//! validated/interpreted later (not ported in this slice).

use crate::fields::{resolve_field_alias, FIELD_FILE, FIELD_REPO, FIELD_REV};

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
