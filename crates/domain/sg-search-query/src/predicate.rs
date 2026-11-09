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
