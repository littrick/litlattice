#![forbid(unsafe_code)]

//! `internal/search/query/fields.go`: the recognized field names and aliases.
//!
//! `ScanField` checks the lowercase name against this set; the parser keeps the
//! original spelling in the node (aliases are resolved later by
//! `SubstituteAliases`, which is not part of the parse contract).

pub const FIELD_DEFAULT: &str = "";
pub const FIELD_CASE: &str = "case";
pub const FIELD_REPO: &str = "repo";
pub const FIELD_FILE: &str = "file";
pub const FIELD_FORK: &str = "fork";
pub const FIELD_ARCHIVED: &str = "archived";
pub const FIELD_LANG: &str = "lang";
pub const FIELD_TYPE: &str = "type";
pub const FIELD_REPO_HAS_FILE: &str = "repohasfile";
pub const FIELD_REPO_HAS_COMMIT_AFTER: &str = "repohascommitafter";
pub const FIELD_PATTERN_TYPE: &str = "patterntype";
pub const FIELD_CONTENT: &str = "content";
pub const FIELD_VISIBILITY: &str = "visibility";
pub const FIELD_REV: &str = "rev";
pub const FIELD_CONTEXT: &str = "context";

// For diff and commit search only:
pub const FIELD_BEFORE: &str = "before";
pub const FIELD_AFTER: &str = "after";
pub const FIELD_AUTHOR: &str = "author";
pub const FIELD_COMMITTER: &str = "committer";
pub const FIELD_MESSAGE: &str = "message";

// Temporary experimental fields:
pub const FIELD_INDEX: &str = "index";
pub const FIELD_COUNT: &str = "count";
pub const FIELD_TIMEOUT: &str = "timeout";
pub const FIELD_COMBY_RULE: &str = "rule";
pub const FIELD_SELECT: &str = "select";

/// `allFields`, including aliases, at their lowercase spelling.
pub const ALL_FIELDS: &[&str] = &[
    FIELD_CASE,
    FIELD_REPO,
    "r",
    FIELD_CONTEXT,
    "g",
    FIELD_FILE,
    "f",
    "path",
    FIELD_FORK,
    FIELD_ARCHIVED,
    FIELD_LANG,
    "l",
    "language",
    FIELD_TYPE,
    FIELD_PATTERN_TYPE,
    FIELD_CONTENT,
    FIELD_VISIBILITY,
    FIELD_REPO_HAS_FILE,
    FIELD_REPO_HAS_COMMIT_AFTER,
    FIELD_BEFORE,
    "until",
    FIELD_AFTER,
    "since",
    FIELD_AUTHOR,
    FIELD_COMMITTER,
    FIELD_MESSAGE,
    "m",
    "msg",
    FIELD_INDEX,
    FIELD_COUNT,
    FIELD_TIMEOUT,
    FIELD_COMBY_RULE,
    FIELD_REV,
    "revision",
    FIELD_SELECT,
];

/// `aliases`.
pub const ALIASES: &[(&str, &str)] = &[
    ("r", FIELD_REPO),
    ("f", FIELD_FILE),
    ("path", FIELD_FILE),
    ("l", FIELD_LANG),
    ("language", FIELD_LANG),
    ("since", FIELD_AFTER),
    ("until", FIELD_BEFORE),
    ("m", FIELD_MESSAGE),
    ("msg", FIELD_MESSAGE),
    ("revision", FIELD_REV),
];

pub fn is_known_field(lowercase: &str) -> bool {
    ALL_FIELDS.contains(&lowercase)
}

/// `resolveFieldAlias`.
pub fn resolve_field_alias(field: &str) -> &str {
    ALIASES
        .iter()
        .find(|(alias, _)| *alias == field)
        .map(|(_, canonical)| *canonical)
        .unwrap_or(field)
}
