#![forbid(unsafe_code)]

//! `github.com/sourcegraph/jsonx` v0.0.0-20200629203448-1a936bd500cf
//! (ported from VS Code's tolerant JSON parser/formatter/editor).
//!
//! Ported: `scanner.go`, `visitor.go`, `tree.go`, `format.go`, `edit.go`,
//! `parser.go` — enough for `settingsMutation.editSettings`/
//! `editConfiguration` (`ComputePropertyEdit`/`ComputePropertyRemoval` +
//! `FormatEdit` + `ApplyEdits`) with `internal/conf.FormatOptions`, and for
//! `internal/jsonc.Parse` / `conf.ValidateSettings`.
//!
//! Not ported (see `m9_8fr_jsonx_overview.md` §3.1 and
//! `m9_8fs_validate_settings_overview.md` §3.1): `NodeValue`, `Segment`
//! JSON encoding, the stringer `String()` implementations for the numeric
//! enums (only `NodeType::as_str` and [`ParseError::code_name`] are needed)
//! and the `json.Marshal` byte output of `Parse`.

pub mod edit;
pub mod format;
pub mod parse;
pub mod scanner;
pub mod tree;
pub mod visitor;

pub use edit::{
    apply_edits, compute_property_edit, compute_property_removal, format_edit, go_json_string,
    Edit, EditValue,
};
pub use format::{format, format_range, settings_format_options, FormatOptions};
pub use parse::{parse, parse_codes, JsonValue, ParseError};
pub use scanner::{ScanErrorCode, ScanOptions, Scanner, SyntaxKind};
pub use tree::{
    find_node_at_location, object_property_names, parse_tree, property_path, Literal, Node,
    NodeType, Path, Segment, Tree,
};
pub use visitor::{walk, ParseErrorCode, ParseOptions, Visitor};
