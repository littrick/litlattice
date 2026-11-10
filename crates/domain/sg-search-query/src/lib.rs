#![forbid(unsafe_code)]

//! `sg-search-query`: the search query parser.
//!
//! Port of `internal/search/query`'s parse contract: the AST ([`Node`],
//! [`Pattern`], [`Parameter`], [`Operator`]), the byte-oriented scanner
//! (`ScanAnyPattern`, `ScanBalancedPattern`, `ScanField`, `ScanValue`,
//! `ScanDelimited`, `ScanPredicate`), the recursive-descent parser with the
//! baseline heuristics, and the `Hoist` rewrite applied to unambiguous
//! `and`/`or` expressions.
//!
//! Not ported in this slice: predicate argument semantics (`Predicate.Unmarshal`),
//! repository revision parsing and `PrettyJSON`. Predicate syntax is still
//! scanned and labeled `IsPredicate` so parse trees match the baseline.
//! `ToJSON` is ported as [`to_json`] (M9.8fx).

pub mod basic;
pub mod fields;
pub mod labels;
pub mod mapper;
pub mod node;
pub mod parser;
pub mod pipeline;
pub mod predicate;
pub mod printer;
pub mod range;
pub mod transformer;
pub mod validate;

pub use basic::{plan_to_q, to_nodes, Basic, Parameters, Plan};
pub use labels::Labels;
pub use node::{
    to_string as query_to_string, Annotation, Node, Operator, OperatorKind, Parameter, Pattern,
};
pub use parser::{parse, ParseError, SearchType};
pub use pipeline::{parse_regexp, pipeline, pipeline_query};
pub use printer::{string_human, to_json};
pub use range::{Position, Range};
pub use transformer::add_regexp_field;
