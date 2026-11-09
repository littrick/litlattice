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
//! `Plan`/`Basic` construction (`Pipeline`), repository revision parsing and
//! the JSON printers. Predicate syntax is still scanned and labeled
//! `IsPredicate` so parse trees match the baseline.

pub mod fields;
pub mod labels;
pub mod node;
pub mod parser;
pub mod predicate;
pub mod range;
pub mod transformer;

pub use labels::Labels;
pub use node::{
    to_string as query_to_string, Annotation, Node, Operator, OperatorKind, Parameter, Pattern,
};
pub use parser::{parse, ParseError, SearchType};
pub use range::{Position, Range};
