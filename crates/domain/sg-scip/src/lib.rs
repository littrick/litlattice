#![forbid(unsafe_code)]

//! SCIP index parsing and canonicalization.
//!
//! This crate is the Rust port of `github.com/sourcegraph/scip/bindings/go/scip`
//! for the subset the precise-code-intel upload processor uses:
//!
//! - [`stream::parse_streaming`] — the streaming `IndexVisitor` parser
//!   (`parse.go`). It reads top-level `Index` fields one at a time so a large
//!   upload never has to be materialized as a single `Index` message.
//! - [`canonicalize`] — `CanonicalizeDocument` and its helpers (`flatten.go`,
//!   `sort.go`, `sanitize.go`).
//! - [`symbol::package_from_symbol`] — `ParseSymbol` restricted to what
//!   `packageFromSymbol` needs.
//! - [`symbol_formatter`] — `ParseSymbol`, `SymbolFormatter` and the ranking
//!   exporter's `canonicalizeSymbol` (no-version formatting + md5).
//!
//! The generated message types live in [`scip`] and come from the vendored
//! `proto/scip.proto` (Apache-2.0, upstream `sourcegraph/scip`).

pub mod canonicalize;
pub mod position;
pub mod stream;
pub mod symbol;
pub mod symbol_formatter;

/// The prost-generated SCIP message types (`scip.proto`).
#[allow(clippy::all)]
pub mod scip {
    include!(concat!(env!("OUT_DIR"), "/scip.rs"));
}

pub use prost::Message;
pub use scip::*;

pub use canonicalize::canonicalize_document;
pub use position::{Position, Range};
pub use stream::{parse_streaming, IndexEvent, IndexVisitor, ParseError, StreamingReader};
pub use symbol::{is_local_symbol, package_from_symbol, Package};
pub use symbol_formatter::{
    canonicalize_symbol, parse_symbol, Descriptor, DescriptorSuffix, ParseSymbolError,
    ParsedSymbol, SymbolFormatter, SymbolPackage, SKIP_PREFIX,
};
