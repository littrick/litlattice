#![forbid(unsafe_code)]

//! Port of `canonicalize.go`, `flatten.go`, `sort.go` and `sanitize.go`.
//!
//! Canonicalization gives equivalent document payloads one deterministic
//! form, so they hash to the same value when stored in the code-intel
//! database, and lets consumers binary-search canonical documents.

use std::cmp::Ordering;
use std::collections::HashMap;

use prost::Message;

use crate::position::Range;
use crate::scip::{Diagnostic, Document, Occurrence, Relationship, SymbolInformation};

/// `CanonicalizeDocument`.
pub fn canonicalize_document(document: &mut Document) {
    document.occurrences = canonicalize_occurrences(std::mem::take(&mut document.occurrences));
    document.symbols = canonicalize_symbols(std::mem::take(&mut document.symbols));
    sanitize_document(document);
}

/// `CanonicalizeOccurrences`.
pub fn canonicalize_occurrences(occurrences: Vec<Occurrence>) -> Vec<Occurrence> {
    let mut canonicalized = flatten_occurrences(remove_illegal_occurrences(occurrences))
        .into_iter()
        .map(canonicalize_occurrence)
        .collect::<Vec<_>>();
    sort_occurrences(&mut canonicalized);
    canonicalized
}

/// `RemoveIllegalOccurrences`: drop occurrences whose range is not exactly
/// three or four components.
pub fn remove_illegal_occurrences(occurrences: Vec<Occurrence>) -> Vec<Occurrence> {
    occurrences
        .into_iter()
        .filter(|occurrence| occurrence.range.len() == 3 || occurrence.range.len() == 4)
        .collect()
}

/// `CanonicalizeOccurrence`.
pub fn canonicalize_occurrence(mut occurrence: Occurrence) -> Occurrence {
    occurrence.range = Range::new_unchecked(&occurrence.range).scip_range();
    occurrence.diagnostics = canonicalize_diagnostics(std::mem::take(&mut occurrence.diagnostics));
    occurrence
}

/// `CanonicalizeDiagnostics`.
pub fn canonicalize_diagnostics(diagnostics: Vec<Diagnostic>) -> Vec<Diagnostic> {
    let mut diagnostics = diagnostics
        .into_iter()
        .map(canonicalize_diagnostic)
        .collect::<Vec<_>>();
    diagnostics.sort_by(|a, b| {
        a.severity
            .cmp(&b.severity)
            .then_with(|| a.message.cmp(&b.message))
    });
    diagnostics
}

/// `CanonicalizeDiagnostic`.
pub fn canonicalize_diagnostic(mut diagnostic: Diagnostic) -> Diagnostic {
    diagnostic.tags.sort_unstable();
    diagnostic
}

/// `CanonicalizeSymbols`.
pub fn canonicalize_symbols(symbols: Vec<SymbolInformation>) -> Vec<SymbolInformation> {
    let mut symbols = flatten_symbols(symbols)
        .into_iter()
        .map(canonicalize_symbol)
        .collect::<Vec<_>>();
    symbols.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    symbols
}

/// `CanonicalizeSymbol`.
pub fn canonicalize_symbol(mut symbol: SymbolInformation) -> SymbolInformation {
    symbol.relationships = canonicalize_relationships(std::mem::take(&mut symbol.relationships));
    symbol
}

/// `CanonicalizeRelationships`.
pub fn canonicalize_relationships(relationships: Vec<Relationship>) -> Vec<Relationship> {
    let mut relationships = flatten_relationships(relationships);
    relationships.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    relationships
}

/// `FlattenOccurrences`: merge occurrences with equivalent ranges and symbols.
pub fn flatten_occurrences(mut occurrences: Vec<Occurrence>) -> Vec<Occurrence> {
    if occurrences.is_empty() {
        return occurrences;
    }

    sort_occurrences(&mut occurrences);

    let mut flattened: Vec<Occurrence> = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let Some(top) = flattened.last_mut() else {
            flattened.push(occurrence);
            continue;
        };
        if !raw_ranges_equal(&top.range, &occurrence.range) || top.symbol != occurrence.symbol {
            flattened.push(occurrence);
            continue;
        }

        if top.syntax_kind == 0 {
            // Take the first valid syntax kind.
            top.syntax_kind = occurrence.syntax_kind;
        }
        top.symbol_roles |= occurrence.symbol_roles;
        top.override_documentation
            .extend(occurrence.override_documentation);
        top.diagnostics.extend(occurrence.diagnostics);
    }

    flattened
}

/// `FlattenSymbols`: merge symbols with the same name.
pub fn flatten_symbols(symbols: Vec<SymbolInformation>) -> Vec<SymbolInformation> {
    let mut symbol_map: HashMap<String, SymbolInformation> = HashMap::with_capacity(symbols.len());
    for symbol in symbols {
        match symbol_map.get_mut(&symbol.symbol) {
            None => {
                symbol_map.insert(symbol.symbol.clone(), symbol);
            }
            Some(existing) => {
                existing.documentation =
                    combine_documentation(&existing.documentation, &symbol.documentation);
                existing.relationships.extend(symbol.relationships);
            }
        }
    }
    symbol_map.into_values().collect()
}

/// `FlattenRelationship`.
pub fn flatten_relationships(relationships: Vec<Relationship>) -> Vec<Relationship> {
    let mut relationship_map: HashMap<String, Vec<Relationship>> =
        HashMap::with_capacity(relationships.len());
    for relationship in relationships {
        relationship_map
            .entry(relationship.symbol.clone())
            .or_default()
            .push(relationship);
    }

    relationship_map
        .into_values()
        .filter_map(|group| {
            // `into_iter` rather than `drain(..).next()`: dropping a
            // partially-consumed `Drain` discards the remaining elements.
            let mut group = group.into_iter();
            let mut combined = group.next()?;
            for relationship in group {
                combined.is_reference |= relationship.is_reference;
                combined.is_implementation |= relationship.is_implementation;
                combined.is_type_definition |= relationship.is_type_definition;
                combined.is_definition |= relationship.is_definition;
            }
            Some(combined)
        })
        .collect()
}

/// `SortOccurrences`: ascending range start, enclosing ranges first; ties by
/// symbol name.
pub fn sort_occurrences(occurrences: &mut [Occurrence]) {
    occurrences.sort_by(|a, b| {
        let ordering =
            Range::new_unchecked(&a.range).compare_strict(&Range::new_unchecked(&b.range));
        if ordering != Ordering::Equal {
            ordering
        } else {
            a.symbol.cmp(&b.symbol)
        }
    });
}

/// `rawRangesEqual`: equal encodings, or equal decoded start/end positions.
fn raw_ranges_equal(a: &[i32], b: &[i32]) -> bool {
    if a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x == y) {
        return true;
    }
    let (ra, rb) = (Range::new_unchecked(a), Range::new_unchecked(b));
    ra.start == rb.start && ra.end == rb.end
}

/// `combineDocumentation`: append components not already present.
fn combine_documentation(existing: &[String], additional: &[String]) -> Vec<String> {
    let mut combined = existing.to_vec();
    for documentation in additional {
        if !combined.contains(documentation) {
            combined.push(documentation.clone());
        }
    }
    combined
}

/// `SanitizeDocument` is a no-op in Rust: protobuf string fields are `String`
/// (always valid UTF-8), whereas Go byte-strings can hold invalid UTF-8.
pub fn sanitize_document(_document: &mut Document) {}

/// The serialized payload the code-intel database stores for a canonical
/// document (`proto.Marshal` in the baseline).
pub fn encode_document(document: &Document) -> Vec<u8> {
    document.encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn occurrence(range: Vec<i32>, symbol: &str, roles: i32) -> Occurrence {
        Occurrence {
            range,
            symbol: symbol.to_owned(),
            symbol_roles: roles,
            ..Occurrence::default()
        }
    }

    #[test]
    fn canonicalize_sorts_and_merges_occurrences() {
        let canonical = canonicalize_occurrences(vec![
            occurrence(vec![1, 0, 2], "b", 0),
            occurrence(vec![1, 0, 2], "b", 8),
            occurrence(vec![0, 0, 1], "a", 1),
            occurrence(vec![2, 0], "illegal", 1),
            occurrence(vec![3, 0, 3, 4], "c", 0),
        ]);

        assert_eq!(canonical.len(), 3);
        assert_eq!(canonical[0].range, vec![0, 0, 1]);
        assert_eq!(canonical[1].range, vec![1, 0, 2]);
        assert_eq!(canonical[1].symbol_roles, 8);
        assert_eq!(canonical[2].range, vec![3, 0, 4]);
    }

    #[test]
    fn canonicalize_merges_symbols_and_relationships() {
        let canonical = canonicalize_symbols(vec![
            SymbolInformation {
                symbol: "s".to_owned(),
                documentation: vec!["one".to_owned()],
                relationships: vec![Relationship {
                    symbol: "r".to_owned(),
                    is_reference: true,
                    ..Relationship::default()
                }],
                ..SymbolInformation::default()
            },
            SymbolInformation {
                symbol: "s".to_owned(),
                documentation: vec!["one".to_owned(), "two".to_owned()],
                relationships: vec![Relationship {
                    symbol: "r".to_owned(),
                    is_definition: true,
                    ..Relationship::default()
                }],
                ..SymbolInformation::default()
            },
        ]);

        assert_eq!(canonical.len(), 1);
        assert_eq!(canonical[0].documentation, vec!["one", "two"]);
        assert_eq!(canonical[0].relationships.len(), 1);
        assert!(canonical[0].relationships[0].is_reference);
        assert!(canonical[0].relationships[0].is_definition);
    }

    #[test]
    fn diagnostics_sort_by_severity_then_message() {
        let diagnostics = canonicalize_diagnostics(vec![
            Diagnostic {
                severity: 2,
                message: "b".to_owned(),
                tags: vec![2, 1],
                ..Diagnostic::default()
            },
            Diagnostic {
                severity: 2,
                message: "a".to_owned(),
                ..Diagnostic::default()
            },
            Diagnostic {
                severity: 1,
                message: "z".to_owned(),
                ..Diagnostic::default()
            },
        ]);

        assert_eq!(diagnostics[0].message, "z");
        assert_eq!(diagnostics[1].message, "a");
        assert_eq!(diagnostics[2].message, "b");
        assert_eq!(diagnostics[2].tags, vec![1, 2]);
    }
}
