#![forbid(unsafe_code)]

//! Port of `internal/codeintel/uploads/internal/background/processor/scip.go`
//! (`aggregateExternalSymbolsAndPaths`, `documentOneShotIterator`,
//! `prepareSCIPDataStream`, `processDocument`, `injectExternalSymbols`,
//! `packageFromSymbol`, `ignorePaths`).
//!
//! The SCIP payload is read twice:
//!
//! 1. **First pass** — collect the metadata, the relative path of every
//!    document (and how often it appears), and the index-level external
//!    symbols by name.
//! 2. **Ignore-path detection** — ask gitserver (through the caller-supplied
//!    `get_children` callback) which documents actually exist at the upload's
//!    commit; index entries for generated or vendored paths are skipped.
//! 3. **Second pass** — flatten repeated documents, canonicalize each document
//!    (clearing the relative path, injecting external symbols, sorting and
//!    merging occurrences/symbols), hand it to the writer callback, and
//!    accumulate the packages the index defines vs. references.
//!
//! The code-graph database writes (`writeSCIPDocuments`) and the worker loop
//! wiring are the next slice; this module stops at the document iterator so
//! the processing can be compared against the Go processor without touching
//! either database.

use std::collections::{HashMap, HashSet};
use std::io::Read;

use sg_scip::scip::{Document, Metadata, SymbolInformation};
use sg_scip::{
    canonicalize_document as scip_canonicalize_document, is_local_symbol, package_from_symbol,
    parse_streaming, IndexVisitor, Package, ParseError,
};

use crate::pathexistence::{DirectoryChildren, ExistenceChecker};
use crate::prelude::GzipSource;

/// `scip.SymbolRole_Definition`.
const SYMBOL_ROLE_DEFINITION: i32 = 0x1;

/// `codegraph.ProcessedMetadata`: the `codeintel_scip_metadata` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessedMetadata {
    pub text_document_encoding: String,
    pub tool_name: String,
    pub tool_version: String,
    pub tool_arguments: Vec<String>,
    pub protocol_version: i32,
}

/// `precise.PackageReference`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageReference {
    pub package: Package,
}

/// `codegraph.ProcessedPackageData`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessedPackageData {
    pub packages: Vec<Package>,
    pub package_references: Vec<PackageReference>,
}

impl ProcessedPackageData {
    /// `ProcessedPackageData.Normalize`: sort both lists by package
    /// (scheme, manager, name, version).
    pub fn normalize(&mut self) {
        self.packages.sort();
        self.package_references
            .sort_by(|a, b| a.package.cmp(&b.package));
    }
}

/// `codegraph.ProcessedSCIPDocument`: the canonical document plus the relative
/// path it was stored under (canonicalization clears the path inside the
/// document).
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessedScipDocument {
    pub path: String,
    pub document: Document,
}

/// `codegraph.SCIPDataStream`.
pub struct ScipDataStream {
    pub metadata: ProcessedMetadata,
    pub document_iterator: DocumentOneShotIterator,
}

/// `firstPassResult`.
#[derive(Debug, Default)]
pub struct FirstPassResult {
    pub metadata: Option<Metadata>,
    pub external_symbols_by_name: HashMap<String, SymbolInformation>,
    pub relative_paths: Vec<String>,
    pub document_count_by_path: HashMap<String, usize>,
}

/// `aggregateExternalSymbolsAndPaths`. The reader is consumed; callers that
/// need another pass open a fresh decoder.
pub fn aggregate_external_symbols_and_paths(
    reader: &mut impl Read,
) -> Result<FirstPassResult, ParseError> {
    #[derive(Default)]
    struct FirstPassVisitor {
        metadata: Option<Metadata>,
        relative_paths: Vec<String>,
        external_symbols_by_name: HashMap<String, SymbolInformation>,
        document_count_by_path: HashMap<String, usize>,
    }

    impl IndexVisitor for FirstPassVisitor {
        fn visit_metadata(&mut self, metadata: Metadata) {
            self.metadata = Some(metadata);
        }

        fn visit_document(&mut self, document: Document) {
            self.relative_paths.push(document.relative_path.clone());
            *self
                .document_count_by_path
                .entry(document.relative_path)
                .or_insert(0) += 1;
        }

        fn visit_external_symbol(&mut self, symbol: SymbolInformation) {
            self.external_symbols_by_name
                .insert(symbol.symbol.clone(), symbol);
        }
    }

    let mut visitor = FirstPassVisitor::default();
    parse_streaming(reader, &mut visitor)?;

    Ok(FirstPassResult {
        metadata: visitor.metadata,
        external_symbols_by_name: visitor.external_symbols_by_name,
        relative_paths: visitor.relative_paths,
        document_count_by_path: visitor.document_count_by_path,
    })
}

/// `ignorePaths`: the set of documents whose paths are not visible in the git
/// clone at the upload's commit.
pub async fn ignore_paths<E, F, Fut>(
    document_relative_paths: &[String],
    root: &str,
    get_children: F,
) -> Result<HashSet<String>, E>
where
    F: FnMut(Vec<String>) -> Fut,
    Fut: std::future::Future<Output = Result<DirectoryChildren, E>>,
{
    let checker = ExistenceChecker::new(root, document_relative_paths, get_children).await?;

    let mut ignore_path_set = HashSet::new();
    for path in document_relative_paths {
        if !checker.exists(path) {
            ignore_path_set.insert(path.clone());
        }
    }
    Ok(ignore_path_set)
}

/// Error from [`prepare_scip_data_stream`]. The `GetChildren` variant carries
/// the caller's callback error unchanged (the baseline returns it unwrapped).
#[derive(Debug)]
pub enum PrepareScipError<E> {
    /// Opening the gzip stream failed.
    Open(std::io::Error),
    /// The streaming parser rejected the payload (first pass).
    Parse(ParseError),
    /// `Index.metadata` is required; the baseline panics on a nil metadata
    /// pointer, the port returns an error instead.
    MissingMetadata,
    /// The `get_children` callback failed while building the existence
    /// checker.
    GetChildren(E),
}

/// Error from [`DocumentOneShotIterator::visit_all_documents`].
#[derive(Debug)]
pub enum VisitAllDocumentsError<E> {
    /// Opening a fresh gzip decoder for the second pass failed.
    Open(std::io::Error),
    /// The writer callback failed. The baseline keeps parsing the rest of the
    /// index and returns the callback's error afterwards.
    Callback(E),
}

/// `prepareSCIPDataStream`: first pass, ignore-path detection, metadata
/// extraction, and the iterator for the second pass.
pub async fn prepare_scip_data_stream<S, E, F, Fut>(
    data: S,
    root: &str,
    get_children: F,
) -> Result<ScipDataStream, PrepareScipError<E>>
where
    S: GzipSource + 'static,
    F: FnMut(Vec<String>) -> Fut,
    Fut: std::future::Future<Output = Result<DirectoryChildren, E>>,
{
    // `aggregateExternalSymbolsAndPaths` + `seekToStart`.
    let mut first_pass_reader = data.open_gzip().map_err(PrepareScipError::Open)?;
    let first_pass = aggregate_external_symbols_and_paths(&mut first_pass_reader)
        .map_err(PrepareScipError::Parse)?;

    let ignore_paths = ignore_paths(&first_pass.relative_paths, root, get_children)
        .await
        .map_err(PrepareScipError::GetChildren)?;

    let metadata = first_pass
        .metadata
        .as_ref()
        .ok_or(PrepareScipError::MissingMetadata)?;
    // `indexSummary.metadata.ToolInfo.*` is dereferenced unconditionally in the
    // baseline; an absent tool info therefore panics there. Using defaults is
    // the safer equivalent (recorded as an erratum in the design docs).
    let tool_info = metadata.tool_info.as_ref();

    let processed_metadata = ProcessedMetadata {
        text_document_encoding: text_encoding_name(metadata.text_document_encoding),
        tool_name: tool_info.map(|info| info.name.clone()).unwrap_or_default(),
        tool_version: tool_info
            .map(|info| info.version.clone())
            .unwrap_or_default(),
        tool_arguments: tool_info
            .map(|info| info.arguments.clone())
            .unwrap_or_default(),
        protocol_version: metadata.version,
    };

    Ok(ScipDataStream {
        metadata: processed_metadata,
        document_iterator: DocumentOneShotIterator {
            data: Box::new(data),
            first_pass,
            ignore_paths,
        },
    })
}

/// `textEncoding.String()`: the protobuf enum name, or the number for unknown
/// values (`protoreflect.EnumStringOf`).
fn text_encoding_name(value: i32) -> String {
    match value {
        0 => "UnspecifiedTextEncoding".to_owned(),
        1 => "UTF8".to_owned(),
        2 => "UTF16".to_owned(),
        other => other.to_string(),
    }
}

/// `documentOneShotIterator`: a single (re-openable) pass over the documents.
pub struct DocumentOneShotIterator {
    data: Box<dyn GzipSource>,
    first_pass: FirstPassResult,
    ignore_paths: HashSet<String>,
}

// Manual constructor for tests and for callers that already performed the
// first pass.
impl DocumentOneShotIterator {
    /// The relative paths the existence check found missing from the commit.
    /// Exposed for observability and the differential harness.
    pub fn ignore_paths(&self) -> &HashSet<String> {
        &self.ignore_paths
    }

    /// `VisitAllDocuments`: visit every non-ignored document exactly once, in
    /// index order, flattening repeated paths. `do_it` receives the canonical
    /// document; package data is accumulated into `packages`.
    pub fn visit_all_documents<E>(
        &self,
        packages: &mut ProcessedPackageData,
        do_it: impl FnMut(&ProcessedScipDocument) -> Result<(), E>,
    ) -> Result<(), VisitAllDocumentsError<E>> {
        let mut reader = self
            .data
            .open_gzip()
            .map_err(VisitAllDocumentsError::Open)?;

        let mut visitor = SecondPassVisitor {
            ignore_paths: &self.ignore_paths,
            first_pass: &self.first_pass,
            repeated_documents_by_path: HashMap::new(),
            package_set: HashMap::new(),
            outer_error: None,
            do_it,
        };

        let parse_result = parse_streaming(&mut reader, &mut visitor);
        if let Err(err) = &parse_result {
            // The baseline logs a warning here: a parse failure that was not
            // caught on the first pass is not returned to the caller.
            let _ = err;
        }

        let SecondPassVisitor {
            package_set,
            outer_error,
            ..
        } = visitor;

        if let Some(err) = outer_error {
            return Err(VisitAllDocumentsError::Callback(err));
        }

        // Separate the packages we define from the packages we only reference.
        // The order is unspecified until `ProcessedPackageData::Normalize`.
        for (package, has_definition) in package_set {
            if has_definition {
                packages.packages.push(package);
            } else {
                packages
                    .package_references
                    .push(PackageReference { package });
            }
        }

        Ok(())
    }
}

/// The second-pass visitor. Mirrors the closure in `VisitAllDocuments`,
/// including its error handling: after a callback error it keeps parsing but
/// still records packages for documents the callback accepted.
struct SecondPassVisitor<'a, E, D> {
    ignore_paths: &'a HashSet<String>,
    first_pass: &'a FirstPassResult,
    repeated_documents_by_path: HashMap<String, Vec<Document>>,
    package_set: HashMap<Package, bool>,
    outer_error: Option<E>,
    do_it: D,
}

impl<E, D> IndexVisitor for SecondPassVisitor<'_, E, D>
where
    D: FnMut(&ProcessedScipDocument) -> Result<(), E>,
{
    fn visit_document(&mut self, mut document: Document) {
        let path = document.relative_path.clone();
        if self.ignore_paths.contains(&path) {
            return;
        }

        if let Some(&document_count) = self.first_pass.document_count_by_path.get(&path) {
            if document_count > 1 {
                let same_path_documents = self
                    .repeated_documents_by_path
                    .entry(path.clone())
                    .or_default();
                same_path_documents.push(document);
                if same_path_documents.len() != document_count {
                    // Wait for the remaining documents with this path.
                    return;
                }

                let same_path_documents = self
                    .repeated_documents_by_path
                    .remove(&path)
                    .unwrap_or_default();
                let flattened = flatten_documents(same_path_documents);
                if flattened.len() != 1 {
                    // The baseline logs a warning and skips the document.
                    return;
                }
                document = flattened.into_iter().next().expect("one document");
            }
        }

        let processed = process_document(document, &self.first_pass.external_symbols_by_name);

        if let Err(err) = (self.do_it)(&processed) {
            self.outer_error = Some(err);
            return;
        }

        // Stash the unique packages of each symbol name in the document. An
        // occurrence that defines the symbol marks the package as defined
        // rather than merely referenced. NOTE: `processed.document` is the
        // canonicalized document, including injected external symbols, which
        // is what the baseline reads here as well.
        for symbol in &processed.document.symbols {
            if let Some(package) = package_from_symbol(&symbol.symbol) {
                self.package_set.entry(package).or_insert(false);
            }

            for relationship in &symbol.relationships {
                if let Some(package) = package_from_symbol(&relationship.symbol) {
                    self.package_set.entry(package).or_insert(false);
                }
            }
        }

        for occurrence in &processed.document.occurrences {
            if occurrence.symbol.is_empty() || is_local_symbol(&occurrence.symbol) {
                continue;
            }

            if let Some(package) = package_from_symbol(&occurrence.symbol) {
                if occurrence.symbol_roles & SYMBOL_ROLE_DEFINITION > 0 {
                    self.package_set.insert(package, true);
                } else {
                    self.package_set.entry(package).or_insert(false);
                }
            }
        }
    }
}

/// `scip.FlattenDocuments`: merge documents that share a relative path
/// (concatenating their symbols and occurrences). The language of the first
/// document wins; the baseline only compares the languages and does nothing
/// about a mismatch (TODO in upstream).
pub fn flatten_documents(documents: Vec<Document>) -> Vec<Document> {
    let mut document_map: HashMap<String, Document> = HashMap::with_capacity(documents.len());
    for document in documents {
        match document_map.get_mut(&document.relative_path) {
            None => {
                document_map.insert(document.relative_path.clone(), document);
            }
            Some(existing) => {
                existing.symbols.extend(document.symbols);
                existing.occurrences.extend(document.occurrences);
            }
        }
    }
    document_map.into_values().collect()
}

/// `processDocument`: stash the path, canonicalize, and wrap.
pub fn process_document(
    mut document: Document,
    external_symbols_by_name: &HashMap<String, SymbolInformation>,
) -> ProcessedScipDocument {
    let path = std::mem::take(&mut document.relative_path);
    canonicalize_document(&mut document, external_symbols_by_name);
    ProcessedScipDocument { path, document }
}

/// `canonicalizeDocument`: clear the relative path, inject external symbols,
/// and apply `scip.CanonicalizeDocument`.
pub fn canonicalize_document(
    document: &mut Document,
    external_symbols_by_name: &HashMap<String, SymbolInformation>,
) {
    // Stored outside the document payload so renames do not invalidate it.
    document.relative_path.clear();

    inject_external_symbols(document, external_symbols_by_name);

    scip_canonicalize_document(document);
}

/// `injectExternalSymbols`: denormalize external symbols into the document for
/// every referenced-but-not-defined symbol, following their relationships
/// breadth-first.
pub fn inject_external_symbols(
    document: &mut Document,
    external_symbols_by_name: &HashMap<String, SymbolInformation>,
) {
    // Existing definitions.
    let mut definitions_set: HashSet<String> = document
        .symbols
        .iter()
        .map(|symbol| symbol.symbol.clone())
        .collect();

    // Occurrence and relationship references.
    let mut references_set: HashSet<String> = HashSet::new();
    for symbol in &document.symbols {
        for relationship in &symbol.relationships {
            references_set.insert(relationship.symbol.clone());
        }
    }
    for occurrence in &document.occurrences {
        if occurrence.symbol.is_empty() || is_local_symbol(&occurrence.symbol) {
            continue;
        }
        references_set.insert(occurrence.symbol.clone());
    }

    // Add referenced symbols that have no definition, following the external
    // symbols' relationships as the frontier.
    while !references_set.is_empty() {
        let mut new_references_set: HashSet<String> = HashSet::new();

        for symbol_name in references_set {
            if definitions_set.contains(&symbol_name) {
                continue;
            }
            definitions_set.insert(symbol_name.clone());

            let Some(symbol) = external_symbols_by_name.get(&symbol_name) else {
                continue;
            };

            // Add the new definition for the referenced symbol.
            document.symbols.push(symbol.clone());

            for relationship in &symbol.relationships {
                new_references_set.insert(relationship.symbol.clone());
            }
        }

        references_set = new_references_set;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_scip::scip::{Occurrence, Relationship};

    fn symbol(name: &str) -> SymbolInformation {
        SymbolInformation {
            symbol: name.to_owned(),
            ..SymbolInformation::default()
        }
    }

    #[test]
    fn injects_external_symbols_breadth_first() {
        let ext_a = SymbolInformation {
            symbol: "s m n 1 A#".to_owned(),
            relationships: vec![Relationship {
                symbol: "s m n 1 B#".to_owned(),
                is_reference: true,
                ..Relationship::default()
            }],
            ..SymbolInformation::default()
        };
        let ext_b = symbol("s m n 1 B#");
        let external =
            HashMap::from([(ext_a.symbol.clone(), ext_a), (ext_b.symbol.clone(), ext_b)]);

        let mut document = Document {
            relative_path: "a.go".to_owned(),
            occurrences: vec![Occurrence {
                range: vec![0, 0, 1],
                symbol: "s m n 1 A#".to_owned(),
                ..Occurrence::default()
            }],
            ..Document::default()
        };

        canonicalize_document(&mut document, &external);

        assert_eq!(document.relative_path, "");
        let names: Vec<&str> = document.symbols.iter().map(|s| s.symbol.as_str()).collect();
        assert_eq!(names, vec!["s m n 1 A#", "s m n 1 B#"]);
    }

    #[test]
    fn local_and_empty_occurrences_are_not_injected() {
        let external = HashMap::from([(
            "s m n 1 A#".to_owned(),
            SymbolInformation {
                symbol: "s m n 1 A#".to_owned(),
                ..SymbolInformation::default()
            },
        )]);
        let mut document = Document {
            relative_path: "a.go".to_owned(),
            occurrences: vec![
                Occurrence {
                    range: vec![0, 0, 1],
                    symbol: "local 1".to_owned(),
                    ..Occurrence::default()
                },
                Occurrence {
                    range: vec![1, 0, 1],
                    symbol: String::new(),
                    ..Occurrence::default()
                },
            ],
            ..Document::default()
        };

        canonicalize_document(&mut document, &external);
        assert!(document.symbols.is_empty());
    }

    #[test]
    fn flatten_documents_concatenates_same_path() {
        let documents = vec![
            Document {
                relative_path: "a.go".to_owned(),
                occurrences: vec![Occurrence {
                    range: vec![0, 0, 1],
                    ..Occurrence::default()
                }],
                ..Document::default()
            },
            Document {
                relative_path: "a.go".to_owned(),
                occurrences: vec![Occurrence {
                    range: vec![1, 0, 1],
                    ..Occurrence::default()
                }],
                ..Document::default()
            },
        ];

        let flattened = flatten_documents(documents);
        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].occurrences.len(), 2);
    }
}
