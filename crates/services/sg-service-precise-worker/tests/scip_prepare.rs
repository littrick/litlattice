#![forbid(unsafe_code)]

//! M9.7e differential harness (Rust side).
//!
//! Runs [`prepare_scip_data_stream`] + [`DocumentOneShotIterator`] over the
//! baseline's checked-in `testdata/index1.scip.gz` fixture with the same
//! static directory oracle as the Go harness
//! (`internal/codeintel/uploads/internal/background/processor/scip_harness_test.go`),
//! and prints a canonical description of the result. The two outputs are
//! compared line-by-line by `test_ws/p9-go/run_scip_diff.sh`.
//!
//! Requires `SG_SCIP_FIXTURE` to point at the gzipped SCIP index; without it
//! the test is skipped so an offline `cargo test` still passes.

use std::collections::HashMap;

use sg_scip::scip::{Diagnostic, Document};
use sg_scip::Package;
use sg_service_precise_worker::pathexistence::DirectoryChildren;
use sg_service_precise_worker::prelude::UploadData;
use sg_service_precise_worker::scip_stream::{
    prepare_scip_data_stream, ProcessedMetadata, ProcessedPackageData, ProcessedScipDocument,
};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn scip_prepare_matches_go_harness() {
    let Ok(fixture) = std::env::var("SG_SCIP_FIXTURE") else {
        eprintln!("SG_SCIP_FIXTURE is not set; skipping the SCIP differential harness");
        return;
    };
    let bytes = std::fs::read(&fixture).expect("read fixture");

    let scenarios = [
        ("full", directory_children()),
        ("no-util", directory_children_without("template/src/util")),
    ];

    for (name, children) in scenarios {
        run_scenario(&bytes, name, children).await;
    }
}

async fn run_scenario(bytes: &[u8], name: &str, children: HashMap<String, Vec<String>>) {
    let stream = prepare_scip_data_stream(
        UploadData::from_bytes(bytes.to_vec()),
        "",
        move |dirnames: Vec<String>| {
            let children = children.clone();
            async move {
                let listed: DirectoryChildren = dirnames
                    .into_iter()
                    .map(|dirname| {
                        let entries = children.get(&dirname).cloned().unwrap_or_default();
                        (dirname, entries)
                    })
                    .collect();
                Ok::<DirectoryChildren, ()>(listed)
            }
        },
    )
    .await
    .expect("prepare SCIP data stream");

    print_metadata(name, &stream.metadata);

    let mut ignored: Vec<&String> = stream.document_iterator.ignore_paths().iter().collect();
    ignored.sort();
    for path in ignored {
        println!("scenario={name} ignore path={path}");
    }

    let mut packages = ProcessedPackageData::default();
    let mut documents: Vec<DocumentLine> = Vec::new();
    stream
        .document_iterator
        .visit_all_documents(&mut packages, |document: &ProcessedScipDocument| {
            documents.push(DocumentLine {
                path: document.path.clone(),
                summary: summarize(&document.path, &document.document),
                language: document.document.language.clone(),
                occurrence_count: document.document.occurrences.len(),
                symbol_count: document.document.symbols.len(),
                sha256: sha256_of(&document.document),
            });
            Ok::<(), ()>(())
        })
        .expect("visit documents");

    for document in documents {
        println!(
            "scenario={name} document path={} language={} occurrences={} symbols={} sha256={}",
            document.path,
            document.language,
            document.occurrence_count,
            document.symbol_count,
            document.sha256,
        );
        println!("scenario={name} summary {}", document.summary);
    }

    packages.normalize();
    for package in &packages.packages {
        print_package(name, package, true);
    }
    for reference in &packages.package_references {
        print_package(name, &reference.package, false);
    }
}

struct DocumentLine {
    path: String,
    language: String,
    occurrence_count: usize,
    symbol_count: usize,
    sha256: String,
    summary: String,
}

/// `sha256(proto.Marshal(document))` — the payload the baseline hashes when
/// storing a canonical document.
fn sha256_of(document: &Document) -> String {
    let encoded = sg_scip::canonicalize::encode_document(document);
    let digest = Sha256::digest(&encoded);
    format!("{digest:x}")
}

fn summarize(path: &str, document: &Document) -> String {
    let mut out = format!("path={path} language={}", document.language);

    for occurrence in &document.occurrences {
        out.push_str(&format!(
            " | occ range={} symbol={} roles={} syntax={}",
            format_i32_slice(&occurrence.range),
            occurrence.symbol,
            occurrence.symbol_roles,
            occurrence.syntax_kind,
        ));
        if !occurrence.override_documentation.is_empty() {
            out.push_str(&format!(
                " odocs={}",
                occurrence.override_documentation.join(",")
            ));
        }
        for diagnostic in &occurrence.diagnostics {
            out.push_str(&format_diagnostic(diagnostic));
        }
    }

    for symbol in &document.symbols {
        out.push_str(&format!(
            " | sym symbol={} kind={} display={} enclosing={} docs={}",
            symbol.symbol,
            symbol.kind,
            symbol.display_name,
            symbol.enclosing_symbol,
            symbol.documentation.join(","),
        ));
        for relationship in &symbol.relationships {
            out.push_str(&format!(
                " rel symbol={} refs={} impl={} typedef={} def={}",
                relationship.symbol,
                relationship.is_reference,
                relationship.is_implementation,
                relationship.is_type_definition,
                relationship.is_definition,
            ));
        }
    }

    out
}

fn format_diagnostic(diagnostic: &Diagnostic) -> String {
    format!(
        " diag sev={} tags={} msg={} code={} source={}",
        diagnostic.severity,
        format_i32_slice(&diagnostic.tags),
        diagnostic.message,
        diagnostic.code,
        diagnostic.source,
    )
}

fn format_i32_slice(values: &[i32]) -> String {
    let parts: Vec<String> = values.iter().map(i32::to_string).collect();
    format!("[{}]", parts.join(" "))
}

fn print_metadata(scenario: &str, metadata: &ProcessedMetadata) {
    println!(
        "scenario={scenario} root= metadata encoding={} tool={} version={} args={} protocol={}",
        metadata.text_document_encoding,
        metadata.tool_name,
        metadata.tool_version,
        metadata.tool_arguments.join(","),
        metadata.protocol_version,
    );
}

fn print_package(scenario: &str, package: &Package, definition: bool) {
    println!(
        "scenario={scenario} package definition={} scheme={} manager={} name={} version={}",
        definition, package.scheme, package.manager, package.name, package.version,
    );
}

fn directory_children() -> HashMap<String, Vec<String>> {
    HashMap::from([
        (String::new(), vec!["template".to_owned()]),
        ("template".to_owned(), vec!["template/src".to_owned()]),
        (
            "template/src".to_owned(),
            vec![
                "template/src/extension.ts".to_owned(),
                "template/src/indicators.ts".to_owned(),
                "template/src/language.ts".to_owned(),
                "template/src/logging.ts".to_owned(),
                "template/src/util".to_owned(),
            ],
        ),
        (
            "template/src/util".to_owned(),
            vec![
                "template/src/util/api.ts".to_owned(),
                "template/src/util/graphql.ts".to_owned(),
                "template/src/util/ix.test.ts".to_owned(),
                "template/src/util/ix.ts".to_owned(),
                "template/src/util/promise.ts".to_owned(),
                "template/src/util/uri.test.ts".to_owned(),
                "template/src/util/uri.ts".to_owned(),
            ],
        ),
    ])
}

fn directory_children_without(dirname: &str) -> HashMap<String, Vec<String>> {
    let mut children = directory_children();
    children.remove(dirname);
    children
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_slice_format_matches_go() {
        assert_eq!(format_i32_slice(&[1, 2, 3]), "[1 2 3]");
        assert_eq!(format_i32_slice(&[]), "[]");
    }
}
