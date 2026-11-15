//! M9.48b differential: Code Intel code-navigation conversions and document
//! consumption (`GitBlob.lsif` / `GitBlobLSIFData`).
//!
//! Go oracle (untracked, `M948B_*`-gated) lives in three packages:
//!
//! * `internal/codeintel/codenav/internal/lsifstore/zz_m948b_codenav_oracle_test.go`
//!   (`extract`, `doc_queries`, `hover`, `find_occurrences`);
//! * `internal/codeintel/codenav/transport/graphql/zz_m948b_codenav_oracle_test.go`
//!   (`cursor`, `severity_roles`);
//! * `internal/codeintel/codenav/zz_m948b_codenav_oracle_test.go`
//!   (`translate`, `snapshot`, `sort_dedup`).
//!
//! This harness drives the real Rust helpers used by
//! [`sg_service_frontend::codeintel_codenav_graphql`] and writes the same
//! normalized JSON schema.
//!
//! Env: `M948B_CORPUS`, `M948B_OUT`.

#![allow(clippy::unwrap_used)]

use serde::Deserialize;
use serde_json::{json, Value};

use sg_scip::scip;
use sg_service_frontend::codeintel_codenav_graphql::{
    self, CompactHunk, UploadLocation, UploadRecord,
};
use sg_service_frontend::git_tree_entry_graphql::{Position, SymbolRange as GqlRange};

#[derive(Deserialize)]
struct Range {
    start: Pos,
    end: Pos,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
struct Pos {
    line: i32,
    character: i32,
}

#[derive(Deserialize)]
struct HunkSpec {
    #[serde(rename = "origStartLine")]
    orig_start_line: i32,
    #[serde(rename = "origLines")]
    orig_lines: i32,
    #[serde(rename = "newStartLine")]
    new_start_line: i32,
    #[serde(rename = "newLines")]
    new_lines: i32,
}

#[derive(Deserialize, Default)]
struct DiagnosticSpec {
    #[serde(default)]
    severity: i32,
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    source: String,
}

#[derive(Deserialize)]
struct OccurrenceSpec {
    #[serde(default)]
    range: Vec<i32>,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    symbol_roles: i32,
    #[serde(default)]
    override_documentation: Vec<String>,
    #[serde(default)]
    diagnostics: Vec<DiagnosticSpec>,
}

#[derive(Deserialize, Default)]
struct SignatureSpec {
    #[serde(default)]
    language: String,
    #[serde(default)]
    text: String,
}

#[derive(Deserialize, Default)]
struct RelationshipSpec {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    is_reference: bool,
    #[serde(default)]
    is_implementation: bool,
    #[serde(default)]
    is_definition: bool,
    #[serde(default)]
    is_type_definition: bool,
}

#[derive(Deserialize, Default)]
struct SymbolSpec {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    documentation: Vec<String>,
    #[serde(default)]
    relationships: Vec<RelationshipSpec>,
    #[serde(default)]
    signature_documentation: Option<SignatureSpec>,
}

#[derive(Deserialize, Default)]
struct DocumentSpec {
    #[serde(default)]
    language: String,
    #[serde(default)]
    relative_path: String,
    #[serde(default)]
    occurrences: Vec<OccurrenceSpec>,
    #[serde(default)]
    symbols: Vec<SymbolSpec>,
}

#[derive(Deserialize)]
struct LocationSpec {
    repository: String,
    path: String,
    #[serde(rename = "uploadId")]
    upload_id: i64,
    range: Range,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    hunks: Vec<HunkSpec>,
    #[serde(default)]
    positions: Vec<Pos>,
    #[serde(default)]
    ranges: Vec<Range>,
    #[serde(default)]
    document: Option<DocumentSpec>,
    #[serde(default)]
    lookup: Option<OccurrenceSpec>,
    #[serde(default)]
    occurrences: Vec<OccurrenceSpec>,
    #[serde(default)]
    line: i32,
    #[serde(default)]
    character: i32,
    #[serde(default)]
    start_line: i32,
    #[serde(default)]
    end_line: i32,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    occurrence_index: usize,
    #[serde(default)]
    severity: i32,
    #[serde(default)]
    symbol_roles: i32,
    #[serde(default)]
    file: String,
    #[serde(default)]
    locations: Vec<LocationSpec>,
}

fn gql_range(range: &Range) -> GqlRange {
    GqlRange {
        start: Position {
            line: range.start.line,
            character: range.start.character,
        },
        end: Position {
            line: range.end.line,
            character: range.end.character,
        },
    }
}

fn gql_range_vec(range: GqlRange) -> Vec<i32> {
    codeintel_codenav_graphql::scip_range_from_symbol(range)
}

/// Four-element form used by the Go store methods.
fn range_vec4(range: GqlRange) -> Vec<i32> {
    vec![
        range.start.line,
        range.start.character,
        range.end.line,
        range.end.character,
    ]
}

fn occurrence(spec: &OccurrenceSpec) -> scip::Occurrence {
    scip::Occurrence {
        range: spec.range.clone(),
        symbol: spec.symbol.clone(),
        symbol_roles: spec.symbol_roles,
        override_documentation: spec.override_documentation.clone(),
        syntax_kind: 0,
        diagnostics: spec
            .diagnostics
            .iter()
            .map(|diagnostic| scip::Diagnostic {
                severity: diagnostic.severity,
                code: diagnostic.code.clone(),
                message: diagnostic.message.clone(),
                source: diagnostic.source.clone(),
                tags: Vec::new(),
            })
            .collect(),
        enclosing_range: Vec::new(),
    }
}

fn build_document(spec: &DocumentSpec) -> scip::Document {
    scip::Document {
        language: spec.language.clone(),
        relative_path: spec.relative_path.clone(),
        occurrences: spec.occurrences.iter().map(occurrence).collect(),
        symbols: spec
            .symbols
            .iter()
            .map(|symbol| scip::SymbolInformation {
                symbol: symbol.symbol.clone(),
                documentation: symbol.documentation.clone(),
                relationships: symbol
                    .relationships
                    .iter()
                    .map(|relationship| scip::Relationship {
                        symbol: relationship.symbol.clone(),
                        is_reference: relationship.is_reference,
                        is_implementation: relationship.is_implementation,
                        is_type_definition: relationship.is_type_definition,
                        is_definition: relationship.is_definition,
                    })
                    .collect(),
                kind: 0,
                display_name: String::new(),
                signature_documentation: symbol.signature_documentation.as_ref().map(
                    |signature| scip::Document {
                        language: signature.language.clone(),
                        text: signature.text.clone(),
                        ..Default::default()
                    },
                ),
                enclosing_symbol: String::new(),
            })
            .collect(),
        text: String::new(),
        position_encoding: 0,
    }
}

fn usage_builders_json(builders: &[codeintel_codenav_graphql::UsageBuilder]) -> Value {
    Value::Array(
        builders
            .iter()
            .map(|builder| {
                json!({
                    "range": gql_range_vec(builder.range),
                    "symbol": builder.symbol,
                    "symbol_roles": builder.symbol_roles,
                })
            })
            .collect(),
    )
}

fn evaluate(case: &Case) -> Value {
    match case.kind.as_str() {
        "translate" => {
            let hunks: Vec<CompactHunk> = case
                .hunks
                .iter()
                .map(|hunk| {
                    codeintel_codenav_graphql::new_compact_hunk(
                        &sg_service_gitserver::diff::Hunk {
                            orig_start_line: hunk.orig_start_line,
                            orig_lines: hunk.orig_lines,
                            new_start_line: hunk.new_start_line,
                            new_lines: hunk.new_lines,
                            ..Default::default()
                        },
                    )
                })
                .collect();
            let positions: Vec<Value> = case
                .positions
                .iter()
                .map(|position| {
                    match codeintel_codenav_graphql::translate_position(
                        &hunks,
                        Position {
                            line: position.line,
                            character: position.character,
                        },
                    ) {
                        Some(position) => json!({"line": position.line, "character": position.character}),
                        None => Value::Null,
                    }
                })
                .collect();
            let ranges: Vec<Value> = case
                .ranges
                .iter()
                .map(|range| match codeintel_codenav_graphql::translate_range(&hunks, gql_range(range)) {
                    Some(range) => json!({"start": {"line": range.start.line, "character": range.start.character}, "end": {"line": range.end.line, "character": range.end.character}}),
                    None => Value::Null,
                })
                .collect();
            json!({"positions": positions, "ranges": ranges})
        }
        "extract" => {
            let document = build_document(case.document.as_ref().expect("document"));
            let lookup = occurrence(case.lookup.as_ref().expect("lookup"));
            let data = codeintel_codenav_graphql::extract_occurrence_data(&document, &lookup);
            json!({
                "definitions": usage_builders_json(&data.definitions),
                "references": usage_builders_json(&data.references),
                "implementations": usage_builders_json(&data.implementations),
                "prototypes": usage_builders_json(&data.prototypes),
                "hover_text": data.hover_text,
            })
        }
        "doc_queries" => {
            let document = build_document(case.document.as_ref().expect("document"));
            let stencil: Vec<Value> = codeintel_codenav_graphql::get_stencil(&document)
                .into_iter()
                .map(|range| json!(range_vec4(range)))
                .collect();
            let ranges: Vec<Value> = codeintel_codenav_graphql::get_ranges(
                &document,
                case.start_line,
                case.end_line,
            )
            .into_iter()
            .map(|range| {
                json!({
                    "range": range_vec4(range.range),
                    "definitions": usage_builders_json(&range.definitions),
                    "references": usage_builders_json(&range.references),
                    "implementations": usage_builders_json(&range.implementations),
                    "hover_text": range.hover_text,
                })
            })
            .collect();
            let mut offset = 0;
            let mut records = Vec::new();
            let total_count = codeintel_codenav_graphql::collect_diagnostics(
                &document,
                &document.relative_path,
                &mut offset,
                case.limit,
                &mut records,
            );
            let diagnostics: Vec<Value> = records
                .into_iter()
                .map(|record| {
                    json!({
                        "range": range_vec4(record.range),
                        "severity": record.severity,
                        "code": record.code,
                        "message": record.message,
                        "source": record.source,
                        "path": record.path,
                    })
                })
                .collect();
            json!({
                "stencil": stencil,
                "ranges": ranges,
                "diagnostics": diagnostics,
                "total_count": total_count,
            })
        }
        "hover" => {
            let document = build_document(case.document.as_ref().expect("document"));
            match codeintel_codenav_graphql::get_hover(&document, case.line, case.character) {
                Some((text, range)) => json!({
                    "exists": true,
                    "text": text,
                    "range": range_vec4(range),
                }),
                None => json!({"exists": false, "text": "", "range": Value::Null}),
            }
        }
        "find_occurrences" => {
            let occurrences: Vec<scip::Occurrence> =
                case.occurrences.iter().map(occurrence).collect();
            let indices: Vec<i64> = codeintel_codenav_graphql::find_occurrences(
                &occurrences,
                case.line,
                case.character,
            )
            .into_iter()
            .map(|found| {
                let base = occurrences.as_ptr() as usize;
                let address = found as *const scip::Occurrence as usize;
                ((address - base) / std::mem::size_of::<scip::Occurrence>()) as i64
            })
            .collect();
            json!({"indices": indices})
        }
        "cursor" => {
            let decoded = match codeintel_codenav_graphql::decode_cursor(case.value.as_deref()) {
                Ok(decoded) => Value::String(decoded),
                Err(_error) => json!({"error": true}),
            };
            let encoded = codeintel_codenav_graphql::encode_cursor(case.value.as_deref());
            let marshaled = codeintel_codenav_graphql::marshal_occurrence_cursor(
                case.occurrence_index,
            );
            let unmarshaled = codeintel_codenav_graphql::unmarshal_occurrence_cursor(&marshaled)
                .unwrap_or(usize::MAX);
            json!({
                "decoded": decoded,
                "encoded": encoded,
                "occ_marshaled": marshaled,
                "occ_unmarshaled": unmarshaled,
            })
        }
        "severity_roles" => {
            let severity = codeintel_codenav_graphql::to_severity(case.severity);
            json!({
                "severity": severity,
                "roles": codeintel_codenav_graphql::occurrence_roles(case.symbol_roles),
            })
        }
        "snapshot" => {
            let document = build_document(case.document.as_ref().expect("document"));
            let data = codeintel_codenav_graphql::snapshot_for_document(
                &document,
                &case.file,
                &|position| Some(position),
            );
            json!({
                "data": data
                    .into_iter()
                    .map(|data| json!({
                        "offset": data.document_offset,
                        "symbol": data.symbol,
                        "additional": if data.additional_data.is_empty() { Value::Null } else { json!(data.additional_data) },
                    }))
                    .collect::<Vec<Value>>(),
            })
        }
        "sort_dedup" => {
            let mut locations: Vec<UploadLocation> = case
                .locations
                .iter()
                .map(|location| UploadLocation {
                    upload: UploadRecord {
                        id: location.upload_id,
                        repository_id: 1,
                        repository_name: location.repository.clone(),
                        commit: "c".to_owned(),
                        root: String::new(),
                        indexer: "scip-go".to_owned(),
                        indexer_version: String::new(),
                    },
                    path: location.path.clone(),
                    target_commit: "t".to_owned(),
                    range: gql_range(&location.range),
                })
                .collect();
            locations = codeintel_codenav_graphql::sort_and_dedup_locations(locations);
            json!({
                "locations": locations
                    .into_iter()
                    .map(|location| json!({
                        "repository": location.upload.repository_name,
                        "path": location.path,
                        "uploadId": location.upload.id,
                        "range": {"start": {"line": location.range.start.line, "character": location.range.start.character}, "end": {"line": location.range.end.line, "character": location.range.end.character}},
                    }))
                    .collect::<Vec<Value>>(),
            })
        }
        other => json!({"skipped": other}),
    }
}

#[test]
fn m948b_git_blob_lsif_data_diff() {
    let corpus = match std::env::var("M948B_CORPUS") {
        Ok(path) => path,
        Err(_) => return,
    };
    let out = std::env::var("M948B_OUT").expect("M948B_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    for case in &cases {
        let document = evaluate(case);
        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            document.to_string(),
        )
        .expect("write");
        println!("RUST {}", case.name);
    }
}
