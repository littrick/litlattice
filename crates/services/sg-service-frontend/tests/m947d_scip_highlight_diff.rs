//! M9.47d differential: SCIP-document consumption in `HighlightedFile`
//! (`html` / `lsif` / `lineRanges`).
//!
//! Go oracle: `cmd/frontend/internal/highlight/zz_m947d_scip_oracle_test.go`
//! (untracked, `M947D_*`-gated), in package `highlight`, constructs
//! `HighlightedCode{code, html, document}` directly and reproduces
//! `HighlightedFileResolver`'s field logic:
//!
//! * `lsif` = `jsonpb{EnumsAsInts:true,EmitDefaults:false}(response.LSIF())`,
//!   `"{}"` when the document is nil (marshal error).
//! * `html` = `response.HTML()` (`DocumentToHTML` when a document exists).
//! * `lineRanges` = `response.LinesForRanges` iff `response.LSIF() != nil`,
//!   otherwise `highlight.SplitLineRanges(response.HTML())`.
//!
//! This harness builds the same `HighlightedFile` and drives the real
//! [`HighlightedFileObject`], writing the same JSON schema.
//!
//! Corpus fields: `name`, `code`, optional `document`
//! (`{language, relative_path, occurrences:[{range, symbol, symbol_roles,
//! override_documentation, syntax_kind}]}`), optional `html` (nil-document
//! control cases) and `line_ranges` (`[{startLine,endLine}]`).
//!
//! Env: `M947D_CORPUS`, `M947D_OUT`.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use sg_service_frontend::git_comparison_graphql::{
    HighlightedFile, LsifDocument, LsifOccurrence,
};
use sg_service_frontend::git_tree_entry_graphql::HighlightedFileObject;

#[derive(Deserialize)]
struct OccSpec {
    #[serde(default)]
    range: Vec<i32>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    symbol_roles: i32,
    #[serde(default)]
    override_documentation: Vec<String>,
    #[serde(default)]
    syntax_kind: i32,
}

#[derive(Deserialize)]
struct DocSpec {
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    relative_path: Option<String>,
    #[serde(default)]
    occurrences: Vec<OccSpec>,
}

#[derive(Deserialize)]
struct LineRange {
    #[serde(rename = "startLine")]
    start_line: i32,
    #[serde(rename = "endLine")]
    end_line: i32,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    code: String,
    #[serde(default)]
    document: Option<DocSpec>,
    #[serde(default)]
    html: Option<String>,
    #[serde(default)]
    line_ranges: Vec<LineRange>,
}

fn ranges_arguments(case: &Case) -> Arguments {
    let items: Vec<InputValue> = case
        .line_ranges
        .iter()
        .map(|range| {
            let mut map = BTreeMap::new();
            map.insert(
                "startLine".to_owned(),
                InputValue::Int(i64::from(range.start_line)),
            );
            map.insert(
                "endLine".to_owned(),
                InputValue::Int(i64::from(range.end_line)),
            );
            InputValue::Object(map)
        })
        .collect();
    Arguments::from_entries(vec![("ranges".to_owned(), InputValue::List(items))])
}

fn json_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Json(Json::Str(text)) => text.clone(),
        _ => panic!("expected string field"),
    }
}

fn json_line_ranges(value: &FieldValue) -> Vec<Vec<String>> {
    match value {
        FieldValue::List { items, .. } => items
            .iter()
            .map(|item| match item {
                FieldValue::List { items, .. } => {
                    items.iter().map(json_string).collect::<Vec<String>>()
                }
                _ => panic!("expected nested list"),
            })
            .collect(),
        _ => panic!("expected list field"),
    }
}

fn build_document(spec: &DocSpec) -> LsifDocument {
    LsifDocument {
        language: spec.language.clone(),
        relative_path: spec.relative_path.clone(),
        occurrences: spec
            .occurrences
            .iter()
            .map(|occ| LsifOccurrence {
                range: occ.range.clone(),
                symbol: occ.symbol.clone(),
                symbol_roles: occ.symbol_roles,
                override_documentation: occ.override_documentation.clone(),
                syntax_kind: occ.syntax_kind,
            })
            .collect(),
    }
}

fn evaluate(case: &Case) -> (String, String, Option<Vec<Vec<String>>>, Option<String>) {
    let file = HighlightedFile {
        lines: Vec::new(),
        aborted: false,
        html: case.html.clone(),
        code: Some(case.code.clone()),
        lsif: case.document.as_ref().map(build_document),
    };
    let object = HighlightedFileObject { file };
    let none = Arguments::default();

    let html = json_string(
        &object
            .field("html", &none, Actor::Anonymous)
            .expect("html")
            .expect("html value"),
    );
    let lsif = json_string(
        &object
            .field("lsif", &none, Actor::Anonymous)
            .expect("lsif")
            .expect("lsif value"),
    );

    match object.field("lineRanges", &ranges_arguments(case), Actor::Anonymous) {
        Ok(Some(value)) => (html, lsif, Some(json_line_ranges(&value)), None),
        Ok(None) => (html, lsif, None, Some("lineRanges returned no resolver".to_owned())),
        Err(error) => (html, lsif, None, Some(error.message)),
    }
}

#[test]
fn m947d_scip_highlight_diff() {
    let corpus = match std::env::var("M947D_CORPUS") {
        Ok(path) => path,
        Err(_) => return,
    };
    let out = std::env::var("M947D_OUT").expect("M947D_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    for case in &cases {
        let (html, lsif, line_ranges, error) = evaluate(case);
        let document = json!({
            "ok": error.is_none(),
            "error": error.unwrap_or_default(),
            "html": html,
            "lsif": lsif,
            "line_ranges": match line_ranges {
                Some(ranges) => Value::Array(
                    ranges
                        .into_iter()
                        .map(|rows| Value::Array(rows.into_iter().map(Value::String).collect()))
                        .collect(),
                ),
                None => Value::Null,
            },
        });
        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            document.to_string(),
        )
        .expect("write");
        println!("RUST {}", case.name);
    }
}
