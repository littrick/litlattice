//! M9.8fr differential of `sg-jsonx` against `github.com/sourcegraph/jsonx`
//! (`ComputePropertyEdit`/`ComputePropertyRemoval`, `FormatRange`,
//! `ApplyEdits`).
//!
//! Env:
//!
//! * `SG_M9FR_DIFF=1`
//! * `M9FR_CORPUS` — JSON `[{name, text, op, path?, value?, raw?, options?}]`
//! * `M9FR_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::path::Path as FsPath;

use serde::Serialize;
use serde_json::Value;

use sg_jsonx::{
    apply_edits, compute_property_edit, compute_property_removal, format_range, EditValue,
    FormatOptions, Path, Segment,
};

#[derive(Serialize)]
struct EditJson {
    offset: i64,
    length: i64,
    content: String,
}

#[derive(Serialize)]
struct ResultJson {
    name: String,
    edits: Option<Vec<EditJson>>,
    #[serde(rename = "parseErrorCodes")]
    parse_error_codes: Option<Vec<u32>>,
    output: String,
    error: Option<String>,
}

fn default_options() -> FormatOptions {
    FormatOptions {
        tab_size: 2,
        insert_spaces: true,
        eol: "\n".to_owned(),
    }
}

fn case_options(case: &Value) -> FormatOptions {
    let Some(options) = case.get("options") else {
        return default_options();
    };
    FormatOptions {
        tab_size: options.get("tabSize").and_then(Value::as_i64).unwrap_or(2),
        insert_spaces: options
            .get("insertSpaces")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        eol: options
            .get("eol")
            .and_then(Value::as_str)
            .unwrap_or("\n")
            .to_owned(),
    }
}

fn case_path(case: &Value) -> Path {
    let Some(components) = case.get("path").and_then(Value::as_array) else {
        return Vec::new();
    };
    components
        .iter()
        .map(|component| match component {
            Value::String(property) => Segment::Property(property.clone()),
            Value::Number(index) => Segment::Index(index.as_i64().unwrap_or(0)),
            _ => panic!("invalid path component"),
        })
        .collect()
}

fn run_case(case: &Value) -> ResultJson {
    let name = case
        .get("name")
        .and_then(Value::as_str)
        .expect("name")
        .to_owned();
    let text = case
        .get("text")
        .and_then(Value::as_str)
        .expect("text")
        .to_owned();
    let op = case.get("op").and_then(Value::as_str).expect("op");
    let path = case_path(case);
    let options = case_options(case);

    let mut result = ResultJson {
        name,
        edits: None,
        parse_error_codes: None,
        output: String::new(),
        error: None,
    };

    let edits = match op {
        "edit" => {
            let value = if let Some(raw) = case.get("raw").and_then(Value::as_str) {
                EditValue::Raw(raw.to_owned())
            } else if let Some(value) = case.get("value") {
                EditValue::Json(serde_json::to_string(value).expect("value json"))
            } else {
                EditValue::Json("null".to_owned())
            };
            compute_property_edit(&text, &path, value, &options)
        }
        "remove" => compute_property_removal(&text, &path, &options),
        "format" => {
            let length = text.chars().count();
            Ok(format_range(&text, 0, length, &options))
        }
        other => panic!("unknown op {other}"),
    };

    let edits = match edits {
        Ok(edits) => edits,
        Err(err) => {
            result.error = Some(err);
            return result;
        }
    };

    let output = match apply_edits(&text, &edits) {
        Ok(output) => output,
        Err(err) => {
            result.error = Some(err);
            return result;
        }
    };

    result.edits = Some(
        edits
            .iter()
            .map(|edit| EditJson {
                offset: edit.offset,
                length: edit.length,
                content: edit.content.clone(),
            })
            .collect(),
    );
    result.parse_error_codes = Some(Vec::new());
    result.output = output;
    result
}

#[test]
fn jsonx_differential() {
    if std::env::var("SG_M9FR_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FR_CORPUS").expect("M9FR_CORPUS");
    let out = std::env::var("M9FR_OUT").expect("M9FR_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Value> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    for case in &cases {
        let result = run_case(case);
        let body = serde_json::to_string(&result).expect("encode");
        std::fs::write(
            FsPath::new(&out).join(format!("{}.json", result.name)),
            &body,
        )
        .expect("write");
        println!("RUST {} len={}", result.name, body.len());
    }
}
