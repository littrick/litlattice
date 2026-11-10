//! M9.8fs differential of `validate_settings` against Go's
//! `conf.ValidateSettings` (JSONC parse + gojsonschema settings schema).
//!
//! Env:
//!
//! * `SG_M9FS_DIFF=1`
//! * `M9FS_CORPUS` — JSON `[{name, text}]`
//! * `M9FS_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::path::Path as FsPath;

use serde::Serialize;
use serde_json::Value;

use sg_settings_schema::validate_settings;

#[derive(Serialize)]
struct ResultJson {
    name: String,
    problems: Vec<String>,
}

#[test]
fn validate_settings_differential() {
    if std::env::var("SG_M9FS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9FS_CORPUS").expect("M9FS_CORPUS");
    let out = std::env::var("M9FS_OUT").expect("M9FS_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Value> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    for case in &cases {
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
        let result = ResultJson {
            name: name.clone(),
            problems: validate_settings(&text),
        };
        let body = serde_json::to_string(&result).expect("encode");
        std::fs::write(FsPath::new(&out).join(format!("{name}.json")), &body).expect("write");
        println!("RUST {name} len={}", body.len());
    }
}
