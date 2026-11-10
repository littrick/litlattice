//! M9.8gp differential of the site branch of `conf.Validate` and of
//! `conf.MustValidateDefaults` against Go.
//!
//! Env:
//!
//! * `SG_M9GP_DIFF=1`
//! * `M9GP_CORPUS` — JSON `[{name, text}]`
//! * `M9GP_OUT` — output directory (one `<name>.json` per case, plus
//!   `__defaults.json` and `__must_validate.json`)

#![allow(clippy::unwrap_used)]

use std::path::Path as FsPath;

use serde::Serialize;
use serde_json::Value;

use sg_site_schema::{confdefaults, validate_default, validate_site};

#[derive(Serialize)]
struct SiteResultJson {
    name: String,
    problems: Vec<String>,
    error: String,
}

#[derive(Serialize)]
struct DefaultResultJson {
    name: String,
    problems: Vec<String>,
    error: String,
}

#[derive(Serialize)]
struct MustValidateResultJson {
    name: String,
    panicked: bool,
    message: String,
}

fn write<T: Serialize>(out: &str, name: &str, result: &T) {
    let body = serde_json::to_string(result).expect("encode");
    std::fs::write(FsPath::new(out).join(format!("{name}.json")), &body).expect("write");
    println!("RUST {name} {body}");
}

#[test]
fn site_schema_differential() {
    if std::env::var("SG_M9GP_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GP_CORPUS").expect("M9GP_CORPUS");
    let out = std::env::var("M9GP_OUT").expect("M9GP_OUT");
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
        let result = match validate_site(&text) {
            Ok(problems) => SiteResultJson {
                name: name.clone(),
                problems,
                error: String::new(),
            },
            Err(error) => SiteResultJson {
                name: name.clone(),
                problems: Vec::new(),
                error: error.to_string(),
            },
        };
        write(&out, &name, &result);
    }

    let defaults: Vec<DefaultResultJson> = confdefaults::DEFAULT_CONFIGS
        .iter()
        .map(|(name, document)| match validate_site(document) {
            Ok(problems) => DefaultResultJson {
                name: (*name).to_owned(),
                problems,
                error: String::new(),
            },
            Err(error) => DefaultResultJson {
                name: (*name).to_owned(),
                problems: Vec::new(),
                error: error.to_string(),
            },
        })
        .collect();
    write(&out, "__defaults", &defaults);

    let must_validate: Vec<MustValidateResultJson> = confdefaults::DEFAULT_CONFIGS
        .iter()
        .map(|(name, document)| match validate_default(name, document) {
            Ok(()) => MustValidateResultJson {
                name: (*name).to_owned(),
                panicked: false,
                message: String::new(),
            },
            Err(message) => MustValidateResultJson {
                name: (*name).to_owned(),
                panicked: true,
                message,
            },
        })
        .collect();
    write(&out, "__must_validate", &must_validate);
}
