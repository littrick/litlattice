//! M9.8fx differential of `Query.parseSearchQuery` (PARSE_TREE) against
//! `cmd/frontend/graphqlbackend/zz_m9fx_parse_search_query_oracle_test.go`.
//!
//! Both sides feed the same corpus through the baseline's
//! `ParseSearchQuery`/`outputParseTree` equivalents and emit
//! `{result, error}` per case, serialized with the Go-compatible encoder.
//!
//! Env:
//!
//! * `SG_M9FX_DIFF=1`
//! * `M9FX_CORPUS`, `M9FX_OUT`

#![allow(clippy::unwrap_used)]

use serde::Deserialize;
use serde_json::{Map, Value};

use sg_service_frontend::parse_search_query::{parse_search_query, ParseSearchQueryArgs};
use sg_service_frontend::settings_merge::marshal_go;

#[derive(Deserialize)]
struct Case {
    #[serde(rename = "patternType", default = "standard")]
    pattern_type: String,
    #[serde(default)]
    query: String,
    #[serde(rename = "outputPhase", default = "parse_tree")]
    output_phase: String,
    #[serde(rename = "outputFormat", default = "json")]
    output_format: String,
    #[serde(rename = "outputVerbosity", default = "basic")]
    output_verbosity: String,
}

fn standard() -> String {
    "standard".to_owned()
}

fn parse_tree() -> String {
    "PARSE_TREE".to_owned()
}

fn json() -> String {
    "JSON".to_owned()
}

fn basic() -> String {
    "BASIC".to_owned()
}

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

fn envelope(case: &Case) -> Value {
    let args = ParseSearchQueryArgs {
        query: case.query.clone(),
        pattern_type: case.pattern_type.clone(),
        output_phase: case.output_phase.clone(),
        output_format: case.output_format.clone(),
        output_verbosity: case.output_verbosity.clone(),
    };
    let mut out = Map::new();
    match parse_search_query(&args) {
        Ok(result) => {
            out.insert("result".to_owned(), Value::String(result));
            out.insert("error".to_owned(), Value::String(String::new()));
        }
        Err(error) => {
            out.insert("result".to_owned(), Value::String(String::new()));
            out.insert("error".to_owned(), Value::String(error));
        }
    }
    Value::Object(out)
}

#[test]
fn parse_search_query_harness() {
    if std::env::var("SG_M9FX_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9FX_CORPUS").expect("M9FX_CORPUS");
    let out_path = std::env::var("M9FX_OUT").expect("M9FX_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let results: Vec<Value> = corpus.cases.iter().map(envelope).collect();
    let encoded = marshal_go(&Value::Array(results));
    std::fs::write(&out_path, &encoded).expect("write parse-search-query");
    println!("RUST parse-search-query len={}", encoded.len());
}
