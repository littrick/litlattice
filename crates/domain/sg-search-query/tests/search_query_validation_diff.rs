//! M9.9e differential of `Pipeline(Init(...))` validation semantics against
//! `internal/search/query/zz_m9e_validation_oracle_test.go`.
//!
//! Both sides feed the same corpus and emit `{plan, error}` per case:
//! `plan` is the `Plan.ToQ()` tree (`Q.String()`), `error` is the validation
//! error text. Cases with an error have an empty plan.
//!
//! Env:
//!
//! * `SG_M9E_DIFF=1`
//! * `M9E_CORPUS`, `M9E_OUT`

#![allow(clippy::unwrap_used)]

use serde::Deserialize;
use serde_json::{Map, Value};

use sg_search_query::{node::to_string, pipeline, plan_to_q, SearchType};

#[derive(Deserialize)]
struct Case {
    #[serde(default)]
    query: String,
    #[serde(default = "default_type")]
    #[serde(rename = "type")]
    search_type: String,
    /// Validation-only case: the plan's `rev:at.time` folding is out of this
    /// slice (see the M9.9e overview §3), so the plan is not compared.
    #[serde(default, rename = "noPlan")]
    no_plan: bool,
}

fn default_type() -> String {
    "literal".to_owned()
}

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

fn search_type(name: &str) -> SearchType {
    match name {
        "regex" | "regexp" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "codycontext" => SearchType::CodyContext,
        "keyword" => SearchType::Keyword,
        other => panic!("unknown search type {other}"),
    }
}

fn envelope(case: &Case) -> Value {
    let mut out = Map::new();
    match pipeline::pipeline_query(&case.query, search_type(&case.search_type)) {
        Ok(plan) => {
            let tree = if case.no_plan {
                String::new()
            } else {
                to_string(&plan_to_q(&plan))
            };
            out.insert("plan".to_owned(), Value::String(tree));
            out.insert("error".to_owned(), Value::String(String::new()));
        }
        Err(error) => {
            out.insert("plan".to_owned(), Value::String(String::new()));
            out.insert("error".to_owned(), Value::String(error));
        }
    }
    Value::Object(out)
}

#[test]
fn search_query_validation_harness() {
    if std::env::var("SG_M9E_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9E_CORPUS").expect("M9E_CORPUS");
    let out_path = std::env::var("M9E_OUT").expect("M9E_OUT");
    let raw = std::fs::read_to_string(corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");
    let outputs: Vec<Value> = corpus.cases.iter().map(envelope).collect();
    std::fs::write(out_path, serde_json::to_vec(&Value::Array(outputs)).unwrap())
        .expect("write output");
}
