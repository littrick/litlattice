//! M9.8ho differential harness: construct the job tree for the ported
//! `job.go` entry points and emit normalized JSON for a byte-for-byte
//! comparison with the Go oracle
//! (`internal/search/job/jobutil/zz_m9ho_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9HO_DIFF=1` enable the harness
//! * `M9HO_CORPUS` corpus JSON
//! * `M9HO_OUT` output JSON

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use sg_search_query::basic::Basic;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_service_frontend::search_client::{Features, Protocol};
use sg_service_frontend::search_job::JobError;
use sg_service_frontend::search_jobutil_job_builders as jb;
use sg_service_frontend::search_jobutil_tree::{
    new_basic_job, new_flat_job, new_plan_job, new_text_search_job, JobNode, TreeInputs,
    TreeRepoOptions,
};

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    original_query: String,
    #[serde(default)]
    search_type: String,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    pattern_type: String,
    #[serde(default)]
    on_sourcegraph_dot_com: bool,
    #[serde(default)]
    context_lines: i32,
    #[serde(default)]
    sanitize_patterns: Vec<String>,
    #[serde(default)]
    user_settings: UserSettingsSpec,
    #[serde(default)]
    features: FeaturesSpec,
}

#[derive(Debug, Default, Deserialize)]
struct UserSettingsSpec {
    #[serde(default)]
    search_include_forks: Option<bool>,
    #[serde(default)]
    search_include_archived: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct FeaturesSpec {
    #[serde(default)]
    content_based_lang_filters: bool,
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let map: BTreeMap<String, Value> = pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
    Value::Object(map.into_iter().collect())
}

fn search_type(value: &str) -> SearchType {
    match value {
        "" | "regexp" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "keyword" => SearchType::Keyword,
        "codycontext" => SearchType::CodyContext,
        other => panic!("unknown search type {other}"),
    }
}

fn pattern_type(case: &Case) -> SearchType {
    if case.pattern_type.is_empty() {
        search_type(&case.search_type)
    } else {
        search_type(&case.pattern_type)
    }
}

fn protocol(value: &str) -> Protocol {
    match value {
        "" | "streaming" => Protocol::Streaming,
        "batch" => Protocol::Batch,
        "exhaustive" => Protocol::Exhaustive,
        other => panic!("unknown protocol {other}"),
    }
}

fn plan(case: &Case) -> Vec<Basic> {
    pipeline_query(&case.query, search_type(&case.search_type))
        .unwrap_or_else(|error| panic!("{}: parse {:?}: {error}", case.name, case.query))
}

fn basic(case: &Case) -> Basic {
    let plan = plan(case);
    assert_eq!(
        plan.len(),
        1,
        "{}: expected one basic query, got {}",
        case.name,
        plan.len()
    );
    plan.into_iter().next().unwrap()
}

fn inputs(case: &Case) -> TreeInputs {
    TreeInputs {
        protocol: protocol(&case.protocol),
        pattern_type: pattern_type(case),
        user_settings: jb::UserSettings {
            search_include_forks: case.user_settings.search_include_forks,
            search_include_archived: case.user_settings.search_include_archived,
        },
        features: Features {
            content_based_lang_filters: case.features.content_based_lang_filters,
            ..Features::default()
        },
        context_lines: case.context_lines,
        sanitize_search_patterns: case.sanitize_patterns.clone(),
        on_sourcegraph_dot_com: case.on_sourcegraph_dot_com,
        original_query: case.original_query.clone(),
        sub_repo_enabled: false,
        cody_context_hook: None,
        cody_context: None,
    }
}

fn attributes(values: Vec<(String, String)>) -> Value {
    Value::Array(
        values
            .into_iter()
            .map(|(key, value)| Value::Array(vec![Value::String(key), Value::String(value)]))
            .collect(),
    )
}

fn tree(node: &JobNode) -> Value {
    let (name, attrs, children) = node.describe();
    obj(vec![
        ("name", Value::String(name.to_owned())),
        ("attributes", attributes(attrs)),
        (
            "children",
            Value::Array(children.into_iter().map(tree).collect()),
        ),
    ])
}

fn result(result: Result<JobNode, JobError>) -> Value {
    match result {
        Ok(node) => obj(vec![("ok", Value::Bool(true)), ("tree", tree(&node))]),
        Err(error) => obj(vec![
            ("ok", Value::Bool(false)),
            ("error", Value::String(error.0)),
        ]),
    }
}

fn run(case: &Case) -> Value {
    let parsed = plan(case);
    let input = inputs(case);
    let outcome = match case.kind.as_str() {
        "plan" => new_plan_job(&input, &parsed),
        "basic" => new_basic_job(&input, &basic(case)),
        "flat" => new_flat_job(&input, &basic(case)),
        "text" => {
            let b = basic(case);
            let result_types =
                jb::compute_result_types(&b, input.pattern_type, jb::DEFAULT_RESULT_TYPES);
            let repo_options =
                TreeRepoOptions::from_job_builders(&jb::to_repo_options(&b, &input.user_settings));
            new_text_search_job(&input, &b, result_types, repo_options)
        }
        other => panic!("unknown kind {other}"),
    };

    obj(vec![
        ("kind", Value::String(case.kind.clone())),
        ("name", Value::String(case.name.clone())),
        ("result", result(outcome)),
    ])
}

#[test]
fn search_jobutil_tree_differential() {
    if std::env::var("SG_M9HO_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9HO_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M9HO_CORPUS").expect("M9HO_CORPUS must be set");
    let out_path = std::env::var("M9HO_OUT").expect("M9HO_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
