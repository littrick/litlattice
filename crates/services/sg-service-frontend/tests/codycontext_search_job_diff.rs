//! M9.8ju differential harness: codycontext `NewSearchJob` construction,
//! `sanitizeSearchPatterns` and the repo-pager tree wiring, compared with the
//! Go oracles
//! (`internal/search/codycontext/zz_m9ju_*_test.go`,
//! `internal/search/job/jobutil/zz_m9ju_plan_oracle_test.go`,
//! `internal/search/client/zz_m9ju_sanitize_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9JU_DIFF=1` enable the harness
//! * `M9JU_CORPUS` corpus JSON
//! * `M9JU_OUT` output JSON

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::Value;

use sg_search_query::basic::Basic;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_search_query::printer::string_human;
use sg_service_frontend::actor_middleware::Actor;
use sg_service_frontend::codycontext::{
    find_symbols, get_file_matcher, get_result_limits, needs_readme_context, new_search_job,
    parse_query, stem_term, transform_patterns, CodyContextJobParts, CodyContextOptions,
};
use sg_service_frontend::search_client::{
    sanitize_search_patterns, Features, OrgLookupError, Protocol, SanitizeDeps, SanitizeUser,
    SearchSanitization,
};
use sg_service_frontend::search_job::JobError;
use sg_service_frontend::search_jobutil_job_builders as jb;
use sg_service_frontend::search_jobutil_tree::{new_plan_job, JobNode, TreeInputs};

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    two_clauses: bool,
    #[serde(default)]
    patterns: Vec<String>,
    #[serde(default)]
    input: String,
    #[serde(default)]
    inputs: Vec<String>,
    #[serde(default)]
    cody: CodySpec,
    #[serde(default)]
    search_type: String,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    original_query: String,
    #[serde(default)]
    context_lines: i32,
    #[serde(default)]
    sanitize: SanitizeSpec,
}

#[derive(Debug, Default, Deserialize)]
struct CodySpec {
    #[serde(default)]
    features_null: bool,
    #[serde(default)]
    code_count: i32,
    #[serde(default)]
    text_count: i32,
    #[serde(default)]
    matcher: bool,
    #[serde(default)]
    deny_paths: Vec<String>,
    #[serde(default)]
    probes: Vec<Probe>,
}

#[derive(Debug, Default, Deserialize)]
struct Probe {
    #[serde(default)]
    repo: i32,
    #[serde(default)]
    path: String,
}

#[derive(Debug, Default, Deserialize)]
struct SanitizeSpec {
    #[serde(default)]
    feature_nil: bool,
    #[serde(default)]
    sanitization_nil: bool,
    #[serde(default)]
    org_name: String,
    #[serde(default)]
    sanitize_patterns: Vec<String>,
    #[serde(default)]
    actor: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    user_site_admin: bool,
    #[serde(default)]
    user_missing: bool,
    #[serde(default)]
    orgs: Vec<String>,
    #[serde(default)]
    orgs_error: bool,
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let map: BTreeMap<String, Value> = pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
    Value::Object(map.into_iter().collect())
}

fn strings(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

fn string_or_null(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn search_type(value: &str) -> SearchType {
    match value {
        "" | "standard" => SearchType::Standard,
        other => panic!("unknown search type {other}"),
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

fn cody_options(spec: &CodySpec) -> Option<CodyContextOptions> {
    if spec.features_null {
        return None;
    }
    let deny = spec.deny_paths.clone();
    let file_matcher = if spec.matcher {
        Some(
            Arc::new(move |_repo: i32, path: &str| !deny.iter().any(|denied| denied == path))
                as Arc<dyn Fn(i32, &str) -> bool + Send + Sync>,
        )
    } else {
        None
    };
    Some(CodyContextOptions {
        code_count: spec.code_count,
        text_count: spec.text_count,
        file_matcher,
    })
}

fn plan_basics(case: &Case) -> Vec<Basic> {
    if case.two_clauses {
        return vec![Basic::default(), Basic::default()];
    }
    pipeline_query(&case.query, SearchType::Standard)
        .unwrap_or_else(|error| panic!("{}: parse {:?}: {error}", case.name, case.query))
}

fn result_ok(fields: Vec<(&str, Value)>) -> Value {
    let mut pairs = vec![("ok", Value::Bool(true))];
    pairs.extend(fields);
    obj(pairs)
}

fn result_err(error: &JobError) -> Value {
    obj(vec![
        ("ok", Value::Bool(false)),
        ("error", Value::String(error.0.clone())),
    ])
}

fn run_parse(case: &Case) -> Value {
    match parse_query(&case.query) {
        Ok(parsed) => result_ok(vec![
            ("patterns", strings(&parsed.patterns)),
            ("symbols", strings(&parsed.symbols)),
            (
                "symbol_query",
                string_or_null(&string_human(&parsed.symbol_query.to_parse_tree())),
            ),
            (
                "keyword_query",
                string_or_null(&string_human(&parsed.keyword_query.to_parse_tree())),
            ),
        ]),
        Err(error) => result_err(&error),
    }
}

fn run_job(case: &Case) -> Value {
    let plan = plan_basics(case);
    let options = cody_options(&case.cody);
    let (code_count, text_count) = get_result_limits(options.as_ref());
    let file_matcher = get_file_matcher(options.as_ref());
    let matcher: Vec<Value> = case
        .cody
        .probes
        .iter()
        .map(|probe| Value::Bool(file_matcher.accepts(probe.repo, &probe.path)))
        .collect();

    match new_search_job(&plan, options.as_ref()) {
        Err(error) => result_err(&error),
        Ok(CodyContextJobParts::Noop) => result_ok(vec![
            ("noop", Value::Bool(true)),
            ("patterns", strings(&[])),
            ("code_count", Value::from(code_count)),
            ("text_count", Value::from(text_count)),
            ("symbol_query", Value::Null),
            ("code_query", Value::Null),
            ("text_query", Value::Null),
            ("matcher", Value::Array(matcher)),
        ]),
        Ok(CodyContextJobParts::Jobs {
            symbol_query,
            code_query,
            text_query,
            patterns,
            code_count,
            text_count,
            ..
        }) => result_ok(vec![
            ("noop", Value::Bool(false)),
            ("patterns", strings(&patterns)),
            ("code_count", Value::from(code_count)),
            ("text_count", Value::from(text_count)),
            (
                "symbol_query",
                match &symbol_query {
                    Some(query) => string_or_null(&string_human(&query.to_parse_tree())),
                    None => Value::Null,
                },
            ),
            (
                "code_query",
                string_or_null(&string_human(&code_query.to_parse_tree())),
            ),
            (
                "text_query",
                string_or_null(&string_human(&text_query.to_parse_tree())),
            ),
            ("matcher", Value::Array(matcher)),
        ]),
    }
}

fn run_plan(case: &Case) -> Value {
    let plan = pipeline_query(&case.query, search_type(&case.search_type))
        .unwrap_or_else(|error| panic!("{}: parse {:?}: {error}", case.name, case.query));
    let inputs = TreeInputs {
        protocol: protocol(&case.protocol),
        pattern_type: SearchType::CodyContext,
        user_settings: jb::UserSettings::default(),
        features: Features::default(),
        context_lines: case.context_lines,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: false,
        original_query: case.original_query.clone(),
        sub_repo_enabled: false,
        cody_context_hook: None,
        cody_context: cody_options(&case.cody),
    };
    match new_plan_job(&inputs, &plan) {
        Ok(node) => obj(vec![("ok", Value::Bool(true)), ("tree", tree(&node))]),
        Err(error) => result_err(&error),
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

struct TestSanitizeDeps {
    spec: SanitizeSpec,
    warns: Mutex<Vec<String>>,
}

impl SanitizeDeps for TestSanitizeDeps {
    fn search_sanitization(&self) -> Option<SearchSanitization> {
        if self.spec.feature_nil || self.spec.sanitization_nil {
            return None;
        }
        Some(SearchSanitization {
            org_name: self.spec.org_name.clone(),
            sanitize_patterns: self.spec.sanitize_patterns.clone(),
        })
    }

    fn actor(&self) -> Actor {
        match self.spec.actor.as_str() {
            "" | "user" => Actor::from_user(self.spec.uid),
            "internal" => Actor::internal(),
            "anonymous" => Actor::from_anonymous_user("m9ju"),
            other => panic!("unknown actor {other}"),
        }
    }

    fn user(&self, id: i32) -> Option<SanitizeUser> {
        if self.spec.user_missing {
            return None;
        }
        Some(SanitizeUser {
            id,
            site_admin: self.spec.user_site_admin,
        })
    }

    fn user_org_names(&self, _user_id: i32) -> Result<Vec<String>, OrgLookupError> {
        if self.spec.orgs_error {
            return Err(OrgLookupError);
        }
        Ok(self.spec.orgs.clone())
    }

    fn warn(&self, message: &str) {
        self.warns
            .lock()
            .expect("warns mutex")
            .push(message.to_owned());
    }
}

fn run_sanitize(case: &Case) -> Value {
    let deps = TestSanitizeDeps {
        spec: SanitizeSpec {
            feature_nil: case.sanitize.feature_nil,
            sanitization_nil: case.sanitize.sanitization_nil,
            org_name: case.sanitize.org_name.clone(),
            sanitize_patterns: case.sanitize.sanitize_patterns.clone(),
            actor: case.sanitize.actor.clone(),
            uid: case.sanitize.uid,
            user_site_admin: case.sanitize.user_site_admin,
            user_missing: case.sanitize.user_missing,
            orgs: case.sanitize.orgs.clone(),
            orgs_error: case.sanitize.orgs_error,
        },
        warns: Mutex::new(Vec::new()),
    };
    let patterns = sanitize_search_patterns(&deps);
    result_ok(vec![("patterns", strings(&patterns))])
}

fn run(case: &Case) -> Value {
    let result = match case.kind.as_str() {
        "parse" => run_parse(case),
        "job" => run_job(case),
        "plan" => run_plan(case),
        "transform" => result_ok(vec![(
            "patterns",
            strings(&transform_patterns(&case.patterns)),
        )]),
        "symbols" => result_ok(vec![("symbols", strings(&find_symbols(&case.patterns)))]),
        "readme" => result_ok(vec![(
            "needs",
            Value::Bool(needs_readme_context(&case.input)),
        )]),
        "stem" => {
            let stems: Vec<String> = case.inputs.iter().map(|input| stem_term(input)).collect();
            result_ok(vec![("stems", strings(&stems))])
        }
        "sanitize" => run_sanitize(case),
        other => panic!("unknown kind {other}"),
    };
    obj(vec![
        ("kind", Value::String(case.kind.clone())),
        ("name", Value::String(case.name.clone())),
        ("result", result),
    ])
}

#[test]
fn codycontext_search_job_differential() {
    if std::env::var("SG_M9JU_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9JU_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M9JU_CORPUS").expect("M9JU_CORPUS must be set");
    let out_path = std::env::var("M9JU_OUT").expect("M9JU_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
