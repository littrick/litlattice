//! M9.8gy differential harness: `internal/search/zoekt/zoekt_global.go`
//! (`DefaultGlobalQueryScope`, `GlobalZoektQuery.ApplyPrivateFilter` /
//! `Generate`) against [`sg_zoekt::global_query`].
//!
//! Run with `SG_M9GY_DIFF=1 M9GY_CORPUS=<json> M9GY_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit the same normalized JSON. Output goes through [`encode_like_go`] so
//! Go's `encoding/json` HTML escaping (`<`, `>`, `&`) compares byte for byte.

use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::global_query::{
    default_global_query_scope, GlobalZoektQuery, RepoOptions, RepoVisibility,
};
use sg_zoekt::query::Query;

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    scope_cases: Vec<ScopeCase>,
    #[serde(default)]
    global_cases: Vec<GlobalCase>,
}

#[derive(Deserialize)]
struct ScopeCase {
    name: String,
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    only_archived: bool,
    #[serde(default)]
    no_archived: bool,
    #[serde(default)]
    only_forks: bool,
    #[serde(default)]
    no_forks: bool,
    #[serde(default)]
    minus_repo_filters: Vec<String>,
}

#[derive(Deserialize)]
struct GlobalCase {
    name: String,
    #[serde(default)]
    base_query: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    include_private: bool,
    #[serde(default)]
    steps: Vec<Vec<RepoSpec>>,
}

#[derive(Deserialize)]
struct RepoSpec {
    id: u32,
    #[serde(default)]
    name: String,
}

fn visibility(value: &str) -> RepoVisibility {
    match value {
        "Public" => RepoVisibility::Public,
        "Any" => RepoVisibility::Any,
        "Private" => RepoVisibility::Private,
        other => RepoVisibility::Other(other.to_owned()),
    }
}

#[test]
fn global_query_diff() {
    if std::env::var("SG_M9GY_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GY_CORPUS").expect("M9GY_CORPUS must be set when SG_M9GY_DIFF=1");
    let out_path = std::env::var("M9GY_OUT").expect("M9GY_OUT must be set when SG_M9GY_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let scope_cases: Vec<Value> = corpus.scope_cases.iter().map(run_scope_case).collect();
    let global_cases: Vec<Value> = corpus.global_cases.iter().map(run_global_case).collect();

    let output = json!({
        "scope_cases": scope_cases,
        "global_cases": global_cases,
    });
    std::fs::write(&out_path, encode_like_go(&output)).expect("write output");
}

fn run_scope_case(case: &ScopeCase) -> Value {
    let options = RepoOptions {
        visibility: visibility(&case.visibility),
        minus_repo_filters: case.minus_repo_filters.clone(),
        only_archived: case.only_archived,
        no_archived: case.no_archived,
        only_forks: case.only_forks,
        no_forks: case.no_forks,
    };
    match default_global_query_scope(&options) {
        Ok(Some(query)) => json!({
            "name": case.name,
            "ok": true,
            "scope": query.to_string(),
        }),
        Ok(None) => json!({
            "name": case.name,
            "ok": true,
            "scope": Value::Null,
        }),
        Err(error) => json!({
            "name": case.name,
            "ok": false,
            "error": error.to_string(),
        }),
    }
}

fn run_global_case(case: &GlobalCase) -> Value {
    let query = Query::Substring {
        pattern: case.base_query.clone(),
        case_sensitive: false,
        file_name: false,
        content: true,
    };
    let scope = match case.scope.as_str() {
        "public" => default_global_query_scope(&RepoOptions {
            visibility: RepoVisibility::Public,
            ..RepoOptions::default()
        })
        .expect("public scope is always valid"),
        "none" => None,
        other => panic!("unknown scope {other:?}"),
    };

    let mut global = GlobalZoektQuery::new(query.clone(), scope, case.include_private);
    let generate_before = global.generate().to_string();

    let mut steps = Vec::with_capacity(case.steps.len());
    for repos in &case.steps {
        let repos: Vec<(u32, String)> = repos
            .iter()
            .map(|repo| (repo.id, repo.name.clone()))
            .collect();
        global.apply_private_filter(&repos);
        steps.push(json!({"generate": global.generate().to_string()}));
    }

    let repo_scope_after: Vec<String> = global
        .repo_scope
        .iter()
        .map(|scope| scope.to_string())
        .collect();

    json!({
        "name": case.name,
        "query": query.to_string(),
        "include_private": case.include_private,
        "generate_before": generate_before,
        "steps": steps,
        "repo_scope_after": repo_scope_after,
    })
}

/// `encoding/json` HTML-escapes `<`, `>`, `&` (and the JS line separators).
/// `serde_json` does not, so rewrite the serialized string. These bytes never
/// appear in JSON syntax outside string values, so a global replace is safe.
fn encode_like_go(value: &Value) -> String {
    serde_json::to_string(value)
        .expect("encode output")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}
