//! M9.8go differential harness: `internal/search/zoekt.QueryToZoektQuery`
//! against [`sg_zoekt::query::from_basic`].
//!
//! Run with `SG_M9GO_DIFF=1 M9GO_CORPUS=<json> M9GO_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON tree; non-literal regexps are compared through a fixed-probe match
//! signature because the two regexp parsers print different canonical forms.

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::basic::Basic;
use sg_search_query::node::visit_field;
use sg_search_query::parse_regexp;
use sg_search_query::transformer::to_basic_query;
use sg_zoekt::query::{from_basic, Features, IndexedRequestType, Query, ResultTypes, TypeKind};

const REGEXP_PROBES: &[&str] = &[
    "",
    "a",
    "A",
    "aa",
    "aaa",
    "ab",
    "foo",
    "FOO",
    "foobar",
    "FOOBAR",
    "foo123",
    "fooX123",
    "x.go",
    "X.GO",
    "a.yaml",
    "vendor/a.go",
    "src/vendor/x.go",
    "abc.go",
    "abc.yaml",
    ".",
    "abc123",
    "_",
    "a+b",
    "file[1]",
];

#[derive(Deserialize)]
struct CorpusCase {
    name: String,
    query: String,
    features: bool,
    #[serde(rename = "type")]
    kind: String,
}

#[test]
fn query_conversion_diff() {
    if std::env::var("SG_M9GO_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GO_CORPUS").expect("M9GO_CORPUS must be set when SG_M9GO_DIFF=1");
    let out_path = std::env::var("M9GO_OUT").expect("M9GO_OUT must be set when SG_M9GO_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("parse corpus");

    let mut entries = Vec::with_capacity(cases.len());
    for case in &cases {
        let mut entry = serde_json::Map::new();
        entry.insert("name".to_owned(), json!(case.name));
        entry.insert("query".to_owned(), json!(case.query));
        entry.insert("features".to_owned(), json!(case.features));
        entry.insert("type".to_owned(), json!(case.kind));
        match convert(case) {
            Ok(query) => {
                entry.insert("ok".to_owned(), json!(true));
                entry.insert("query_normalized".to_owned(), normalize(&query));
            }
            Err(category) => {
                entry.insert("ok".to_owned(), json!(false));
                entry.insert("error".to_owned(), json!(category));
            }
        }
        entries.push(Value::Object(entry));
    }

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn convert(case: &CorpusCase) -> Result<Query, &'static str> {
    let nodes = parse_regexp(&case.query).map_err(|_| "query_parse")?;
    let basic = to_basic_query(&nodes).map_err(|_| "query_parse")?;
    let features = Features {
        content_based_lang_filters: case.features,
    };
    let typ = match case.kind.as_str() {
        "symbol" => IndexedRequestType::Symbol,
        _ => IndexedRequestType::Text,
    };
    let result_types = result_types(&basic);
    from_basic(&basic, result_types, &features, typ).map_err(|_| "conversion_error")
}

/// `computeResultTypes` from the baseline test, for the regexp search type.
fn result_types(basic: &Basic) -> ResultTypes {
    let tree = basic.to_parse_tree();
    let mut types = Vec::new();
    visit_field(&tree, "type", &mut |value, negated, _| {
        if !negated {
            types.push(value.to_owned());
        }
    });
    if types.is_empty() {
        return ResultTypes::file_path_repo();
    }
    let mut result = ResultTypes::default();
    for value in types {
        result = result.with(match value.as_str() {
            "repo" => ResultTypes::REPO,
            "symbol" => ResultTypes::SYMBOL,
            "file" => ResultTypes::FILE,
            "path" => ResultTypes::PATH,
            "diff" => ResultTypes::DIFF,
            "commit" => ResultTypes::COMMIT,
            "structural" => ResultTypes::STRUCTURAL,
            _ => 0,
        });
    }
    result
}

fn normalize(query: &Query) -> Value {
    match query {
        Query::And(children) => json!({
            "kind": "and",
            "children": children.iter().map(normalize).collect::<Vec<_>>(),
        }),
        Query::Or(children) => json!({
            "kind": "or",
            "children": children.iter().map(normalize).collect::<Vec<_>>(),
        }),
        Query::Not(child) => json!({"kind": "not", "child": normalize(child)}),
        Query::Substring {
            pattern,
            case_sensitive,
            file_name,
            content,
        } => json!({
            "kind": "substring",
            "pattern": pattern,
            "case_sensitive": case_sensitive,
            "file_name": file_name,
            "content": content,
        }),
        Query::Regexp {
            regexp,
            file_name,
            content,
            case_sensitive,
        } => {
            let matches = match regex::Regex::new(regexp) {
                Ok(re) => REGEXP_PROBES
                    .iter()
                    .map(|probe| re.is_match(probe))
                    .collect::<Vec<_>>(),
                Err(_) => Vec::new(),
            };
            json!({
                "kind": "regexp",
                "case_sensitive": case_sensitive,
                "file_name": file_name,
                "content": content,
                "matches": matches,
            })
        }
        Query::Symbol(child) => json!({"kind": "symbol", "child": normalize(child)}),
        Query::Language(language) => json!({"kind": "language", "language": language}),
        Query::Type { child, kind } => json!({
            "kind": "type",
            "type": match kind {
                TypeKind::FileMatch => "file_match",
                TypeKind::FileName => "file_name",
                TypeKind::Repo => "repo",
            },
            "child": normalize(child),
        }),
        Query::Const(value) => json!({"kind": "const", "value": value}),
        Query::Boost { child, .. } => json!({"kind": "boost", "child": normalize(child)}),
        _ => json!({"kind": "other"}),
    }
}
