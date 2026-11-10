//! M9.8hc differential harness: `internal/search/client/client.go`'s
//! `searchClient.Plan` (plus `detectSearchType`/`overrideSearchType`) against
//! [`sg_service_frontend::search_client`].
//!
//! Run with `SG_M9HC_DIFF=1 M9HC_CORPUS=<json> M9HC_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.
//!
//! `sanitizeSearchPatterns` is not ported in this slice, so the harness feeds
//! the corpus list straight through the injected dependency; with a valid
//! pattern list and a non-admin user that is exactly what the Go oracle's
//! `sanitizeSearchPatterns` returns.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::node::go_quote;
use sg_search_query::{to_json, SearchType};

use sg_service_frontend::search_client::{
    ExperimentalFeatures, FeatureFlagLoader, PlanDeps, Protocol, SanitizePatternsLoader,
    SearchClient, SearchContextResolver, SearchMode, Settings, SettingsLoader,
};

#[derive(Deserialize, Default)]
struct CorpusSettings {
    #[serde(default)]
    experimental_features: Option<CorpusExperimentalFeatures>,
    #[serde(default)]
    search_context_lines: Option<i32>,
}

#[derive(Deserialize, Default)]
struct CorpusExperimentalFeatures {
    #[serde(default)]
    search_contexts_query: Option<bool>,
}

#[derive(Deserialize, Default, Clone)]
struct CorpusContext {
    #[serde(default)]
    query: String,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct CorpusCase {
    name: String,
    version: String,
    #[serde(default)]
    pattern_type: Option<String>,
    query: String,
    search_mode: i32,
    protocol: i32,
    #[serde(default)]
    context_lines: Option<i32>,
    #[serde(default)]
    settings: CorpusSettings,
    #[serde(default)]
    settings_error: Option<String>,
    #[serde(default)]
    feature_flags: BTreeMap<String, bool>,
    #[serde(default)]
    on_sourcegraph_dot_com: bool,
    #[serde(default)]
    contexts: BTreeMap<String, CorpusContext>,
    #[serde(default)]
    sanitize_patterns: Vec<String>,
}

#[test]
fn search_client_plan_diff() {
    if std::env::var("SG_M9HC_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9HC_CORPUS").expect("M9HC_CORPUS must be set when SG_M9HC_DIFF=1");
    let out_path = std::env::var("M9HC_OUT").expect("M9HC_OUT must be set when SG_M9HC_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("parse corpus");

    let entries: Vec<Value> = cases.iter().map(run_case).collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn build_settings(settings: &CorpusSettings) -> Settings {
    Settings {
        experimental_features: settings.experimental_features.as_ref().map(|features| {
            ExperimentalFeatures {
                search_contexts_query: features.search_contexts_query,
            }
        }),
        search_context_lines: settings.search_context_lines,
    }
}

fn search_mode(value: i32) -> SearchMode {
    if value == 1 {
        SearchMode::SmartSearch
    } else {
        SearchMode::Precise
    }
}

fn protocol(value: i32) -> Protocol {
    match value {
        1 => Protocol::Batch,
        2 => Protocol::Exhaustive,
        _ => Protocol::Streaming,
    }
}

fn search_type_str(search_type: SearchType) -> &'static str {
    match search_type {
        SearchType::Regex => "regex",
        SearchType::Literal => "literal",
        SearchType::Structural => "structural",
        SearchType::Standard => "standard",
        SearchType::CodyContext => "codycontext",
        SearchType::Keyword => "keyword",
    }
}

fn run_case(case: &CorpusCase) -> Value {
    let settings: Result<Settings, String> = match &case.settings_error {
        Some(err) => Err(err.clone()),
        None => Ok(build_settings(&case.settings)),
    };
    let user_settings: SettingsLoader = Box::new(move || settings.clone());

    let flags = case.feature_flags.clone();
    let feature_flags: FeatureFlagLoader = Box::new(move || flags.clone());

    let contexts = case.contexts.clone();
    let resolve_search_context: SearchContextResolver = Arc::new(move |spec: &str| {
        // `ResolveSearchContextSpec` special-cases the global context (and the
        // empty spec) before touching the database.
        if spec.is_empty() || spec == "global" {
            return Ok(String::new());
        }
        match contexts.get(spec) {
            Some(context) => match &context.error {
                Some(err) => Err(err.clone()),
                None => Ok(context.query.clone()),
            },
            None => Err(format!("search context {} not found", go_quote(spec))),
        }
    });

    let sanitize_patterns = if case.sanitize_patterns.is_empty() {
        Vec::new()
    } else {
        case.sanitize_patterns.clone()
    };
    let sanitize: SanitizePatternsLoader = Box::new(move || sanitize_patterns.clone());

    let deps = PlanDeps {
        user_settings,
        feature_flags,
        resolve_search_context,
        sanitize_search_patterns: sanitize,
        on_sourcegraph_dot_com: case.on_sourcegraph_dot_com,
    };

    let result = SearchClient::new(deps).plan(
        &case.version,
        case.pattern_type.as_deref(),
        &case.query,
        search_mode(case.search_mode),
        protocol(case.protocol),
        case.context_lines,
    );

    let mut entry: BTreeMap<&str, Value> = BTreeMap::new();
    entry.insert("name", json!(case.name));
    match result {
        Err(err) => {
            entry.insert("ok", json!(false));
            entry.insert("error", json!(err.to_string()));
        }
        Ok(inputs) => {
            entry.insert("ok", json!(true));
            entry.insert("original_query", json!(inputs.original_query));
            entry.insert("search_mode", json!(inputs.search_mode.as_int()));
            entry.insert("pattern_type", json!(search_type_str(inputs.pattern_type)));
            entry.insert("protocol", json!(inputs.protocol.as_str()));
            entry.insert("context_lines", json!(inputs.context_lines));
            entry.insert(
                "on_sourcegraph_dot_com",
                json!(inputs.on_sourcegraph_dot_com),
            );
            entry.insert(
                "features",
                json!({
                    "content_based_lang_filters": inputs.features.content_based_lang_filters,
                    "debug": inputs.features.debug,
                }),
            );
            entry.insert(
                "sanitize_search_patterns",
                json!(inputs.sanitize_search_patterns),
            );

            let plan: Vec<Value> = inputs
                .plan
                .iter()
                .map(|basic| {
                    serde_json::from_str(&to_json(&basic.to_parse_tree()))
                        .expect("basic parse tree is valid JSON")
                })
                .collect();
            entry.insert("plan", Value::Array(plan));

            let query: Value =
                serde_json::from_str(&to_json(&inputs.query)).expect("query is valid JSON");
            entry.insert("query", query);
        }
    }

    Value::Object(
        entry
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}
