//! M9.8hs differential harness: the frontend search stream wiring
//! (`search_stream::BackendDeps`).
//!
//! Two halves, both gated by `SG_M9HS_DIFF=1`:
//!
//! * `plan` runs `BackendDeps::plan` over the corpus's `kind: "plan"` cases
//!   and writes the same normalized `SearchInputs` JSON projection as the Go
//!   oracle (`internal/search/client/zz_m9hs_plan_oracle_test.go`), including
//!   `MaxResults()`;
//! * `framing` drives `search_response` with `BackendDeps` whose `JobRunner`
//!   replays the corpus event list plus the tail alert/error, and writes the
//!   same `ss` lines as the Go oracle
//!   (`cmd/frontend/internal/search/zz_m9hs_framing_oracle_test.go`).
//!
//! Env: `SG_M9HS_DIFF=1`, `M9HS_CORPUS`, `M9HS_PLAN_OUT`, `M9HS_FRAMING_OUT`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, Request};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::{json, Value};
use time::OffsetDateTime;

use sg_search_query::parser::SearchType;
use sg_search_query::to_json;

use sg_service_frontend::search_client::{
    ExperimentalFeatures, FeatureFlagLoader, PlanDeps, SanitizePatternsLoader,
    SearchContextResolver, Settings, SettingsLoader,
};
use sg_service_frontend::search_stream::args::Args;
use sg_service_frontend::search_stream::convert::{
    FileMatch, FileRef, Match, MinimalRepo, RepoMetadata,
};
use sg_service_frontend::search_stream::events::{EventAlert, Location, Range};
use sg_service_frontend::search_stream::progress::{
    repo_status, RepoStatusMap, SearchEvent, Stats,
};
use sg_service_frontend::search_stream::{
    search_response, BackendDeps, ExecuteError, JobRunOutput, JobRunner, SearchDeps, SearchInputs,
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

#[derive(Deserialize, Default, Clone)]
struct CorpusAlert {
    #[serde(default)]
    title: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    kind: String,
}

#[derive(Deserialize, Default, Clone)]
struct CorpusError {
    #[serde(default)]
    message: String,
    #[serde(default)]
    unsupported: bool,
}

#[derive(Deserialize)]
struct CorpusCase {
    #[serde(default)]
    kind: String,
    name: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    pattern_type: Option<String>,
    #[serde(default)]
    query: String,
    #[serde(default)]
    search_mode: i32,
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
    #[serde(default)]
    target: String,
    #[serde(default)]
    events: Vec<String>,
    #[serde(default)]
    alert: Option<CorpusAlert>,
    #[serde(default)]
    error: Option<CorpusError>,
}

fn read_corpus() -> Vec<CorpusCase> {
    let corpus_path =
        std::env::var("M9HS_CORPUS").expect("M9HS_CORPUS must be set when SG_M9HS_DIFF=1");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    serde_json::from_str(&raw).expect("parse corpus")
}

fn enabled() -> bool {
    std::env::var("SG_M9HS_DIFF").as_deref() == Ok("1")
}

fn lazy_pool() -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/none")
        .expect("lazy pool")
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

fn build_plan_deps(case: &CorpusCase) -> PlanDeps {
    let settings: Result<Settings, String> = match &case.settings_error {
        Some(err) => Err(err.clone()),
        None => Ok(build_settings(&case.settings)),
    };
    let user_settings: SettingsLoader = Box::new(move || settings.clone());

    let flags = case.feature_flags.clone();
    let feature_flags: FeatureFlagLoader = Box::new(move || flags.clone());

    let contexts = case.contexts.clone();
    let resolve_search_context: SearchContextResolver = Arc::new(move |spec: &str, _: &sg_service_frontend::search_contexts::RequestContext| {
        if spec.is_empty() || spec == "global" {
            return Ok(String::new());
        }
        match contexts.get(spec) {
            Some(context) => match &context.error {
                Some(err) => Err(err.clone()),
                None => Ok(context.query.clone()),
            },
            None => Err(format!(
                "search context {} not found",
                sg_search_query::node::go_quote(spec)
            )),
        }
    });

    let sanitize_patterns = case.sanitize_patterns.clone();
    let sanitize: SanitizePatternsLoader = Box::new(move || sanitize_patterns.clone());

    PlanDeps {
        user_settings,
        feature_flags,
        resolve_search_context,
        sanitize_search_patterns: sanitize,
        on_sourcegraph_dot_com: case.on_sourcegraph_dot_com,
    }
}

fn args_for(case: &CorpusCase) -> Args {
    Args {
        query: case.query.clone(),
        version: case.version.clone(),
        pattern_type: case.pattern_type.clone().unwrap_or_default(),
        display: -1,
        max_line_len: -1,
        enable_chunk_matches: false,
        search_mode: i64::from(case.search_mode),
        context_lines: case.context_lines,
        zoekt_search_options_override: String::new(),
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

fn plan_entry(
    case: &CorpusCase,
    result: Result<SearchInputs, sg_service_frontend::search_stream::PlanError>,
) -> Value {
    use sg_service_frontend::search_stream::PlanError;
    let mut entry: BTreeMap<&str, Value> = BTreeMap::new();
    entry.insert("name", json!(case.name));
    match result {
        Err(err) => {
            entry.insert("ok", json!(false));
            let error = match err {
                PlanError::Query {
                    message,
                    expected_operand,
                } => json!({
                    "query": true,
                    "message": message,
                    "expected_operand": expected_operand,
                }),
                PlanError::Other(message) => json!({
                    "query": false,
                    "message": message,
                }),
            };
            entry.insert("error", error);
        }
        Ok(inputs) => {
            entry.insert("ok", json!(true));
            entry.insert("max_results", json!(inputs.max_results));
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
        }
    }
    Value::Object(
        entry
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

#[tokio::test]
async fn search_stream_hs_plan_diff() {
    if !enabled() {
        return;
    }
    let out_path =
        std::env::var("M9HS_PLAN_OUT").expect("M9HS_PLAN_OUT must be set when SG_M9HS_DIFF=1");
    let cases = read_corpus();

    let mut entries = Vec::new();
    for case in cases.iter().filter(|case| case.kind == "plan") {
        let backend = BackendDeps::with_deps(
            lazy_pool(),
            build_plan_deps(case),
            Arc::new(sg_service_frontend::search_stream::UnportedJobRunner),
        );
        let result = backend.plan(&args_for(case)).await;
        entries.push(plan_entry(case, result));
    }

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

// ---------------------------------------------------------------------------
// Framing half
// ---------------------------------------------------------------------------

fn ts(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
}

fn repo_meta() -> HashMap<i32, RepoMetadata> {
    let mut map = HashMap::new();
    map.insert(
        1,
        RepoMetadata {
            id: 1,
            name: "github.com/foo/bar".to_owned(),
            description: "A repo <desc> & stuff".to_owned(),
            fork: false,
            archived: false,
            private: true,
            stars: 42,
            last_fetched: Some(ts(1609556645)),
            key_value_pairs: None,
            topics: Some(vec!["t1".to_owned(), "t2".to_owned()]),
        },
    );
    map.insert(
        2,
        RepoMetadata {
            id: 2,
            name: "gitlab.com/a/b".to_owned(),
            description: String::new(),
            fork: false,
            archived: false,
            private: false,
            stars: 0,
            last_fetched: None,
            key_value_pairs: None,
            topics: None,
        },
    );
    map.insert(
        3,
        RepoMetadata {
            id: 3,
            name: "example.com/x/y".to_owned(),
            description: String::new(),
            fork: true,
            archived: false,
            private: false,
            stars: 7,
            last_fetched: None,
            key_value_pairs: None,
            topics: Some(vec!["only".to_owned()]),
        },
    );
    map.insert(
        4,
        RepoMetadata {
            id: 4,
            name: "archived.example.com/old/repo".to_owned(),
            description: String::new(),
            fork: false,
            archived: true,
            private: false,
            stars: 0,
            last_fetched: None,
            key_value_pairs: None,
            topics: None,
        },
    );
    map
}

fn repo(id: i32) -> MinimalRepo {
    let metadata = repo_meta();
    let metadata = &metadata[&id];
    MinimalRepo {
        id: metadata.id,
        name: metadata.name.clone(),
    }
}

fn range(offset: i64, line: i64, column: i64, length: i64) -> Range {
    Range {
        start: Location {
            offset,
            line,
            column,
        },
        end: Location {
            offset: offset + length,
            line,
            column: column + length,
        },
    }
}

fn path_file() -> FileMatch {
    FileMatch {
        file: FileRef {
            repo: repo(1),
            commit_id: "def456".to_owned(),
            path: "docs/README.md".to_owned(),
            input_rev: None,
            precise_language: String::new(),
        },
        chunk_matches: Vec::new(),
        symbols: Vec::new(),
        path_matches: vec![range(0, 1, 0, 5)],
        limit_hit: false,
        debug: None,
    }
}

fn skipped_stats() -> Stats {
    let mut status = RepoStatusMap::default();
    status.update(2, repo_status::MISSING);
    status.update(3, repo_status::CLONING);
    status.update(4, repo_status::TIMED_OUT);
    Stats {
        is_limit_hit: true,
        repos: HashSet::from([1, 2, 3, 4]),
        status,
        backends_missing: 2,
        excluded_forks: 3,
        excluded_archived: 1,
    }
}

fn event(name: &str) -> SearchEvent {
    match name {
        "path" => SearchEvent {
            results: vec![Match::File(path_file())],
            stats: Stats::default(),
        },
        "progress" => SearchEvent {
            results: Vec::new(),
            stats: Stats {
                repos: HashSet::from([1]),
                ..Stats::default()
            },
        },
        "skipped" => SearchEvent {
            results: Vec::new(),
            stats: skipped_stats(),
        },
        other => panic!("unknown event {other:?}"),
    }
}

struct ScriptedRunner {
    events: Vec<SearchEvent>,
    alert: Option<EventAlert>,
    error: Option<ExecuteError>,
}

impl JobRunner for ScriptedRunner {
    fn run(
        &self,
        _job: &mut sg_service_frontend::search_jobutil_tree::JobNode,
        sender: &mut dyn FnMut(SearchEvent),
    ) -> JobRunOutput {
        for event in &self.events {
            sender(event.clone());
        }
        JobRunOutput {
            alert: self.alert.clone(),
            error: self.error.clone(),
        }
    }
}

struct FramingDeps {
    backend: BackendDeps,
    metadata: HashMap<i32, RepoMetadata>,
}

#[async_trait]
impl SearchDeps for FramingDeps {
    async fn plan(
        &self,
        args: &Args,
    ) -> Result<SearchInputs, sg_service_frontend::search_stream::PlanError> {
        self.backend.plan(args).await
    }

    async fn execute(
        &self,
        inputs: &SearchInputs,
        query: &str,
    ) -> std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = sg_service_frontend::search_stream::ExecuteItem> + Send,
        >,
    > {
        self.backend.execute(inputs, query).await
    }

    async fn repo_metadata(&self, ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String> {
        Ok(ids
            .iter()
            .filter_map(|id| {
                self.metadata
                    .get(id)
                    .map(|metadata| (*id, metadata.clone()))
            })
            .collect())
    }

    async fn repo_names(&self, ids: &[i32]) -> Vec<(i32, String)> {
        ids.iter()
            .filter_map(|id| {
                self.metadata
                    .get(id)
                    .map(|metadata| (*id, metadata.name.clone()))
            })
            .collect()
    }

}

fn header_field(headers: &http::HeaderMap, name: &str) -> String {
    match headers.get(name) {
        None => "-".to_owned(),
        Some(value) => format!("{:?}", value.to_str().unwrap_or("")),
    }
}

#[tokio::test]
async fn search_stream_hs_framing_diff() {
    if !enabled() {
        return;
    }
    let out_path = std::env::var("M9HS_FRAMING_OUT")
        .expect("M9HS_FRAMING_OUT must be set when SG_M9HS_DIFF=1");
    let cases = read_corpus();

    let mut out = String::new();
    for case in cases.iter().filter(|case| case.kind == "framing") {
        let events: Vec<SearchEvent> = case.events.iter().map(|name| event(name)).collect();
        let runner = ScriptedRunner {
            events,
            alert: case.alert.as_ref().map(|alert| EventAlert {
                title: alert.title.clone(),
                description: alert.description.clone(),
                kind: alert.kind.clone(),
                proposed_queries: None,
            }),
            error: case.error.as_ref().map(|error| ExecuteError {
                message: error.message.clone(),
                unsupported: error.unsupported,
            }),
        };
        let backend = BackendDeps::with_deps(lazy_pool(), build_plan_deps(case), Arc::new(runner));
        let deps = FramingDeps {
            backend,
            metadata: repo_meta(),
        };

        let request = Request::builder()
            .method(Method::GET)
            .uri(&case.target)
            .body(())
            .expect("request");
        let response = search_response(&request, Arc::new(deps)).await;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = String::from_utf8_lossy(&body);

        out.push_str(&format!(
            "ss {} status={} ctype={} cache={} conn={} te={} xab={} vary={} ce={} body={body:?}\n",
            case.name,
            status,
            header_field(&headers, "content-type"),
            header_field(&headers, "cache-control"),
            header_field(&headers, "connection"),
            header_field(&headers, "transfer-encoding"),
            header_field(&headers, "x-accel-buffering"),
            header_field(&headers, "vary"),
            header_field(&headers, "content-encoding"),
        ));
    }

    std::fs::write(&out_path, out).expect("write output");
}
