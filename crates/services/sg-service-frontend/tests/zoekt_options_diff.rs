//! M9.42c differential harness: the zoekt search options override.
//!
//! * `options`: `search.ZoektParameters.ToSearchOptions`
//!   (`internal/search/types.go:170-235`) including the deferred
//!   `Features.ZoektSearchOptionsOverride` JSON override, against
//!   [`sg_service_frontend::search_stream::zoekt_search::ZoektParameters`].
//! * `auth`: the HTTP `is_authenticated` gate
//!   (`cmd/frontend/internal/search/search.go:144`) end to end through
//!   [`search_response`], recording the override `execute` receives.
//!
//! Env: `SG_M942C_DIFF=1`, `M942C_CORPUS`, `M942C_OPTIONS_OUT`,
//! `M942C_AUTH_OUT`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use http::{Method, Request};
use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::parser::SearchType;
use sg_service_frontend::actor_chain::ChainActor;
use sg_service_frontend::actor_middleware::Actor;
use sg_service_frontend::search_client::{
    Features as ClientFeatures, Protocol, SearchMode,
};
use sg_service_frontend::search_stream::args::Args;
use sg_service_frontend::search_stream::convert::RepoMetadata;
use sg_service_frontend::search_stream::zoekt_search::{
    Features as ZoektFeatures, SelectPath, ZoektParameters,
};
use sg_service_frontend::search_stream::{
    search_response, ExecuteItem, PlanError, SearchDeps, SearchInputs,
};
use sg_zoekt::query::{IndexedRequestType, Query};
use sg_zoekt::types::DEFAULT_DOCUMENT_RANKS_WEIGHT;

#[derive(Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    typ: String,
    #[serde(default)]
    select: Vec<String>,
    #[serde(default)]
    file_match_limit: i32,
    #[serde(default)]
    num_context_lines: i32,
    #[serde(default)]
    features_debug: bool,
    #[serde(default)]
    pattern_type: String,
    #[serde(default, rename = "override")]
    override_json: String,
    #[serde(default)]
    use_document_ranks: bool,
    #[serde(default)]
    document_ranks_weight: Option<f64>,
    #[serde(default)]
    flush_wall_time_ms: i64,
    #[serde(default)]
    actor: String,
    #[serde(default)]
    target: String,
}

fn enabled() -> bool {
    std::env::var("SG_M942C_DIFF").as_deref() == Ok("1")
}

fn read_corpus() -> Vec<Case> {
    let corpus_path =
        std::env::var("M942C_CORPUS").expect("M942C_CORPUS must be set when SG_M942C_DIFF=1");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    serde_json::from_str(&raw).expect("parse corpus")
}

fn request_type(value: &str) -> IndexedRequestType {
    match value {
        "symbol" => IndexedRequestType::Symbol,
        _ => IndexedRequestType::Text,
    }
}

fn pattern_type(value: &str) -> SearchType {
    match value {
        "keyword" => SearchType::Keyword,
        "codycontext" => SearchType::CodyContext,
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        _ => SearchType::Standard,
    }
}

fn params(case: &Case) -> ZoektParameters {
    ZoektParameters {
        query: Query::Const(true),
        typ: request_type(&case.typ),
        file_match_limit: case.file_match_limit,
        select: SelectPath(case.select.clone()),
        num_context_lines: case.num_context_lines,
        features: ZoektFeatures {
            debug: case.features_debug,
            zoekt_search_options_override: case.override_json.clone(),
        },
        pattern_type: pattern_type(&case.pattern_type),
        trace: false,
        flush_wall_time_override: (case.flush_wall_time_ms > 0)
            .then(|| Duration::from_millis(case.flush_wall_time_ms as u64)),
        use_document_ranks: case.use_document_ranks,
        document_ranks_weight: case
            .document_ranks_weight
            .unwrap_or(DEFAULT_DOCUMENT_RANKS_WEIGHT),
    }
}

fn options_entry(case: &Case) -> Value {
    let options = params(case).to_search_options();
    json!({
        "name": case.name,
        "options": {
            "EstimateDocCount": options.estimate_doc_count,
            "Whole": options.whole,
            "ShardMaxMatchCount": options.shard_max_match_count,
            "TotalMaxMatchCount": options.total_max_match_count,
            "ShardRepoMaxMatchCount": options.shard_repo_max_match_count,
            "MaxWallTime": options.max_wall_time.as_nanos() as i64,
            "FlushWallTime": options.flush_wall_time.as_nanos() as i64,
            "MaxDocDisplayCount": options.max_doc_display_count,
            "MaxMatchDisplayCount": options.max_match_display_count,
            "NumContextLines": options.num_context_lines,
            "ChunkMatches": options.chunk_matches,
            "UseDocumentRanks": options.use_document_ranks,
            // Rendered like Go's `strconv.FormatFloat(v, 'g', -1, 64)` so
            // both sides compare byte-for-byte.
            "DocumentRanksWeight": format!("{}", options.document_ranks_weight),
            "UseBM25Scoring": options.use_bm25_scoring,
            "Trace": options.trace,
            "DebugScore": options.debug_score,
        }
    })
}

#[test]
fn zoekt_options_diff() {
    if !enabled() {
        return;
    }
    let out_path = std::env::var("M942C_OPTIONS_OUT")
        .expect("M942C_OPTIONS_OUT must be set when SG_M942C_DIFF=1");
    let entries: Vec<Value> = read_corpus()
        .iter()
        .filter(|case| case.kind == "options")
        .map(options_entry)
        .collect();
    std::fs::write(&out_path, serde_json::to_string(&entries).expect("encode"))
        .expect("write options output");
}

// ---------------------------------------------------------------------------
// HTTP actor gate
// ---------------------------------------------------------------------------

struct AuthDeps {
    captured: Arc<Mutex<Option<String>>>,
}

fn auth_inputs() -> SearchInputs {
    SearchInputs {
        max_results: 10_000,
        original_query: "foo".to_owned(),
        plan: Vec::new(),
        search_mode: SearchMode::SmartSearch,
        pattern_type: SearchType::Standard,
        features: ClientFeatures::default(),
        protocol: Protocol::Streaming,
        context_lines: 1,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: false,
        cody_context: None,
    }
}

#[async_trait]
impl SearchDeps for AuthDeps {
    async fn plan(&self, _args: &Args) -> Result<SearchInputs, PlanError> {
        Ok(auth_inputs())
    }

    async fn execute(
        &self,
        inputs: &SearchInputs,
        _query: &str,
    ) -> Pin<Box<dyn tokio_stream::Stream<Item = ExecuteItem> + Send>> {
        *self.captured.lock().expect("lock") =
            Some(inputs.features.zoekt_search_options_override.clone());
        Box::pin(tokio_stream::empty())
    }

    async fn repo_metadata(&self, _ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String> {
        Ok(HashMap::new())
    }

    async fn repo_names(&self, _ids: &[i32]) -> Vec<(i32, String)> {
        Vec::new()
    }
}

fn auth_actor(kind: &str) -> Option<Actor> {
    match kind {
        "user" => Some(Actor::from_user(7)),
        "anonymous" => Some(Actor::from_anonymous_user("anon-1")),
        "internal" => Some(Actor::internal()),
        _ => None,
    }
}

#[tokio::test]
async fn zoekt_options_auth_diff() {
    if !enabled() {
        return;
    }
    let out_path = std::env::var("M942C_AUTH_OUT")
        .expect("M942C_AUTH_OUT must be set when SG_M942C_DIFF=1");
    let cases = read_corpus();

    let mut out = String::new();
    for case in cases.iter().filter(|case| case.kind == "auth") {
        let captured = Arc::new(Mutex::new(None));
        let deps = AuthDeps {
            captured: captured.clone(),
        };
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(&case.target)
            .body(())
            .expect("request");
        if let Some(actor) = auth_actor(&case.actor) {
            request.extensions_mut().insert(ChainActor(actor));
        }
        let _ = search_response(&request, Arc::new(deps)).await;
        let override_value = captured.lock().expect("lock").clone().unwrap_or_default();
        out.push_str(&format!(
            "auth {} override={}\n",
            case.name,
            sg_search_query::node::go_quote(&override_value)
        ));
    }

    std::fs::write(&out_path, out).expect("write auth output");
}
