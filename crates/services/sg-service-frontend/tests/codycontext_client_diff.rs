//! M9.9c differential harness: production `CodyContextClient` pieces
//! (`addLimitsAndFilter`, `getEmbeddingsContext`, `getKeywordContext`, the
//! dotcom/enterprise content filters and the full `GetCodyContext`
//! orchestration) against the Go oracle.
//!
//! Go oracle: `cmd/frontend/internal/codycontext/zz_m99c_oracle_test.go`.
//!
//! Env:
//!
//! * `SG_M99C_DIFF=1` enable the harness
//! * `M99C_CORPUS` corpus JSON
//! * `M99C_OUT` output JSON

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::codycontext::embeddings::{
    EmbeddingsHttpRequest, EmbeddingsHttpResponse, EmbeddingsHttpTransport,
};
use sg_service_frontend::codycontext::{
    add_limits_and_filter, CodyContextClient, CodyContextOptions, CodyFileMatcher, CodyGitserver,
    ContextList, DotcomContentFilter, EnterpriseContentFilter, GetContextArgs,
    HttpEmbeddingsClient, KeywordContextSearcher, KeywordPlanRequest, KeywordSearchOutcome,
    RepoEmbeddingStore, RepoIdName,
};
use sg_service_frontend::search_client::{Protocol, SearchMode};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, FileMatch, FileRef, MinimalRepo, Symbol, SymbolMatch,
};
use sg_service_frontend::search_stream::events::Location;
use sg_service_frontend::search_stream::SearchInputs;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    features_present: bool,
    #[serde(default)]
    code_count: i32,
    #[serde(default)]
    text_count: i32,
    #[serde(default)]
    deny: Vec<Probe>,
    #[serde(default)]
    probes: Vec<Probe>,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    query: String,
    #[serde(default)]
    file_patterns: Vec<String>,
    #[serde(default)]
    response: ResponseSpec,
    #[serde(default)]
    embedded: Vec<bool>,
    #[serde(default)]
    events: Vec<EventSpec>,
    #[serde(default)]
    plan_error: String,
    #[serde(default)]
    execute_error: String,
    #[serde(default)]
    alert: Option<AlertSpec>,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    embeddings_enabled: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    branch: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    lookup_error: bool,
    #[serde(default)]
    ignore_file: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct Probe {
    #[serde(default)]
    repo: i32,
    #[serde(default)]
    path: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ResponseSpec {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct EventSpec {
    #[serde(default)]
    results: Vec<MatchSpec>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MatchSpec {
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    repo_name: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    symbol_line: Option<i32>,
    #[serde(default)]
    chunk_line: Option<i32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AlertSpec {
    #[serde(default)]
    title: String,
    #[serde(default)]
    description: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn repo_id_names(specs: &[RepoSpec]) -> Vec<RepoIdName> {
    specs
        .iter()
        .map(|spec| RepoIdName {
            id: spec.id,
            name: spec.name.clone(),
        })
        .collect()
}

fn matcher_from_deny(deny: &[Probe]) -> CodyFileMatcher {
    let denied: HashSet<String> = deny
        .iter()
        .map(|probe| format!("{}:{}", probe.repo, probe.path))
        .collect();
    CodyFileMatcher::Custom(Arc::new(move |repo_id, path| {
        !denied.contains(&format!("{repo_id}:{path}"))
    }))
}

fn matcher_probes(matcher: &CodyFileMatcher, probes: &[Probe]) -> Vec<bool> {
    probes
        .iter()
        .map(|probe| matcher.accepts(probe.repo, &probe.path))
        .collect()
}

fn chunks_json(lists: &[ContextList]) -> Value {
    Value::Array(
        lists
            .iter()
            .map(|list| {
                json!({
                    "name": list.name,
                    "file_chunks": list.file_chunks.iter().map(|chunk| json!({
                        "repo_name": chunk.repo_name,
                        "repo_id": chunk.repo_id,
                        "commit_id": chunk.commit_id,
                        "path": chunk.path,
                        "start_line": chunk.start_line,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn request_path(url: &str) -> String {
    let rest = url.splitn(2, "://").nth(1).unwrap_or(url);
    match rest.find('/') {
        Some(index) => rest[index..].to_owned(),
        None => "/".to_owned(),
    }
}

fn sentinel_inputs() -> SearchInputs {
    SearchInputs {
        max_results: 0,
        original_query: String::new(),
        plan: Vec::new(),
        search_mode: SearchMode::Precise,
        pattern_type: sg_search_query::parser::SearchType::Standard,
        features: Default::default(),
        protocol: Protocol::Streaming,
        context_lines: 0,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: false,
        cody_context: None,
    }
}

fn protocol_int(protocol: Protocol) -> i32 {
    match protocol {
        Protocol::Streaming => 0,
        Protocol::Batch => 1,
        Protocol::Exhaustive => 2,
    }
}

fn build_file_match(spec: &MatchSpec) -> FileMatch {
    let file = FileRef {
        repo: MinimalRepo {
            id: spec.repo_id,
            name: spec.repo_name.clone(),
        },
        commit_id: spec.commit.clone(),
        path: spec.path.clone(),
        input_rev: None,
        precise_language: String::new(),
    };
    let symbols = spec
        .symbol_line
        .map(|line| {
            vec![SymbolMatch {
                symbol: Symbol {
                    name: String::new(),
                    path: spec.path.clone(),
                    line,
                    character: 0,
                    kind: String::new(),
                    language: String::new(),
                    parent: String::new(),
                    parent_kind: String::new(),
                },
                file: file.clone(),
            }]
        })
        .unwrap_or_default();
    let chunk_matches = spec
        .chunk_line
        .map(|line| {
            vec![ChunkMatch {
                content: "line\n".to_owned(),
                content_start: Location {
                    offset: 0,
                    line: i64::from(line),
                    column: 0,
                },
                ranges: Vec::new(),
            }]
        })
        .unwrap_or_default();
    FileMatch {
        file,
        chunk_matches,
        symbols,
        path_matches: Vec::new(),
        limit_hit: false,
        debug: None,
    }
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeGitserver {
    specs: BTreeMap<String, RepoSpec>,
    default_branch_calls: Mutex<usize>,
    file_reader_calls: Mutex<usize>,
}

#[async_trait]
impl CodyGitserver for FakeGitserver {
    async fn default_branch(&self, repo: &str) -> Result<(String, String), String> {
        *self.default_branch_calls.lock().expect("lock") += 1;
        let spec = self
            .specs
            .get(repo)
            .ok_or_else(|| format!("unexpected repo {repo:?}"))?;
        if spec.lookup_error {
            return Err("lookup failed".to_owned());
        }
        Ok((spec.branch.clone(), spec.commit.clone()))
    }

    async fn read_cody_ignore(&self, repo: &str, _commit: &str) -> Result<Option<String>, String> {
        *self.file_reader_calls.lock().expect("lock") += 1;
        let spec = self
            .specs
            .get(repo)
            .ok_or_else(|| format!("unexpected repo {repo:?}"))?;
        Ok(spec.ignore_file.clone())
    }
}

struct FakeEmbeddingsTransport {
    status: u16,
    body: String,
    requests: Mutex<Vec<Value>>,
}

#[async_trait]
impl EmbeddingsHttpTransport for FakeEmbeddingsTransport {
    async fn execute(
        &self,
        request: &EmbeddingsHttpRequest,
    ) -> Result<EmbeddingsHttpResponse, String> {
        let body: Value = serde_json::from_str(&request.body).unwrap_or(Value::Null);
        self.requests.lock().expect("lock").push(json!({
            "method": request.method,
            "path": request_path(&request.url),
            "content_type": request.content_type,
            "body": body,
        }));
        Ok(EmbeddingsHttpResponse {
            status: self.status,
            body: self.body.clone(),
        })
    }
}

#[derive(Default)]
struct FakeRepoEmbeddings {
    embedded: BTreeMap<i32, bool>,
}

#[async_trait]
impl RepoEmbeddingStore for FakeRepoEmbeddings {
    async fn repo_embedding_exists(&self, repo_id: i32) -> Result<bool, String> {
        Ok(self.embedded.get(&repo_id).copied().unwrap_or(false))
    }
}

#[derive(Debug, Clone)]
struct ExecutionRecord {
    code_count: i32,
    text_count: i32,
    matcher_present: bool,
}

struct FakeKeywordSearcher {
    plan_error: Option<String>,
    execute_error: Option<String>,
    events: Vec<EventSpec>,
    alert: Option<(String, String)>,
    plans: Mutex<Vec<KeywordPlanRequest>>,
    executions: Mutex<Vec<ExecutionRecord>>,
}

#[async_trait]
impl KeywordContextSearcher for FakeKeywordSearcher {
    async fn plan(&self, request: &KeywordPlanRequest) -> Result<SearchInputs, String> {
        self.plans.lock().expect("lock").push(request.clone());
        if let Some(error) = &self.plan_error {
            return Err(error.clone());
        }
        Ok(sentinel_inputs())
    }

    async fn execute(&self, inputs: &SearchInputs) -> Result<KeywordSearchOutcome, String> {
        let options = inputs.cody_context.as_ref();
        self.executions.lock().expect("lock").push(ExecutionRecord {
            code_count: options.map(|options| options.code_count).unwrap_or(0),
            text_count: options.map(|options| options.text_count).unwrap_or(0),
            matcher_present: options.is_some_and(|options| options.file_matcher.is_some()),
        });
        if let Some(error) = &self.execute_error {
            return Err(error.clone());
        }
        let file_matches = self
            .events
            .iter()
            .flat_map(|event| event.results.iter().map(build_file_match))
            .collect();
        Ok(KeywordSearchOutcome {
            file_matches,
            alert: self.alert.clone(),
        })
    }
}

fn dummy_client(
    embeddings: Option<Arc<dyn sg_service_frontend::codycontext::EmbeddingsSearch>>,
    repo_embeddings: Arc<dyn RepoEmbeddingStore>,
    keyword: Arc<dyn KeywordContextSearcher>,
    embeddings_enabled: bool,
    cody_disabled_reason: Option<String>,
) -> CodyContextClient {
    CodyContextClient::new(
        false,
        false,
        cody_disabled_reason,
        Arc::new(FakeGitserver::default()),
        EnterpriseContentFilter::parse(&[], &[]),
        embeddings_enabled,
        embeddings,
        repo_embeddings,
        keyword,
    )
}

fn noop_keyword() -> Arc<dyn KeywordContextSearcher> {
    Arc::new(FakeKeywordSearcher {
        plan_error: None,
        execute_error: None,
        events: Vec::new(),
        alert: None,
        plans: Mutex::new(Vec::new()),
        executions: Mutex::new(Vec::new()),
    })
}

// ---------------------------------------------------------------------------
// Case runners
// ---------------------------------------------------------------------------

fn run_limits(case: &Case) -> Value {
    let mut inputs = sentinel_inputs();
    inputs.cody_context = if case.features_present {
        Some(CodyContextOptions::default())
    } else {
        None
    };
    let matcher = matcher_from_deny(&case.deny);
    add_limits_and_filter(&mut inputs, case.code_count, case.text_count, &matcher);
    let options = inputs.cody_context.as_ref();
    json!({
        "features_created": inputs.cody_context.is_some(),
        "code_count": options.map(|options| options.code_count).unwrap_or(0),
        "text_count": options.map(|options| options.text_count).unwrap_or(0),
        "matcher_present": options.and_then(|options| options.file_matcher.as_ref()).is_some(),
        "probes": options
            .and_then(|options| options.file_matcher.as_ref())
            .map(|matcher| case.probes.iter().map(|probe| matcher(probe.repo, &probe.path)).collect::<Vec<_>>())
            .unwrap_or_default(),
    })
}

async fn run_embeddings(case: &Case) -> Value {
    let transport = Arc::new(FakeEmbeddingsTransport {
        status: case.response.status,
        body: serde_json::to_string(&case.response.body).unwrap_or_default(),
        requests: Mutex::new(Vec::new()),
    });
    let client = HttpEmbeddingsClient::new("http://fake.test", transport.clone());
    let context_client = dummy_client(
        Some(Arc::new(client)),
        Arc::new(FakeRepoEmbeddings::default()),
        noop_keyword(),
        true,
        None,
    );
    let matcher = matcher_from_deny(&case.deny);
    let args = GetContextArgs {
        repos: repo_id_names(&case.repos),
        repo_stats_present: false,
        file_patterns: Vec::new(),
        query: case.query.clone(),
        code_results_count: case.code_count,
        text_results_count: case.text_count,
    };
    let result = context_client.get_embeddings_context(&args, &matcher).await;
    let (lists, error) = match &result {
        Ok(lists) => (chunks_json(lists), Value::Null),
        Err(error) => (Value::Array(Vec::new()), Value::String(error.clone())),
    };
    json!({
        "requests": transport.requests.lock().expect("lock").clone(),
        "context_lists": lists,
        "error": error,
    })
}

async fn run_keyword(case: &Case) -> Value {
    let keyword = Arc::new(FakeKeywordSearcher {
        plan_error: if case.plan_error.is_empty() {
            None
        } else {
            Some(case.plan_error.clone())
        },
        execute_error: if case.execute_error.is_empty() {
            None
        } else {
            Some(case.execute_error.clone())
        },
        events: case.events.clone(),
        alert: case
            .alert
            .as_ref()
            .map(|alert| (alert.title.clone(), alert.description.clone())),
        plans: Mutex::new(Vec::new()),
        executions: Mutex::new(Vec::new()),
    });
    let context_client = dummy_client(
        None,
        Arc::new(FakeRepoEmbeddings::default()),
        keyword.clone(),
        false,
        None,
    );
    let matcher = matcher_from_deny(&case.deny);
    let args = GetContextArgs {
        repos: repo_id_names(&case.repos),
        repo_stats_present: false,
        file_patterns: case.file_patterns.clone(),
        query: case.query.clone(),
        code_results_count: case.code_count,
        text_results_count: case.text_count,
    };
    let result = context_client.get_keyword_context(&args, &matcher).await;
    let (lists, error) = match &result {
        Ok(lists) => (chunks_json(lists), Value::Null),
        Err(error) => (Value::Array(Vec::new()), Value::String(error.clone())),
    };

    let plans: Vec<Value> = keyword
        .plans
        .lock()
        .expect("lock")
        .iter()
        .map(|request| {
            json!({
                "version": request.version,
                "pattern_type": request.pattern_type,
                "query": request.query,
                "search_mode": request.search_mode.as_int(),
                "protocol": protocol_int(request.protocol),
                "context_lines": request.context_lines,
            })
        })
        .collect();
    let executions: Vec<Value> = keyword
        .executions
        .lock()
        .expect("lock")
        .iter()
        .map(|record| {
            json!({
                "code_count": record.code_count,
                "text_count": record.text_count,
                "matcher_present": record.matcher_present,
            })
        })
        .collect();
    json!({
        "plans": plans,
        "executions": executions,
        "context_lists": lists,
        "error": error,
    })
}

async fn run_dotcom(case: &Case) -> Value {
    let mut specs = BTreeMap::new();
    for spec in &case.repos {
        specs.insert(spec.name.clone(), spec.clone());
    }
    let gitserver = Arc::new(FakeGitserver {
        specs,
        default_branch_calls: Mutex::new(0),
        file_reader_calls: Mutex::new(0),
    });
    let filter = DotcomContentFilter {
        enabled: case.enabled,
    };
    let (repos, matcher) = filter
        .get_matcher(&repo_id_names(&case.repos), gitserver.as_ref())
        .await;
    json!({
        "filterable": repos.iter().map(|repo| json!({"id": repo.id, "name": repo.name})).collect::<Vec<_>>(),
        "matcher_present": true,
        "probes": matcher_probes(&matcher, &case.probes),
        "default_branch_calls": *gitserver.default_branch_calls.lock().expect("lock"),
        "new_file_reader_calls": *gitserver.file_reader_calls.lock().expect("lock"),
        "error": Value::Null,
    })
}

fn run_enterprise(case: &Case) -> Value {
    let filter = EnterpriseContentFilter::parse(&case.include, &case.exclude);
    let (repos, matcher, error) = filter.get_matcher(&repo_id_names(&case.repos));
    json!({
        "filterable": repos.iter().map(|repo| json!({"id": repo.id, "name": repo.name})).collect::<Vec<_>>(),
        "matcher_present": true,
        "probes": matcher_probes(&matcher, &case.probes),
        "error": error,
    })
}

async fn run_full(case: &Case) -> Value {
    let transport = Arc::new(FakeEmbeddingsTransport {
        status: case.response.status,
        body: serde_json::to_string(&case.response.body).unwrap_or_default(),
        requests: Mutex::new(Vec::new()),
    });
    let client = HttpEmbeddingsClient::new("http://fake.test", transport.clone());
    let keyword = Arc::new(FakeKeywordSearcher {
        plan_error: None,
        execute_error: None,
        events: case.events.clone(),
        alert: None,
        plans: Mutex::new(Vec::new()),
        executions: Mutex::new(Vec::new()),
    });
    let mut embedded = BTreeMap::new();
    for (index, spec) in case.repos.iter().enumerate() {
        embedded.insert(spec.id, case.embedded.get(index).copied().unwrap_or(false));
    }
    let context_client = dummy_client(
        Some(Arc::new(client)),
        Arc::new(FakeRepoEmbeddings { embedded }),
        keyword.clone(),
        case.embeddings_enabled,
        if case.disabled {
            Some("not authenticated".to_owned())
        } else {
            None
        },
    );
    let args = GetContextArgs {
        repos: repo_id_names(&case.repos),
        repo_stats_present: false,
        file_patterns: case.file_patterns.clone(),
        query: case.query.clone(),
        code_results_count: case.code_count,
        text_results_count: case.text_count,
    };
    let result = context_client.get_cody_context(&args).await;
    let (lists, error) = match &result {
        Ok(result) => (chunks_json(&result.context_lists), Value::Null),
        Err(error) => (Value::Array(Vec::new()), Value::String(error.clone())),
    };
    let plans: Vec<Value> = keyword
        .plans
        .lock()
        .expect("lock")
        .iter()
        .map(|request| {
            json!({
                "version": request.version,
                "pattern_type": request.pattern_type,
                "query": request.query,
                "search_mode": request.search_mode.as_int(),
                "protocol": protocol_int(request.protocol),
                "context_lines": request.context_lines,
            })
        })
        .collect();
    let executions: Vec<Value> = keyword
        .executions
        .lock()
        .expect("lock")
        .iter()
        .map(|record| {
            json!({
                "code_count": record.code_count,
                "text_count": record.text_count,
                "matcher_present": record.matcher_present,
            })
        })
        .collect();
    json!({
        "requests": transport.requests.lock().expect("lock").clone(),
        "plans": plans,
        "executions": executions,
        "context_lists": lists,
        "error": error,
    })
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn codycontext_client_diff() {
    if std::env::var("SG_M99C_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M99C_CORPUS").expect("M99C_CORPUS");
    let out_path = std::env::var("M99C_OUT").expect("M99C_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let mut output = Vec::with_capacity(cases.len());
    for case in &cases {
        let result = match case.kind.as_str() {
            "limits" => run_limits(case),
            "embeddings" => run_embeddings(case).await,
            "keyword" => run_keyword(case).await,
            "dotcom" => run_dotcom(case).await,
            "enterprise" => run_enterprise(case),
            "full" => run_full(case).await,
            other => panic!("unknown kind {other:?}"),
        };
        output.push(json!({
            "kind": case.kind,
            "name": case.name,
            "result": result,
        }));
    }

    std::fs::write(
        &out_path,
        serde_json::to_string(&output).expect("encode output"),
    )
    .expect("write output");
}
