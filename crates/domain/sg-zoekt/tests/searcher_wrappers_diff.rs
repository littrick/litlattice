//! M9.8gr differential harness: `internal/search/backend/cached.go` and the
//! pure label functions of `metered_searcher.go` against
//! [`sg_zoekt::cached`] / [`sg_zoekt::metered`].
//!
//! Run with `SG_M9GR_DIFF=1 M9GR_CORPUS=<json> M9GR_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). The Go oracle emits the same
//! normalized JSON. Cache cases use a zero jitter so the TTL decisions match
//! Go: the corpus offsets stay clear of every possible jitter boundary.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::cached::{CachedStreamer, ManualClock};
use sg_zoekt::metered::{
    code_and_error_str, list_category, list_category_raw, query_string, ErrorClassifier,
};
use sg_zoekt::query::{Query, TypeKind};
use sg_zoekt::streamer::Streamer;
use sg_zoekt::types::{
    ListOptions, ListResponse, MinimalRepoListEntry, RepoListEntry, RepoListField, Repository,
    SearchChunk, SearchOptions,
};

#[derive(Deserialize)]
struct Corpus {
    cache_cases: Vec<CacheCase>,
    query_string_cases: Vec<LabelCase>,
    list_category_cases: Vec<ListCategoryCase>,
    error_code_cases: Vec<ErrorCodeCase>,
}

#[derive(Deserialize)]
struct CacheCase {
    name: String,
    ttl_ms: u64,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct Step {
    op: Op,
    offset_ms: u64,
    query: Expr,
    opts: Option<String>,
    expect_calls: usize,
    upstream: Upstream,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Op {
    List,
    Update,
}

#[derive(Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Expr {
    Nil,
    Const {
        value: bool,
    },
    Substr {
        pattern: String,
        #[serde(default)]
        case_sensitive: bool,
        #[serde(default)]
        file_name: bool,
        #[serde(default)]
        content: bool,
    },
    Regexp {
        regexp: String,
        #[serde(default)]
        case_sensitive: bool,
        #[serde(default)]
        file_name: bool,
        #[serde(default)]
        content: bool,
    },
    And {
        children: Vec<Expr>,
    },
    Or {
        children: Vec<Expr>,
    },
    Not {
        child: Box<Expr>,
    },
    Repo {
        regexp: String,
    },
    RepoRegexp {
        regexp: String,
    },
    Language {
        language: String,
    },
    Symbol {
        child: Box<Expr>,
    },
    Type {
        r#type: String,
        child: Box<Expr>,
    },
    Boost {
        boost: f64,
        child: Box<Expr>,
    },
    Branch {
        pattern: String,
        #[serde(default)]
        exact: bool,
    },
    #[serde(rename = "reposet")]
    RepoSet {
        repos: Vec<String>,
    },
    #[serde(rename = "filenameset")]
    FileNameSet {
        names: Vec<String>,
    },
}

impl Expr {
    fn to_query(&self) -> Option<Query> {
        match self {
            Expr::Nil => None,
            Expr::Const { value } => Some(Query::Const(*value)),
            Expr::Substr {
                pattern,
                case_sensitive,
                file_name,
                content,
            } => Some(Query::Substring {
                pattern: pattern.clone(),
                case_sensitive: *case_sensitive,
                file_name: *file_name,
                content: *content,
            }),
            Expr::Regexp {
                regexp,
                case_sensitive,
                file_name,
                content,
            } => Some(Query::Regexp {
                regexp: regexp.clone(),
                case_sensitive: *case_sensitive,
                file_name: *file_name,
                content: *content,
            }),
            Expr::And { children } => {
                Some(Query::And(children.iter().map(Expr::required).collect()))
            }
            Expr::Or { children } => Some(Query::Or(children.iter().map(Expr::required).collect())),
            Expr::Not { child } => Some(Query::Not(Box::new(child.required()))),
            Expr::Repo { regexp } => Some(Query::Repo(regexp.clone())),
            Expr::RepoRegexp { regexp } => Some(Query::RepoRegexp(regexp.clone())),
            Expr::Language { language } => Some(Query::Language(language.clone())),
            Expr::Symbol { child } => Some(Query::Symbol(Box::new(child.required()))),
            Expr::Type { r#type, child } => Some(Query::Type {
                child: Box::new(child.required()),
                kind: match r#type.as_str() {
                    "filematch" => TypeKind::FileMatch,
                    "filename" => TypeKind::FileName,
                    "repo" => TypeKind::Repo,
                    other => panic!("unknown type kind {other}"),
                },
            }),
            Expr::Boost { boost, child } => Some(Query::Boost {
                child: Box::new(child.required()),
                boost: *boost,
            }),
            Expr::Branch { pattern, exact } => Some(Query::Branch {
                pattern: pattern.clone(),
                exact: *exact,
            }),
            Expr::RepoSet { repos } => Some(Query::RepoSet(
                repos.iter().cloned().map(|repo| (repo, true)).collect(),
            )),
            Expr::FileNameSet { names } => Some(Query::FileNameSet(
                names.iter().cloned().collect::<BTreeSet<_>>(),
            )),
        }
    }

    fn required(&self) -> Query {
        self.to_query().expect("nil query inside a composite")
    }
}

#[derive(Deserialize, Clone)]
struct Upstream {
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    repos_map: Vec<u32>,
    #[serde(default)]
    crashes: i64,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize, Clone)]
struct RepoSpec {
    id: u32,
    name: String,
}

impl Upstream {
    fn into_result(self) -> Result<ListResponse, TestError> {
        if let Some(message) = self.error {
            return Err(TestError::Other(message));
        }
        Ok(ListResponse {
            repos: self
                .repos
                .into_iter()
                .map(|repo| RepoListEntry {
                    repository: Repository {
                        id: repo.id,
                        name: repo.name,
                        ..Repository::default()
                    },
                    index_time_unix: 0,
                })
                .collect(),
            repos_map: self
                .repos_map
                .into_iter()
                .map(|id| (id, MinimalRepoListEntry::default()))
                .collect(),
            crashes: self.crashes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestError {
    Canceled,
    DeadlineExceeded,
    Other(String),
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestError::Canceled => f.write_str("context canceled"),
            TestError::DeadlineExceeded => f.write_str("context deadline exceeded"),
            TestError::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for TestError {}

impl ErrorClassifier for TestError {
    fn is_canceled(&self) -> bool {
        matches!(self, TestError::Canceled)
    }

    fn is_deadline_exceeded(&self) -> bool {
        matches!(self, TestError::DeadlineExceeded)
    }
}

struct FakeState {
    queue: VecDeque<Upstream>,
    calls: usize,
}

#[derive(Clone)]
struct FakeStreamer {
    state: Arc<Mutex<FakeState>>,
}

impl FakeStreamer {
    fn new(responses: Vec<Upstream>) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeState {
                queue: responses.into(),
                calls: 0,
            })),
        }
    }

    fn calls(&self) -> usize {
        self.state.lock().expect("fake state poisoned").calls
    }
}

impl Streamer for FakeStreamer {
    type Error = TestError;

    fn list(
        &self,
        _query: &Query,
        _opts: Option<ListOptions>,
    ) -> impl Future<Output = Result<ListResponse, TestError>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().expect("fake state poisoned");
            state.calls += 1;
            match state.queue.pop_front() {
                Some(upstream) => upstream.into_result(),
                None => Err(TestError::Other("no upstream response".to_owned())),
            }
        }
    }

    async fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        _sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), TestError> {
        Ok(())
    }
}

#[derive(Deserialize)]
struct LabelCase {
    name: String,
    expr: Expr,
}

#[derive(Deserialize)]
struct ListCategoryCase {
    name: String,
    input: Option<i32>,
}

#[derive(Deserialize)]
struct ErrorCodeCase {
    name: String,
    kind: String,
}

#[test]
fn searcher_wrappers_diff() {
    if std::env::var("SG_M9GR_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GR_CORPUS").expect("M9GR_CORPUS must be set when SG_M9GR_DIFF=1");
    let out_path = std::env::var("M9GR_OUT").expect("M9GR_OUT must be set when SG_M9GR_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    let output = runtime.block_on(async {
        let mut cache = Vec::with_capacity(corpus.cache_cases.len());
        for case in &corpus.cache_cases {
            cache.push(run_cache_case(case).await);
        }

        let query_string: Vec<Value> = corpus
            .query_string_cases
            .iter()
            .map(|case| {
                let value = match case.expr.to_query() {
                    Some(query) => query_string(Some(&query)),
                    None => query_string(None),
                };
                json!({"name": case.name, "value": value})
            })
            .collect();

        let list_category: Vec<Value> = corpus
            .list_category_cases
            .iter()
            .map(|case| {
                let value = match case.input {
                    Some(field) => list_category_raw(field),
                    None => list_category(None),
                };
                json!({"name": case.name, "value": value})
            })
            .collect();

        let error_code: Vec<Value> = corpus
            .error_code_cases
            .iter()
            .map(|case| {
                let error = match case.kind.as_str() {
                    "nil" => None,
                    "canceled" => Some(TestError::Canceled),
                    "deadline" => Some(TestError::DeadlineExceeded),
                    "other" => Some(TestError::Other("boom".to_owned())),
                    other => panic!("unknown error kind {other}"),
                };
                let (code, message) =
                    code_and_error_str(error.as_ref().map(|err| err as &dyn ErrorClassifier));
                json!({"name": case.name, "code": code, "error": message})
            })
            .collect();

        json!({
            "cache": cache,
            "query_string": query_string,
            "list_category": list_category,
            "error_code": error_code,
        })
    });

    std::fs::write(
        &out_path,
        serde_json::to_string_pretty(&output).expect("encode output"),
    )
    .expect("write output");
}

async fn run_cache_case(case: &CacheCase) -> Value {
    let responses: Vec<Upstream> = case
        .steps
        .iter()
        .filter(|step| step.expect_calls > 0)
        .map(|step| step.upstream.clone())
        .collect();
    let fake = FakeStreamer::new(responses);
    let clock = ManualClock::new();
    let cached = CachedStreamer::with_clock_and_jitter(
        Duration::from_millis(case.ttl_ms),
        fake.clone(),
        clock.clone(),
        |_| 0,
    );

    let mut steps = Vec::with_capacity(case.steps.len());
    let mut previous_calls = 0usize;
    for (index, step) in case.steps.iter().enumerate() {
        clock.set(Duration::from_millis(step.offset_ms));
        let query = step.query.required();
        let opts = decode_opts(step.opts.as_deref());

        let result = match step.op {
            Op::List => cached.list(&query, opts).await,
            Op::Update => cached.debug_update(&query, opts).await,
        };
        cached.wait_for_refresh().await;

        let calls = fake.calls();
        assert_eq!(
            calls,
            previous_calls + step.expect_calls,
            "case {} step {} upstream call count",
            case.name,
            index
        );
        previous_calls = calls;

        let cache_entries: Vec<Value> = cached
            .debug_cache_entries()
            .await
            .into_iter()
            .map(|entry| {
                json!({
                    "key": entry.key,
                    "ttl_ms": entry.ttl.as_millis() as i64,
                    "has_error": entry.has_error,
                    "crashes": entry.crashes,
                })
            })
            .collect();

        steps.push(json!({
            "step": index,
            "offset_ms": step.offset_ms,
            "op": match step.op { Op::List => "list", Op::Update => "update" },
            "result": summarize_result(&result),
            "upstream_calls": calls,
            "cache": cache_entries,
        }));
    }

    json!({"case": case.name, "steps": steps})
}

fn decode_opts(opts: Option<&str>) -> Option<ListOptions> {
    opts.map(|opts| ListOptions {
        field: match opts {
            "repos" => RepoListField::Repos,
            "repos_map" => RepoListField::ReposMap,
            "unknown" => RepoListField::Unknown,
            other => panic!("unknown opts {other}"),
        },
    })
}

fn summarize_result(result: &Result<ListResponse, TestError>) -> Value {
    match result {
        Ok(list) => {
            let mut repos: Vec<Value> = list
                .repos
                .iter()
                .map(|entry| json!({"id": entry.repository.id, "name": entry.repository.name}))
                .collect();
            repos.sort_by_key(|repo| repo["id"].as_u64().unwrap_or_default());
            let mut repos_map: Vec<u32> = list.repos_map.keys().copied().collect();
            repos_map.sort_unstable();
            json!({
                "kind": "ok",
                "repos": repos,
                "repos_map": repos_map,
                "crashes": list.crashes,
            })
        }
        Err(error) => json!({"kind": "error", "message": error.to_string()}),
    }
}
