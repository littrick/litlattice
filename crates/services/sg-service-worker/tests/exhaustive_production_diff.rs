//! M9.11b differential harness: the exhaustive production wiring.
//!
//! Drives the worker's production searcher — [`FromSearchClient::production`]
//! with [`FrontendRepoResolver`] (the M9.10a `search_repos::Resolver`) and
//! [`ProductionJobRunner`] (the frontend exhaustive job tree) — over injected
//! fakes, and writes the same normalized JSON as the Go oracle
//! (`internal/search/exhaustive/service/zz_m911b_oracle_test.go`), which runs
//! `FromSearchClient(client.Mocked(runtimeClients))`.
//!
//! Both sides consume `test_ws/p9-go/data/exhaustive_production/corpus.json`.
//! Gated by `SG_M911B_DIFF=1`; reads `M911B_CORPUS` and writes `M911B_OUT`.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use sg_db::repos::{RepoChangelistIDs, RepoListItem, RepoListOptions};
use sg_service_frontend::search_contexts::{
    RequestContext, SearchContext, SearchContextRepositoryRevisions,
};
use sg_service_frontend::search_env::SearcherGrpcConnectionCache;
use sg_service_frontend::search_job::{JobError, JobStreamer, PrivateRepoLister};
use sg_service_frontend::search_repos::{
    GitserverError, RepoResolverDb, Resolver, ResolverGitserver, ResolverSearcher,
    ResolverZoektLister,
};
use sg_service_frontend::search_runtime::SearcherRuntimeClients;
use sg_service_frontend::search_searcher_client::{
    protocol, SearcherClient, SearcherError, SearcherGitserver, SearcherLimiter, SymbolsClient,
    SymbolsParameters, SymbolsResponse,
};
use sg_service_frontend::search_stream::TreeRuntimeClients;
use sg_service_worker::jobs::exhaustive_search_job::{MatchWriter, NewSearcher};
use sg_service_worker::jobs::exhaustive_search_searcher::{
    FromSearchClient, FrontendRepoResolver, JobClients, ProductionJobRunner,
};
use sg_zoekt::indexed_repo_revs::MinimalRepo;
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListOptions, ListResponse, SearchOptions, SearchResult};
use time::OffsetDateTime;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct RepoSpec {
    id: i32,
    name: String,
    #[serde(default)]
    branches: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct Corpus {
    repos: Vec<RepoSpec>,
    cases: Vec<CaseSpec>,
}

#[derive(Debug, Deserialize)]
struct CaseSpec {
    name: String,
    kind: String,
    query: String,
    #[serde(default)]
    user_id: i32,
}

#[derive(Debug, Serialize)]
struct Out {
    name: String,
    ok: bool,
    error: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    refspecs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    repo_revs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    results: Vec<BTreeMap<String, Value>>,
}

impl Out {
    fn new(name: &str) -> Out {
        Out {
            name: name.to_owned(),
            ok: false,
            error: String::new(),
            refspecs: Vec::new(),
            repo_revs: Vec::new(),
            results: Vec::new(),
        }
    }
}

fn repo_item(repo: &RepoSpec) -> RepoListItem {
    RepoListItem {
        id: repo.id,
        name: repo.name.clone(),
        ..RepoListItem::default()
    }
}

// ---------------------------------------------------------------------------
// `search_repos::Resolver` fakes (mirror `searcher_test.go`'s DB/gitserver)
// ---------------------------------------------------------------------------

struct FakeDb {
    repos: Arc<Vec<RepoSpec>>,
}

impl RepoResolverDb for FakeDb {
    fn resolve_search_context(
        &self,
        _spec: &str,
        _ctx: &RequestContext,
    ) -> Result<SearchContext, JobError> {
        Ok(SearchContext {
            name: "global".to_owned(),
            public: true,
            auto_defined: true,
            ..SearchContext::default()
        })
    }

    fn list_minimal_repos(&self, options: &RepoListOptions) -> Result<Vec<RepoListItem>, JobError> {
        // `searcher_test.go:251-269`: include patterns are substring filters
        // (with "." matching everything); pagination is not exercised.
        Ok(self
            .repos
            .iter()
            .filter(|repo| {
                options
                    .include_patterns
                    .iter()
                    .all(|pattern| pattern == "." || repo.name.contains(pattern))
            })
            .map(repo_item)
            .collect())
    }

    fn list_minimal_repos_by_ids(&self, ids: &[i32]) -> Result<Vec<RepoListItem>, JobError> {
        Ok(self
            .repos
            .iter()
            .filter(|repo| ids.contains(&repo.id))
            .map(repo_item)
            .collect())
    }

    fn get_search_context_repository_revisions(
        &self,
        _id: i64,
    ) -> Result<Vec<SearchContextRepositoryRevisions>, JobError> {
        Ok(Vec::new())
    }

    fn batch_get_repo_commit_changelist(
        &self,
        _repos: &[RepoChangelistIDs],
    ) -> Result<BTreeMap<u32, BTreeMap<i64, String>>, JobError> {
        Ok(BTreeMap::new())
    }
}

struct FakeResolverGit {
    repos: Arc<Vec<RepoSpec>>,
}

impl ResolverGitserver for FakeResolverGit {
    fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, GitserverError> {
        self.repos
            .iter()
            .find(|candidate| candidate.name == repo)
            .and_then(|candidate| candidate.branches.get(rev))
            .cloned()
            .ok_or(GitserverError::Missing)
    }

    fn list_refs(&self, repo: &str) -> Result<Vec<String>, JobError> {
        let mut refs: Vec<String> = self
            .repos
            .iter()
            .find(|candidate| candidate.name == repo)
            .map(|candidate| {
                candidate
                    .branches
                    .keys()
                    .map(|branch| format!("refs/heads/{branch}"))
                    .collect()
            })
            .unwrap_or_default();
        refs.sort();
        Ok(refs)
    }

    fn commits_after(
        &self,
        _repo: &str,
        _rev: &str,
        _after: OffsetDateTime,
    ) -> Result<usize, GitserverError> {
        Ok(0)
    }
}

struct FakeZoekt;

impl ResolverZoektLister for FakeZoekt {
    fn list(&self, _query: &Query, _opts: Option<ListOptions>) -> Result<ListResponse, JobError> {
        Ok(ListResponse {
            repos: Vec::new(),
            repos_map: BTreeMap::new(),
            crashes: 0,
            stats: Default::default(),
        })
    }
}

struct FakeResolverSearcher;

impl ResolverSearcher for FakeResolverSearcher {
    fn search(&self, _request: &protocol::Request) -> Result<bool, JobError> {
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Job-tree runtime fakes (mirror `mockSearcher`/`mockGitserver`)
// ---------------------------------------------------------------------------

struct NoopLister;

impl PrivateRepoLister for NoopLister {
    fn list_minimal_repos(
        &self,
        _options: &sg_service_frontend::search_job::ReposListOptions,
    ) -> Result<Vec<MinimalRepo>, JobError> {
        Ok(Vec::new())
    }
}

struct NoopStreamer;

impl JobStreamer for NoopStreamer {
    fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        _sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError> {
        Ok(())
    }
}

struct RuntimeGit {
    repos: Arc<Vec<RepoSpec>>,
}

impl SearcherGitserver for RuntimeGit {
    fn resolve_revision(&self, repo: &str, revspec: &str) -> Result<String, SearcherError> {
        self.repos
            .iter()
            .find(|candidate| candidate.name == repo)
            .and_then(|candidate| candidate.branches.get(revspec))
            .cloned()
            .ok_or_else(|| {
                SearcherError::new(
                    sg_service_frontend::search_searcher_client::SearcherErrorKind::RevisionNotFound,
                    format!("revision not found: {repo}@{revspec}"),
                )
            })
    }

    fn default_branch(&self, _repo: &str) -> Result<String, SearcherError> {
        Ok("refs/heads/main".to_owned())
    }
}

/// `mockSearcher` (`searcher_test.go:290-335`): one `"line1"` content match for
/// every searched repo/revision.
struct AlwaysMatchSearcher;

impl SearcherClient for AlwaysMatchSearcher {
    fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
        Ok(vec!["test".to_owned()])
    }

    fn search(
        &self,
        _request: &protocol::Request,
        on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError> {
        on_match(protocol::FileMatch {
            path: String::new(),
            language: String::new(),
            limit_hit: false,
            chunk_matches: vec![protocol::ChunkMatch {
                content: "line1".to_owned(),
                content_start: protocol::Location {
                    offset: 0,
                    line: 1,
                    column: 0,
                },
                ranges: vec![protocol::Range {
                    start: protocol::Location {
                        offset: 1,
                        line: 1,
                        column: 1,
                    },
                    end: protocol::Location {
                        offset: 3,
                        line: 1,
                        column: 3,
                    },
                }],
            }],
        });
        Ok(false)
    }
}

struct NoopLimiter;

impl SearcherLimiter for NoopLimiter {
    fn set_limit(&self, _limit: usize) {}

    fn acquire(&self) -> Result<(), SearcherError> {
        Ok(())
    }
}

struct NoopSymbols;

impl SymbolsClient for NoopSymbols {
    fn search(&self, _params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        Err(SearcherError::other(
            "symbols are not used by text searches",
        ))
    }
}

fn runtime(repos: Arc<Vec<RepoSpec>>) -> TreeRuntimeClients {
    TreeRuntimeClients {
        db: Arc::new(NoopLister),
        zoekt: Arc::new(NoopStreamer),
        max_repos: None,
        searcher: Some(Arc::new(SearcherRuntimeClients {
            client: Arc::new(AlwaysMatchSearcher),
            limiter: Arc::new(NoopLimiter),
            gitserver: Arc::new(RuntimeGit { repos }),
            symbols: Arc::new(NoopSymbols),
            searcher_connection_cache: Arc::new(SearcherGrpcConnectionCache::new()),
        })),
        commit: None,
        repo_pager: None,
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct CollectingWriter {
    values: Mutex<Vec<Value>>,
}

impl MatchWriter for CollectingWriter {
    fn write(&self, r#match: Value) -> Result<(), String> {
        self.values.lock().unwrap().push(r#match);
        Ok(())
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

fn normalize_results(writer: &CollectingWriter) -> Vec<BTreeMap<String, Value>> {
    writer
        .values
        .lock()
        .unwrap()
        .iter()
        .map(|value| {
            let mut map = BTreeMap::new();
            for key in ["type", "repositoryID", "commit", "path", "chunkMatches"] {
                map.insert(
                    key.to_owned(),
                    value.get(key).cloned().unwrap_or(Value::Null),
                );
            }
            map
        })
        .collect()
}

fn refspec_strings(specs: &[sg_db::exhaustive_search::RepositoryRevSpecs]) -> Vec<String> {
    specs
        .iter()
        .map(|spec| {
            format!(
                "RepositoryRevSpec{{{}@{}}}",
                spec.repository, spec.revision_specifiers
            )
        })
        .collect()
}

fn repo_rev_strings(repo_revs: &[sg_db::exhaustive_search::RepositoryRevision]) -> Vec<String> {
    repo_revs
        .iter()
        .map(|repo_rev| {
            format!(
                "RepositoryRevision{{{}@{}}}",
                repo_rev.repository_rev_specs.repository, repo_rev.revision
            )
        })
        .collect()
}

#[tokio::test]
async fn exhaustive_production_diff() {
    if std::env::var("SG_M911B_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M911B_CORPUS").expect("M911B_CORPUS");
    let out_path = std::env::var("M911B_OUT").expect("M911B_OUT");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");
    let repos = Arc::new(corpus.repos);

    let mut outputs = Vec::new();
    for case in &corpus.cases {
        outputs.push(run_case(case, Arc::clone(&repos)).await);
    }

    let mut encoded = serde_json::to_string_pretty(&outputs).expect("encode");
    encoded.push('\n');
    std::fs::write(&out_path, encoded).expect("write output");
}

async fn run_case(case: &CaseSpec, repos: Arc<Vec<RepoSpec>>) -> Out {
    let mut out = Out::new(&case.name);

    let resolver = Arc::new(Resolver {
        db: Arc::new(FakeDb {
            repos: Arc::clone(&repos),
        }),
        gitserver: Arc::new(FakeResolverGit {
            repos: Arc::clone(&repos),
        }),
        zoekt: Arc::new(FakeZoekt),
        searcher: Some(Arc::new(FakeResolverSearcher)),
        max_repos: None,
    });

    let searcher = FromSearchClient::production(JobClients::new(
        Arc::new(FrontendRepoResolver::new(resolver)),
        Arc::new(ProductionJobRunner::with_now(
            runtime(repos),
            OffsetDateTime::UNIX_EPOCH,
        )),
    ));

    let query = match searcher.new_search(case.user_id, &case.query).await {
        Ok(query) => query,
        Err(error) => {
            out.error = error;
            return out;
        }
    };

    match case.kind.as_str() {
        "new_search" => {
            out.ok = true;
        }
        "full" => {
            let refspecs = match query.repository_rev_specs().await {
                Ok(refspecs) => refspecs,
                Err(error) => {
                    out.error = error;
                    return out;
                }
            };
            out.refspecs = refspec_strings(&refspecs);

            let mut repo_revs = Vec::new();
            for refspec in &refspecs {
                match query.resolve_repository_rev_spec(refspec).await {
                    Ok(part) => repo_revs.extend(part),
                    Err(error) => {
                        out.error = error;
                        return out;
                    }
                }
            }
            out.repo_revs = repo_rev_strings(&repo_revs);

            let writer = CollectingWriter {
                values: Mutex::new(Vec::new()),
            };
            for repo_rev in &repo_revs {
                if let Err(error) = query.search(repo_rev, &writer).await {
                    out.error = error;
                    return out;
                }
            }
            out.results = normalize_results(&writer);
            out.ok = true;
        }
        other => panic!("unknown case kind {other}"),
    }

    out
}
