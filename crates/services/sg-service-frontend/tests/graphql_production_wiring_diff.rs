#![allow(clippy::unwrap_used)]

//! M9.8ii differential of the production GraphQL assembly
//! (`serve_cmd.go:213` -> `graphqlbackend.NewSchema` optional resolvers,
//! `internal/searchcontexts/init.go`, `cmd/frontend/internal/codeintel/init.go`
//! and `internal/codeintel/ranking/transport/graphql/root_resolver.go`).
//!
//! Go oracle: `cmd/frontend/graphqlbackend/zz_m9ii_oracle_test.go`.
//! The Rust side exercises the same assembled surface through
//! [`SchemaGraphqlDeps::with_production_surfaces`] with fakes for the search
//! deps, search contexts database and ranking service; the Go side drives the
//! real `mainSchema` + `search_contexts.graphql` + `codeintel.ranking.graphql`
//! with fake root resolvers.
//!
//! Env:
//!
//! * `SG_M9II_DIFF=1`
//! * `M9II_CORPUS` — the corpus JSON
//! * `M9II_OUT` — output file

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value};
use time::OffsetDateTime;
use tokio_stream::Stream;

use sg_graphql::exec::Actor;
use sg_graphql::schema::assemble_oss;
use sg_search_query::parser::SearchType;
use sg_service_frontend::graphql_api::{
    GraphqlDeps, GraphqlExecRequest, GraphqlResponse, SchemaGraphqlDeps,
};
use sg_service_frontend::ranking_graphql::{
    DateTime, NoopObserver, Progress, RankingError, RankingService, SiteAdminChecker, Summary,
};
use sg_service_frontend::search_client::{Features, Protocol, SearchMode};
use sg_service_frontend::search_contexts::{
    Error, GetSearchContextOptions, MinimalRepo, Namespace, OrgMembership, ReposListOptions,
    RequestContext, SearchContext, SearchContextRepositoryRevisions, SearchContextsDatabase, User,
};
use sg_service_frontend::search_contexts_graphql::{
    ListSearchContextsOptions, ListSearchContextsPageOptions, MinimalRepoRef, NamespaceOrg,
    NamespaceUser, SearchContextRecord, SearchContextsGraphqlDatabase,
};
use sg_service_frontend::search_graphql::BackendSearchExecutor;
use sg_service_frontend::search_stream::args::Args;
use sg_service_frontend::search_stream::convert::{
    FileMatch, FileRef, Match, MinimalRepo as MatchRepo, RepoMetadata,
};
use sg_service_frontend::search_stream::progress::{SearchEvent, Stats};
use sg_service_frontend::search_stream::{ExecuteItem, PlanError, SearchDeps, SearchInputs};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct ProgressDef {
    started_at: String,
    completed_at: Option<String>,
    processed: i32,
    total: i32,
}

#[derive(Debug, Clone, Deserialize)]
struct SummaryDef {
    graph_key: String,
    visible_to_zoekt: bool,
    path_mapper_progress: ProgressDef,
    reference_mapper_progress: ProgressDef,
    reducer_progress: Option<ProgressDef>,
}

#[derive(Debug, Clone, Deserialize)]
struct ValueOkDef {
    value: String,
    ok: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct CountsDef {
    num_target_indexes: i32,
    num_exported_indexes: i32,
    num_repositories_without_current_ranks: i32,
}

#[derive(Debug, Clone, Deserialize)]
struct ServiceDef {
    derivative: ValueOkDef,
    summaries: Vec<SummaryDef>,
    next_job_starts_at: ValueOkDef,
    coverage_counts: CountsDef,
}

#[derive(Debug, Clone, Deserialize)]
struct WiringCase {
    name: String,
    /// `anonymous` or `user`.
    actor: String,
    /// `service` (production `with_ranking_service` path, authorized by the
    /// per-request viewer checker) or `resolver` (prebuilt root resolver with
    /// an allowing checker, exposing the success payload).
    ranking: String,
    queries: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WiringCorpus {
    service: ServiceDef,
    cases: Vec<WiringCase>,
}

fn parse_time(value: &str) -> DateTime {
    DateTime::parse_rfc3339(value).expect("RFC3339 corpus date")
}

fn progress(def: &ProgressDef) -> Progress {
    Progress {
        started_at: parse_time(&def.started_at),
        completed_at: def.completed_at.as_deref().map(parse_time),
        processed: i64::from(def.processed),
        total: i64::from(def.total),
    }
}

fn summary(def: &SummaryDef) -> Summary {
    Summary {
        graph_key: def.graph_key.clone(),
        visible_to_zoekt: def.visible_to_zoekt,
        path_mapper_progress: progress(&def.path_mapper_progress),
        reference_mapper_progress: progress(&def.reference_mapper_progress),
        reducer_progress: def.reducer_progress.as_ref().map(progress),
    }
}

// ---------------------------------------------------------------------------
// Fake ranking service
// ---------------------------------------------------------------------------

struct FakeRankingService {
    def: ServiceDef,
}

#[async_trait]
impl RankingService for FakeRankingService {
    async fn derivative_graph_key(&self) -> Result<(String, bool), RankingError> {
        Ok((self.def.derivative.value.clone(), self.def.derivative.ok))
    }

    async fn bump_derivative_graph_key(&self) -> Result<(), RankingError> {
        Ok(())
    }

    async fn summaries(&self) -> Result<Vec<Summary>, RankingError> {
        Ok(self.def.summaries.iter().map(summary).collect())
    }

    async fn next_job_starts_at(&self) -> Result<(DateTime, bool), RankingError> {
        if self.def.next_job_starts_at.ok {
            Ok((parse_time(&self.def.next_job_starts_at.value), true))
        } else {
            Ok((DateTime::from_unix_seconds(0), false))
        }
    }

    async fn coverage_counts(
        &self,
        _graph_key: &str,
    ) -> Result<sg_service_frontend::ranking_graphql::CoverageCounts, RankingError> {
        Ok(sg_service_frontend::ranking_graphql::CoverageCounts {
            num_target_indexes: i64::from(self.def.coverage_counts.num_target_indexes),
            num_exported_indexes: i64::from(self.def.coverage_counts.num_exported_indexes),
            num_repositories_without_current_ranks: i64::from(
                self.def
                    .coverage_counts
                    .num_repositories_without_current_ranks,
            ),
        })
    }

    async fn delete_ranking_progress(&self, _graph_key: &str) -> Result<(), RankingError> {
        Ok(())
    }
}

struct AllowChecker;

#[async_trait]
impl SiteAdminChecker for AllowChecker {
    async fn check_current_user_is_site_admin(&self) -> Result<(), RankingError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fake search deps (driven through the real BackendSearchExecutor)
// ---------------------------------------------------------------------------

fn file_match() -> FileMatch {
    FileMatch {
        file: FileRef {
            repo: MatchRepo {
                id: 1,
                name: "github.com/foo/bar".to_owned(),
            },
            commit_id: "abcdef1234567890".to_owned(),
            path: "src/main.go".to_owned(),
            input_rev: Some("main".to_owned()),
            precise_language: "Go".to_owned(),
        },
        chunk_matches: Vec::new(),
        symbols: Vec::new(),
        path_matches: Vec::new(),
        limit_hit: false,
        debug: None,
    }
}

struct FakeSearchDeps;

#[async_trait]
impl SearchDeps for FakeSearchDeps {
    async fn plan(&self, _args: &Args) -> Result<SearchInputs, PlanError> {
        Ok(SearchInputs {
            max_results: 30,
            original_query: "type:file foo".to_owned(),
            plan: Vec::new(),
            search_mode: SearchMode::Precise,
            pattern_type: SearchType::Standard,
            features: Features::default(),
            protocol: Protocol::Streaming,
            context_lines: 0,
            sanitize_search_patterns: Vec::new(),
            on_sourcegraph_dot_com: false,
        })
    }

    async fn execute(
        &self,
        _inputs: &SearchInputs,
        _query: &str,
    ) -> Pin<Box<dyn Stream<Item = ExecuteItem> + Send>> {
        Box::pin(tokio_stream::iter(vec![ExecuteItem::Event(SearchEvent {
            results: vec![Match::File(file_match())],
            stats: Stats::default(),
        })]))
    }

    async fn repo_metadata(&self, ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String> {
        Ok(ids
            .iter()
            .map(|id| {
                (
                    *id,
                    RepoMetadata {
                        id: *id,
                        name: "github.com/foo/bar".to_owned(),
                        description: String::new(),
                        fork: false,
                        archived: false,
                        private: false,
                        stars: 0,
                        last_fetched: None,
                        key_value_pairs: None,
                        topics: None,
                    },
                )
            })
            .collect())
    }

    async fn repo_names(&self, _ids: &[i32]) -> Vec<(i32, String)> {
        Vec::new()
    }

    fn is_authenticated(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Fake search contexts database
// ---------------------------------------------------------------------------

fn records() -> Vec<SearchContextRecord> {
    vec![
        SearchContextRecord {
            id: 0,
            name: "global".to_owned(),
            description: "All repositories on Sourcegraph".to_owned(),
            public: true,
            namespace_user_id: 0,
            namespace_org_id: 0,
            namespace_user_name: String::new(),
            namespace_org_name: String::new(),
            query: String::new(),
            auto_defined: true,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            default: false,
            starred: false,
        },
        SearchContextRecord {
            id: 1,
            name: "my-context".to_owned(),
            description: "My context".to_owned(),
            public: false,
            namespace_user_id: 1,
            namespace_org_id: 0,
            namespace_user_name: "alice".to_owned(),
            namespace_org_name: String::new(),
            query: "repo:foo".to_owned(),
            auto_defined: false,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            default: false,
            starred: false,
        },
    ]
}

struct FakeSearchContexts;

impl SearchContextsDatabase for FakeSearchContexts {
    fn get_namespace_by_name(&self, _name: &str) -> Result<Namespace, Error> {
        Ok(Namespace {
            user: 1,
            organization: 0,
        })
    }

    fn get_org_member_by_org_id_and_user_id(
        &self,
        _org_id: i32,
        _user_id: i32,
    ) -> Result<Option<OrgMembership>, Error> {
        Ok(None)
    }

    fn get_current_user(&self, _uid: i32) -> Result<Option<User>, Error> {
        Ok(None)
    }

    fn get_search_context(&self, opts: &GetSearchContextOptions) -> Result<SearchContext, Error> {
        records()
            .into_iter()
            .find(|record| record.name == opts.name && !record.auto_defined)
            .map(|record| SearchContext {
                id: record.id,
                name: record.name,
                description: record.description,
                public: record.public,
                namespace_user_id: record.namespace_user_id,
                namespace_org_id: record.namespace_org_id,
                namespace_user_name: record.namespace_user_name,
                namespace_org_name: record.namespace_org_name,
                query: record.query,
                auto_defined: record.auto_defined,
            })
            .ok_or_else(Error::search_context_not_found)
    }

    fn create_search_context_with_repository_revisions(
        &self,
        _search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        Err(Error::other("unused"))
    }

    fn update_search_context_with_repository_revisions(
        &self,
        _search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        Err(Error::other("unused"))
    }

    fn delete_search_context(&self, _search_context_id: i64) -> Result<(), Error> {
        Ok(())
    }

    fn get_all_revisions_for_repos(
        &self,
        _repo_ids: &[i32],
    ) -> Result<BTreeMap<i32, Vec<String>>, Error> {
        Ok(BTreeMap::new())
    }

    fn get_all_queries(&self) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }

    fn get_search_context_repository_revisions(
        &self,
        _search_context_id: i64,
    ) -> Result<Vec<SearchContextRepositoryRevisions>, Error> {
        Ok(Vec::new())
    }

    fn list_minimal_repos(&self, _opts: &ReposListOptions) -> Result<Vec<MinimalRepo>, Error> {
        Ok(Vec::new())
    }

    fn create_search_context_star_for_user(
        &self,
        _user_id: i32,
        _search_context_id: i64,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn delete_search_context_star_for_user(
        &self,
        _user_id: i32,
        _search_context_id: i64,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn set_user_default_search_context_id(
        &self,
        _user_id: i32,
        _search_context_id: i64,
    ) -> Result<(), Error> {
        Ok(())
    }
}

impl SearchContextsGraphqlDatabase for FakeSearchContexts {
    fn as_search_contexts_database(&self) -> &dyn SearchContextsDatabase {
        self
    }

    fn get_search_context_by_id(
        &self,
        _ctx: &RequestContext,
        id: i64,
    ) -> Result<SearchContextRecord, Error> {
        records()
            .into_iter()
            .find(|record| record.id == id)
            .ok_or_else(Error::search_context_not_found)
    }

    fn list_search_contexts(
        &self,
        _ctx: &RequestContext,
        _page: &ListSearchContextsPageOptions,
        _opts: &ListSearchContextsOptions,
    ) -> Result<Vec<SearchContextRecord>, Error> {
        Ok(records())
    }

    fn count_search_contexts(
        &self,
        _ctx: &RequestContext,
        _opts: &ListSearchContextsOptions,
    ) -> Result<i32, Error> {
        Ok(records().len() as i32)
    }

    fn get_default_search_context_for_current_user(
        &self,
        _ctx: &RequestContext,
    ) -> Result<SearchContextRecord, Error> {
        Ok(records()[0].clone())
    }

    fn get_repos_set_by_ids(&self, _ids: &[i32]) -> Result<BTreeMap<i32, MinimalRepoRef>, Error> {
        Ok(BTreeMap::new())
    }

    fn get_user_by_id(&self, id: i32) -> Result<NamespaceUser, Error> {
        Ok(NamespaceUser {
            id,
            username: "alice".to_owned(),
        })
    }

    fn get_org_by_id(&self, id: i32) -> Result<NamespaceOrg, Error> {
        Ok(NamespaceOrg {
            id,
            name: "org".to_owned(),
        })
    }

    fn get_orgs_by_user_id(&self, _user_id: i32) -> Result<Vec<NamespaceOrg>, Error> {
        Ok(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn normalized_response(response: &GraphqlResponse) -> Value {
    let data = response
        .data
        .as_ref()
        .map(|data| serde_json::from_str::<Value>(data).expect("data json"))
        .unwrap_or(Value::Null);
    let errors: Vec<Value> = response
        .errors
        .iter()
        .map(|error| {
            Value::Object(Map::from_iter([
                ("message".to_owned(), Value::from(error.message.clone())),
                (
                    "path".to_owned(),
                    Value::Array(error.path.clone().unwrap_or_default()),
                ),
            ]))
        })
        .collect();
    Value::Object(Map::from_iter([
        ("data".to_owned(), data),
        ("errors".to_owned(), Value::Array(errors)),
    ]))
}

fn actor(case: &WiringCase) -> Actor {
    match case.actor.as_str() {
        "anonymous" => Actor::Anonymous,
        "user" => Actor::User { uid: 2 },
        other => panic!("unknown actor {other}"),
    }
}

#[tokio::test]
async fn graphql_production_wiring_harness() {
    if std::env::var("SG_M9II_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9II_CORPUS").expect("M9II_CORPUS");
    let out_path = std::env::var("M9II_OUT").expect("M9II_OUT");
    let corpus: WiringCorpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let base_schema = Arc::new(assemble_oss().expect("oss schema"));

    let mut cases = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let ranking_service: Arc<dyn RankingService> = Arc::new(FakeRankingService {
            def: corpus.service.clone(),
        });
        let search_executor: Arc<dyn sg_service_frontend::search_graphql::SearchExecutor> =
            Arc::new(BackendSearchExecutor::new(Arc::new(FakeSearchDeps)));
        let search_contexts: sg_service_frontend::graphql_resolvers::SearchContextsDatabaseHandle =
            Arc::new(FakeSearchContexts);

        let deps = SchemaGraphqlDeps::new(Arc::clone(&base_schema));
        let deps = match case.ranking.as_str() {
            "service" => deps.with_production_surfaces(
                Some(search_contexts),
                true,
                Some((ranking_service, "dev".to_owned())),
                Some(search_executor),
            ),
            "resolver" => {
                let deps = deps
                    .with_production_surfaces(
                        Some(search_contexts),
                        true,
                        None,
                        Some(search_executor),
                    )
                    .expect("production surfaces");
                let resolver = Arc::new(sg_service_frontend::ranking_graphql::RootResolver::new(
                    ranking_service,
                    Arc::new(AllowChecker),
                    Arc::new(NoopObserver),
                    "dev",
                ));
                deps.with_ranking(resolver)
            }
            other => panic!("unknown ranking mode {other}"),
        }
        .expect("assembled schema");

        let mut responses = Vec::with_capacity(case.queries.len());
        for query in &case.queries {
            let variables = Map::new();
            let response = deps
                .exec(GraphqlExecRequest {
                    query,
                    operation_name: "",
                    variables: &variables,
                    actor: actor(case),
                })
                .await;
            responses.push(normalized_response(&response));
        }
        cases.push(Value::Object(Map::from_iter([
            ("name".to_owned(), Value::from(case.name.clone())),
            ("responses".to_owned(), Value::Array(responses)),
        ])));
    }
    let document = Value::Object(Map::from_iter([("cases".to_owned(), Value::Array(cases))]));
    std::fs::write(out_path, serde_json::to_string(&document).expect("json")).expect("write");
}
