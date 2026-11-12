#![allow(clippy::unwrap_used)]

//! M9.8ic differential of the ranking GraphQL executor wiring
//! (`codeintel.ranking.graphql`, `cmd/frontend/internal/codeintel/init.go:107,118`,
//! `internal/codeintel/ranking/transport/graphql/root_resolver.go:22,35,84,96`).
//!
//! Go oracle: `cmd/frontend/graphqlbackend/zz_m9ic_oracle_test.go`.
//! The Rust side drives the real `SchemaGraphqlDeps::exec` with the M9.8hd
//! `ranking_graphql::RootResolver` and fake `RankingService`/`SiteAdminChecker`
//! traits; the Go side drives graph-gophers with the same minimal execution SDL
//! and a fake ranking store with identical semantics.
//!
//! Env:
//!
//! * `SG_M9IC_DIFF=1`
//! * `M9IC_CORPUS` — the corpus JSON
//! * `M9IC_OUT` — output file

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};

use sg_graphql::exec::Actor;
use sg_graphql::schema::assemble_oss;
use sg_service_frontend::graphql_api::{
    GraphqlDeps, GraphqlExecRequest, GraphqlResponse, SchemaGraphqlDeps,
};
use sg_service_frontend::ranking_graphql::{
    CoverageCounts, DateTime, EmptyResponse, NoopObserver, Progress, RankingError, RankingService,
    RootResolver, SiteAdminChecker, Summary,
};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Default)]
struct ProgressDef {
    #[serde(default)]
    started_at: String,
    /// `null`/absent means the baseline's nil `*Progress`.
    #[serde(default)]
    completed_at: Option<String>,
    #[serde(default)]
    processed: i64,
    #[serde(default)]
    total: i64,
}

fn parse_time(value: &str) -> DateTime {
    DateTime::parse_rfc3339(value).unwrap_or_else(|| panic!("invalid corpus timestamp {value:?}"))
}

/// `NextJobStartsAt`'s `ok=false` value is ignored but still needs a `time.Time`;
/// the corpus uses an empty string there.
fn parse_time_or_epoch(value: &str) -> DateTime {
    if value.is_empty() {
        DateTime::from_unix_seconds(0)
    } else {
        parse_time(value)
    }
}

impl ProgressDef {
    fn to_progress(&self) -> Progress {
        Progress {
            started_at: parse_time(&self.started_at),
            completed_at: self.completed_at.as_deref().map(parse_time),
            processed: self.processed,
            total: self.total,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct SummaryDef {
    #[serde(default)]
    graph_key: String,
    #[serde(default)]
    visible_to_zoekt: bool,
    #[serde(default)]
    path_mapper_progress: ProgressDef,
    #[serde(default)]
    reference_mapper_progress: ProgressDef,
    #[serde(default)]
    reducer_progress: Option<ProgressDef>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ValueOkDef {
    #[serde(default)]
    value: String,
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    error: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct CountsDef {
    #[serde(default)]
    num_target_indexes: i64,
    #[serde(default)]
    num_exported_indexes: i64,
    #[serde(default)]
    num_repositories_without_current_ranks: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ServiceDef {
    #[serde(default)]
    derivative: ValueOkDef,
    #[serde(default)]
    summaries: Vec<SummaryDef>,
    #[serde(default)]
    summaries_error: String,
    #[serde(default)]
    next_job_starts_at: ValueOkDef,
    #[serde(default)]
    coverage_counts: CountsDef,
    #[serde(default)]
    coverage_error: String,
    #[serde(default)]
    bump_error: String,
    #[serde(default)]
    delete_error: String,
}

#[derive(Debug, Deserialize)]
struct ExecCase {
    name: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    site_admin: bool,
    #[serde(default)]
    graph_key_root: String,
    #[serde(default)]
    service: ServiceDef,
    #[serde(default)]
    queries: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ExecCorpus {
    #[serde(default)]
    cases: Vec<ExecCase>,
}

// ---------------------------------------------------------------------------
// Fake injected traits
// ---------------------------------------------------------------------------

struct FakeService {
    service: ServiceDef,
}

#[async_trait::async_trait]
impl RankingService for FakeService {
    async fn derivative_graph_key(&self) -> Result<(String, bool), RankingError> {
        if !self.service.derivative.error.is_empty() {
            return Err(RankingError::Service(self.service.derivative.error.clone()));
        }
        Ok((
            self.service.derivative.value.clone(),
            self.service.derivative.ok,
        ))
    }

    async fn bump_derivative_graph_key(&self) -> Result<(), RankingError> {
        if !self.service.bump_error.is_empty() {
            return Err(RankingError::Service(self.service.bump_error.clone()));
        }
        Ok(())
    }

    async fn summaries(&self) -> Result<Vec<Summary>, RankingError> {
        if !self.service.summaries_error.is_empty() {
            return Err(RankingError::Service(self.service.summaries_error.clone()));
        }
        Ok(self
            .service
            .summaries
            .iter()
            .map(|summary| Summary {
                graph_key: summary.graph_key.clone(),
                visible_to_zoekt: summary.visible_to_zoekt,
                path_mapper_progress: summary.path_mapper_progress.to_progress(),
                reference_mapper_progress: summary.reference_mapper_progress.to_progress(),
                reducer_progress: summary
                    .reducer_progress
                    .as_ref()
                    .map(ProgressDef::to_progress),
            })
            .collect())
    }

    async fn next_job_starts_at(&self) -> Result<(DateTime, bool), RankingError> {
        if !self.service.next_job_starts_at.error.is_empty() {
            return Err(RankingError::Service(
                self.service.next_job_starts_at.error.clone(),
            ));
        }
        Ok((
            parse_time_or_epoch(&self.service.next_job_starts_at.value),
            self.service.next_job_starts_at.ok,
        ))
    }

    async fn coverage_counts(&self, _graph_key: &str) -> Result<CoverageCounts, RankingError> {
        if !self.service.coverage_error.is_empty() {
            return Err(RankingError::Service(self.service.coverage_error.clone()));
        }
        Ok(CoverageCounts {
            num_target_indexes: self.service.coverage_counts.num_target_indexes,
            num_exported_indexes: self.service.coverage_counts.num_exported_indexes,
            num_repositories_without_current_ranks: self
                .service
                .coverage_counts
                .num_repositories_without_current_ranks,
        })
    }

    async fn delete_ranking_progress(&self, _graph_key: &str) -> Result<(), RankingError> {
        if !self.service.delete_error.is_empty() {
            return Err(RankingError::Service(self.service.delete_error.clone()));
        }
        Ok(())
    }
}

struct FakeChecker {
    site_admin: bool,
}

#[async_trait::async_trait]
impl SiteAdminChecker for FakeChecker {
    async fn check_current_user_is_site_admin(&self) -> Result<(), RankingError> {
        if self.site_admin {
            Ok(())
        } else {
            Err(RankingError::MustBeSiteAdmin)
        }
    }
}

// `EmptyResponse` is imported for the trait surface documentation; the
// resolver returns it from the success paths exercised through GraphQL.
const _: fn() -> EmptyResponse = sg_service_frontend::ranking_graphql::empty_response;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn normalized_response(response: &GraphqlResponse) -> Value {
    let data = response
        .data
        .as_ref()
        .map(|data| serde_json::from_str::<Value>(data).expect("data json"))
        .unwrap_or(Value::Null);
    let errors = response
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

#[tokio::test]
async fn ranking_graphql_exec_harness() {
    if std::env::var("SG_M9IC_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IC_CORPUS").expect("M9IC_CORPUS");
    let out_path = std::env::var("M9IC_OUT").expect("M9IC_OUT");
    let corpus: ExecCorpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let base_schema = Arc::new(assemble_oss().expect("oss schema"));

    let mut cases = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let deps = if case.enabled {
            let resolver = Arc::new(RootResolver::new(
                Arc::new(FakeService {
                    service: case.service.clone(),
                }),
                Arc::new(FakeChecker {
                    site_admin: case.site_admin,
                }),
                Arc::new(NoopObserver),
                case.graph_key_root.clone(),
            ));
            SchemaGraphqlDeps::new(base_schema.clone())
                .with_ranking(resolver)
                .expect("ranking schema")
        } else {
            SchemaGraphqlDeps::new(base_schema.clone())
        };
        let actor = if case.site_admin {
            Actor::User { uid: 1 }
        } else {
            Actor::Anonymous
        };
        let mut responses = Vec::with_capacity(case.queries.len());
        for query in &case.queries {
            let variables = Map::new();
            let response = deps
                .exec(GraphqlExecRequest {
                    query,
                    operation_name: "",
                    variables: &variables,
                    actor,
                    anonymous_uid: "",
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
