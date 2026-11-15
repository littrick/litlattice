//! M9.45d differential harness: `CreateCodeMonitor`/`UpdateCodeMonitor` with
//! the production [`ProductionSnapshot`] against the Go baseline
//! (`cmd/frontend/internal/codemonitors/resolvers` + the real
//! `codemonitors.Snapshot`) on a shared scratch Postgres database.
//!
//! Go oracle: `cmd/frontend/graphqlbackend/zz_m945d_codemonitor_snapshot_oracle_test.go`
//! (UNTRACKED, gated by `SG_M945D_CM_SNAPSHOT_DIFF=1`).
//!
//! Env: `SG_M945D_CM_SNAPSHOT_DIFF=1`, `SG_QTEST_DSN`, `M945D_CORPUS`,
//! `M945D_OUT`.
//!
//! The scratch `repo` table is empty in both runs, so `Snapshot` plans the
//! query but resolves zero repos and returns an empty map — the same shape the
//! fake zero-repo runtime below produces. Plan/hook errors (multiple commit
//! branches, non-commit leaves, parse errors) are deterministic and differ per
//! query.

#![allow(clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Map;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{assemble, CODE_MONITORS_SCHEMA, MAIN_SCHEMA};
use sg_proto::gitserver as pb;
use sg_service_frontend::code_monitors_graphql::{
    CodeMonitorsResolver, NoopTestSender, ProductionSnapshot,
};
use sg_service_frontend::code_monitors_graphql_store::PgCodeMonitorsGraphqlStore;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::search_commit_job::{CommitUserDB, CommitUserEmail, GitserverClient};
use sg_service_frontend::search_job::{
    JobError, JobStreamer, PrivateRepoLister, ReposListOptions,
};
use sg_service_frontend::search_jobutil_repos::{RepoPage, RepoPageSource, ZoektRepoLister};
use sg_service_frontend::search_runtime::CommitRuntimeClientsOwned;
use sg_service_frontend::search_searcher_client::{SearcherError, SearcherErrorKind};
use sg_service_frontend::search_stream::{RepoPagerRuntimeClients, TreeRuntimeClients};
use sg_zoekt::indexed_repo_revs::MinimalRepo;
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListResponse, SearchOptions, SearchResult};

#[derive(Deserialize)]
struct Corpus {
    now: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
    query: String,
}

struct EmptyRepoLister;

impl PrivateRepoLister for EmptyRepoLister {
    fn list_minimal_repos(&self, _options: &ReposListOptions) -> Result<Vec<MinimalRepo>, JobError> {
        Ok(Vec::new())
    }
}

struct EmptyJobStreamer;

impl JobStreamer for EmptyJobStreamer {
    fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        _sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError> {
        Err(JobError("zoekt is not configured for this test".to_owned()))
    }
}

struct EmptyPages;

impl RepoPageSource for EmptyPages {
    fn pages(&self) -> Vec<RepoPage> {
        vec![RepoPage {
            repo_revs: Vec::new(),
            backends_missing: 0,
        }]
    }

    fn error(&self) -> Option<JobError> {
        None
    }
}

struct EmptyZoektLister;

impl ZoektRepoLister for EmptyZoektLister {
    fn list(&self) -> Result<ListResponse, JobError> {
        Ok(ListResponse::default())
    }
}

struct UnusedUserDB;

impl CommitUserDB for UnusedUserDB {
    fn get_user_id_by_username(&self, _username: &str) -> Result<Option<i32>, JobError> {
        Ok(None)
    }

    fn list_emails_by_user(&self, _user_id: i32) -> Result<Vec<CommitUserEmail>, JobError> {
        Ok(Vec::new())
    }
}

struct UnusedGitserver;

impl GitserverClient for UnusedGitserver {
    fn search(
        &self,
        _request: &pb::SearchRequest,
        _on_matches: &mut dyn FnMut(Vec<pb::CommitMatch>),
    ) -> Result<bool, SearcherError> {
        Ok(false)
    }

    fn resolve_revision(
        &self,
        repo: &str,
        rev: &str,
        _ensure_revision: bool,
    ) -> Result<String, SearcherError> {
        Err(SearcherError::new(
            SearcherErrorKind::RevisionNotFound,
            format!("revision not found: {repo}@{rev}"),
        ))
    }
}

fn zero_repo_runtime() -> TreeRuntimeClients {
    TreeRuntimeClients {
        db: Arc::new(EmptyRepoLister),
        zoekt: Arc::new(EmptyJobStreamer),
        max_repos: None,
        searcher: None,
        commit: Some(Arc::new(CommitRuntimeClientsOwned {
            db: Arc::new(UnusedUserDB),
            gitserver: Arc::new(UnusedGitserver),
            leaf: None,
            user_external_account_key_present: false,
        })),
        repo_pager: Some(Arc::new(RepoPagerRuntimeClients::new(
            Arc::new(EmptyPages),
            Arc::new(EmptyZoektLister),
        ))),
    }
}

const DUMP_SQL: &str = "
SELECT json_build_object(
  'monitors', (SELECT coalesce(json_agg(x ORDER BY x.id), '[]'::json) FROM (SELECT id, created_by, description, enabled, namespace_user_id, namespace_org_id FROM cm_monitors) x),
  'queries', (SELECT coalesce(json_agg(x ORDER BY x.id), '[]'::json) FROM (SELECT id, monitor, query FROM cm_queries) x),
  'emails', (SELECT coalesce(json_agg(x ORDER BY x.id), '[]'::json) FROM (SELECT id, monitor, enabled, priority::text AS priority, header, include_results FROM cm_emails) x),
  'webhooks', (SELECT coalesce(json_agg(x ORDER BY x.id), '[]'::json) FROM (SELECT id, monitor, url, enabled, include_results FROM cm_webhooks) x),
  'slack_webhooks', (SELECT coalesce(json_agg(x ORDER BY x.id), '[]'::json) FROM (SELECT id, monitor, url, enabled, include_results FROM cm_slack_webhooks) x),
  'recipients', (SELECT coalesce(json_agg(x ORDER BY x.id), '[]'::json) FROM (SELECT id, email, namespace_user_id, namespace_org_id FROM cm_recipients) x),
  'last_searched', (SELECT coalesce(json_agg(x ORDER BY x.monitor_id, x.repo_id), '[]'::json) FROM (SELECT monitor_id, repo_id, to_json(commit_oids) AS commit_oids FROM cm_last_searched) x)
)::text";

fn parse_time(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &Rfc3339).expect("rfc3339 time")
}

fn normalized(response: &sg_graphql::exec::ExecResponse, dump: serde_json::Value) -> String {
    let mut entries: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
    entries.insert("db", dump);
    if !response.errors.is_empty() {
        entries.insert(
            "errors",
            serde_json::Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        );
    }
    if let Some(data) = response.data_json() {
        entries.insert("data", serde_json::from_str(&data).unwrap());
    }
    serde_json::to_string(&entries).unwrap()
}

async fn dump(pool: &PgPool) -> serde_json::Value {
    let raw: String = sqlx::query_scalar(DUMP_SQL).fetch_one(pool).await.unwrap();
    serde_json::from_str(&raw).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn code_monitor_snapshot_harness() {
    if std::env::var("SG_M945D_CM_SNAPSHOT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let corpus_path = std::env::var("M945D_CORPUS").expect("M945D_CORPUS");
    let out = std::env::var("M945D_OUT").expect("M945D_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let schema = assemble(&[MAIN_SCHEMA, CODE_MONITORS_SCHEMA]).expect("schema");
    let now = parse_time(&corpus.now);

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect");

    let runtime = zero_repo_runtime();
    for case in &corpus.cases {
        let resolver = Arc::new(
            CodeMonitorsResolver::new(
                Arc::new(PgCodeMonitorsGraphqlStore::new(pool.clone())),
                Arc::new(ProductionSnapshot::new(runtime.clone())),
                Arc::new(NoopTestSender),
                true,
                false,
            )
            .with_clock(Arc::new(move || now)),
        );
        let actor = if case.viewer.is_empty() || case.viewer == "anonymous" {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let roots = FrontendRootResolver::with_user(None).with_code_monitors(Some(resolver));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let dump = dump(&pool).await;
        let body = normalized(&response, dump);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
