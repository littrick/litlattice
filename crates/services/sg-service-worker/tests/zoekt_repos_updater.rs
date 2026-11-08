#![forbid(unsafe_code)]

//! M9.8x differential harness (Rust side): the `zoekt-repos-updater` job.
//!
//! Mirrors `cmd/worker/internal/zoektrepos/zoektrepos_harness_test.go`.
//! `test_ws/p9-go/run_zoekt_repos_diff.sh` applies the shared fixture
//! immediately before each side and diffs the canonical lines:
//!
//! * `jjob`/`jconfig`: the job's description and config slice;
//! * `jname`/`jdesc`/`jinterval`: the routine contract;
//! * `scenario`: the outcome of running the handler's two calls over scripted
//!   endpoints;
//! * `row`: the `zoekt_repos` rows 9700-9799 afterwards.
//!
//! The production handler's endpoint source is scripted through
//! [`EndpointSource`]; the aggregation itself (later endpoints overwriting
//! earlier ones, rollout errors skipped, other errors aborting) is the real
//! `list_all_indexed` / `aggregate`.

use std::sync::Arc;

use async_trait::async_trait;
use sg_service_worker::framework::periodic::Handler;
use sg_service_worker::jobs::zoektrepos::{EndpointSource, ZoektReposHandler, ZoektReposUpdater};
use sg_service_worker::jobs::{Job, JobDeps};
use sg_zoekt::client::Error;
use sg_zoekt::types::{
    ListResponse, MinimalRepoListEntry as ZoektEntry, RepositoryBranch as ZoektBranch,
};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn zoekt_repos_updater_matches_go_harness() {
    if std::env::var("SG_ZOEKT_REPOS_HARNESS_ON").ok().as_deref() != Some("1") {
        eprintln!("SG_ZOEKT_REPOS_HARNESS_ON is not set");
        return;
    }

    let dsn = std::env::var("SG_ZOEKT_REPOS_DSN").expect("SG_ZOEKT_REPOS_DSN is not set");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    // 1. Job metadata.
    let job = ZoektReposUpdater;
    println!("jjob description={:?}", job.description());
    println!("jconfig count={}", job.configs().len());

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    println!("jname name={}", routines[0].name());
    println!("jdesc description={:?}", routines[0].description());
    println!("jinterval secs={}", routines[0].interval().as_secs());

    // 2. Scenarios. The fixture rows live at repo_id 9700-9799.
    let indexed = || {
        Script::Map(vec![
            (9700, "main", "abc", 1_700_000_000),
            (9701, "main", "new", 1_700_000_001),
            (9702, "main", "v2", 1_682_899_200),
            (9703, "", "", 0),
            (9705, "main", "b", 0),
        ])
    };

    run(&pool, "indexed", FakeSource::single(indexed())).await;
    run(&pool, "same", FakeSource::single(indexed())).await;
    run(
        &pool,
        "empty",
        FakeSource::new(vec!["a"], vec![Script::Map(Vec::new())]),
    )
    .await;
    run(
        &pool,
        "rollout",
        FakeSource::new(
            vec!["a", "b"],
            vec![
                Script::Map(vec![(9706, "dev", "zzz", 1_710_000_000)]),
                Script::Rollout,
            ],
        ),
    )
    .await;
    run(
        &pool,
        "error",
        FakeSource::new(
            vec!["a", "b"],
            vec![
                Script::Map(vec![(9704, "main", "would-change", 1_720_000_000)]),
                Script::Fail,
            ],
        ),
    )
    .await;
}

async fn run(pool: &PgPool, name: &str, source: FakeSource) {
    let handler = ZoektReposHandler::with_source(pool.clone(), Arc::new(source));
    let result = handler.handle(CancellationToken::new()).await;
    let outcome = match &result {
        Ok(()) => "ok".to_owned(),
        Err(error) => error.message().to_owned(),
    };
    println!("scenario name={name} err={outcome}");
    print_rows(pool).await;
}

/// One endpoint's scripted answer.
enum Script {
    /// `ReposMap` entries: `(repo_id, branch, version, index_time_unix)`.
    Map(Vec<(u32, &'static str, &'static str, i64)>),
    /// `*net.OpError` dial refusal in the baseline: skipped as a rollout.
    Rollout,
    /// A non-rollout failure: aborts the reconciliation.
    Fail,
}

struct FakeSource {
    endpoints: Vec<&'static str>,
    scripts: Vec<Script>,
}

impl FakeSource {
    fn single(script: Script) -> Self {
        Self::new(vec!["a"], vec![script])
    }

    fn new(endpoints: Vec<&'static str>, scripts: Vec<Script>) -> Self {
        Self { endpoints, scripts }
    }
}

#[async_trait]
impl EndpointSource for FakeSource {
    async fn endpoints(&self) -> Result<Vec<String>, String> {
        Ok(self
            .endpoints
            .iter()
            .map(|endpoint| endpoint.to_string())
            .collect())
    }

    async fn list(&self, endpoint: &str, _ctx: &CancellationToken) -> Result<ListResponse, Error> {
        let index = self
            .endpoints
            .iter()
            .position(|candidate| *candidate == endpoint)
            .expect("scripted endpoint");
        match &self.scripts[index] {
            Script::Map(entries) => Ok(ListResponse {
                repos: Vec::new(),
                repos_map: entries
                    .iter()
                    .map(|(id, branch, version, index_time_unix)| {
                        (
                            *id,
                            ZoektEntry {
                                has_symbols: true,
                                branches: if branch.is_empty() {
                                    Vec::new()
                                } else {
                                    vec![ZoektBranch {
                                        name: (*branch).to_owned(),
                                        version: (*version).to_owned(),
                                    }]
                                },
                                index_time_unix: *index_time_unix,
                            },
                        )
                    })
                    .collect(),
                crashes: 0,
            }),
            Script::Rollout => Err(Error::Transport(
                "dial tcp 127.0.0.1:6070: connect: connection refused".to_owned(),
            )),
            Script::Fail => Err(Error::Status(Box::new(tonic::Status::internal(
                "endpoint failed",
            )))),
        }
    }
}

async fn print_rows(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT repo_id, index_status, branches::text AS branches, \
                COALESCE(extract(epoch FROM last_indexed_at)::bigint::text, 'NULL') AS last_indexed, \
                CASE WHEN updated_at = '2000-01-01T00:00:00Z'::timestamptz \
                    THEN 'same' ELSE 'changed' END AS updated \
         FROM zoekt_repos \
         WHERE repo_id BETWEEN 9700 AND 9799 \
         ORDER BY repo_id",
    )
    .fetch_all(pool)
    .await
    .expect("rows");

    for row in rows {
        println!(
            "row id={} status={} branches={} last={} updated={}",
            row.get::<i32, _>("repo_id"),
            row.get::<String, _>("index_status"),
            row.get::<String, _>("branches"),
            row.get::<String, _>("last_indexed"),
            row.get::<String, _>("updated"),
        );
    }
}
