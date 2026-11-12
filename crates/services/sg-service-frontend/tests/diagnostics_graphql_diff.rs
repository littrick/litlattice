//! M9.14c differential of the read-only diagnostic `Query` fields
//! (`statusMessages`, `repositoryStats`, `gitservers`).
//!
//! The Go oracle (`cmd/frontend/graphqlbackend/zz_m9_14c_graphql_oracle_test.go`,
//! `TestM9DiagnosticsGraphQLOracle`) drives the real resolvers against
//! `dbmocks.MockDB` and a fake gitserver client; this harness drives the
//! ported prefetcher against the same fixtures through the
//! [`DiagnosticsStore`] seam.
//!
//! `backgroundJobs` and `RepositoryStats.embedded` are not part of this
//! differential (the Go baseline needs a real Redis recorder reader and
//! basestore SQL respectively); they are covered by the crate's unit tests.
//!
//! Env:
//!
//! * `SG_M9_14C_DIAGNOSTICS_DIFF=1`
//! * `M9_14C_DIAGNOSTICS_CORPUS` — JSON `[{name, query, uid, internal, world}]`
//! * `M9_14C_DIAGNOSTICS_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use sg_db::repo_statistics::RepoStatistics;
use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::diagnostics_graphql::{
    prefetch_diagnostics, BackgroundJobData, DiagnosticsDeps, DiagnosticsStore, GitserverInfoData,
    RepositoryStatsData, StatusMessageInputs, SyncErrorInput, ZoektStats,
};
use sg_service_frontend::external_services::{
    ConnectionCheckData, ExternalServiceNodeData, SyncJobsData,
};
use sg_service_frontend::graphql_resolvers::{FrontendRootResolver, NestedPrefetch, NodeRoots};

const SDL: &str = "
schema { query: Query }

type Query {
  statusMessages: [StatusMessage!]!
  repositoryStats: RepositoryStats!
  gitservers: GitserverInstanceConnection!
}

union StatusMessage =
      GitUpdatesDisabled
    | NoRepositoriesDetected
    | CloningProgress
    | ExternalServiceSyncError
    | SyncError
    | IndexingProgress
    | GitserverDiskThresholdReached

type GitUpdatesDisabled { message: String! }
type NoRepositoriesDetected { message: String! }
type CloningProgress { message: String! }
type ExternalServiceSyncError { message: String! externalService: ExternalService }
type ExternalService { id: ID! kind: String! displayName: String! }
type SyncError { message: String! }
type IndexingProgress { notIndexed: Int! indexed: Int! }
type GitserverDiskThresholdReached { message: String! }

type RepositoryStats {
  gitDirBytes: BigInt!
  total: Int!
  cloned: Int!
  cloning: Int!
  notCloned: Int!
  failedFetch: Int!
  corrupted: Int!
}

type GitserverInstanceConnection {
  nodes: [GitserverInstance!]!
  totalCount: Int!
  pageInfo: PageInfo!
}

type GitserverInstance {
  id: ID!
  address: String!
  freeDiskSpaceBytes: BigInt!
  totalDiskSpaceBytes: BigInt!
}

type PageInfo { endCursor: String hasNextPage: Boolean! }

scalar BigInt
";

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    uid: i32,
    #[serde(default)]
    internal: bool,
    world: String,
}

#[derive(Serialize)]
struct Envelope {
    data: Value,
    errors: Vec<String>,
}

#[derive(Clone, Default)]
struct Fixture {
    sync_errors: Vec<SyncErrorInput>,
    stats: RepoStatistics,
    zoekt: Option<ZoektStats>,
    git_dir_bytes: i64,
    servers: Vec<GitserverInfoData>,
    dotcom: bool,
    disable_auto_updates: bool,
    /// `GetByID` results for `ExternalServiceSyncError.externalService`.
    external_services: HashMap<i64, Result<ExternalServiceNodeData, String>>,
}

fn external_service_node(id: i64) -> ExternalServiceNodeData {
    ExternalServiceNodeData {
        id,
        kind: "GITHUB".to_owned(),
        display_name: format!("github-{id}"),
        config: "{}".to_owned(),
        url: Ok("https://github.com".to_owned()),
        webhook_url: Ok(None),
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        updated_at: time::OffsetDateTime::UNIX_EPOCH,
        creator: None,
        last_updater: None,
        warning: None,
        last_sync_error: None,
        repo_count: 0,
        last_sync_at: None,
        next_sync_at: None,
        unrestricted: false,
        supports_repo_exclusion: true,
        has_connection_check: true,
        check_connection: ConnectionCheckData::Unknown,
        sync_jobs: SyncJobsData::default(),
    }
}

fn zoekt(total: i64, indexed: i32, not_indexed: i32) -> Option<ZoektStats> {
    Some(ZoektStats {
        total,
        indexed,
        not_indexed,
    })
}

fn fixture(world: &str) -> Fixture {
    match world {
        "empty" => Fixture {
            git_dir_bytes: 0,
            ..Fixture::default()
        },
        "healthy" => Fixture {
            stats: RepoStatistics {
                total: 5,
                ..RepoStatistics::default()
            },
            ..Fixture::default()
        },
        "sync" => Fixture {
            disable_auto_updates: true,
            sync_errors: vec![
                SyncErrorInput {
                    message: "boom".to_owned(),
                    service_id: 7,
                },
                SyncErrorInput {
                    message: "db down".to_owned(),
                    service_id: 8,
                },
            ],
            stats: RepoStatistics {
                total: 10,
                cloned: 5,
                failed_fetch: 1,
                not_cloned: 2,
                cloning: 1,
                ..RepoStatistics::default()
            },
            zoekt: zoekt(10, 6, 4),
            servers: vec![GitserverInfoData::with_percent_used("gs1", 10, 100, 95.5)],
            external_services: [
                (7, Ok(external_service_node(7))),
                (8, Err("database unavailable".to_owned())),
            ]
            .into_iter()
            .collect(),
            ..Fixture::default()
        },
        "disk" => Fixture {
            sync_errors: vec![SyncErrorInput {
                message: "x".to_owned(),
                service_id: 9,
            }],
            stats: RepoStatistics {
                total: 100,
                ..RepoStatistics::default()
            },
            zoekt: Some(ZoektStats::default()),
            servers: vec![
                GitserverInfoData::with_percent_used("gs1", 1, 10, 90.0),
                GitserverInfoData::with_percent_used("gs2", 9, 10, 42.0),
            ],
            ..Fixture::default()
        },
        "dotcom" => Fixture {
            dotcom: true,
            sync_errors: vec![SyncErrorInput {
                message: "x".to_owned(),
                service_id: 9,
            }],
            stats: RepoStatistics {
                total: 100,
                not_cloned: 2,
                ..RepoStatistics::default()
            },
            zoekt: Some(ZoektStats {
                total: 100,
                indexed: 6,
                not_indexed: 3,
            }),
            ..Fixture::default()
        },
        "stats" => Fixture {
            stats: RepoStatistics {
                total: 10,
                cloned: 5,
                cloning: 1,
                not_cloned: 2,
                failed_fetch: 3,
                corrupted: 4,
                ..RepoStatistics::default()
            },
            git_dir_bytes: 1_234_567_890_123,
            ..Fixture::default()
        },
        "gitservers" => Fixture {
            servers: vec![
                GitserverInfoData::with_percent_used("10.0.0.1:3178", 1000, 4000, 0.0),
                GitserverInfoData::with_percent_used("10.0.0.2:3178", 2000, 4000, 0.0),
            ],
            ..Fixture::default()
        },
        other => panic!("unknown diagnostics world {other}"),
    }
}

struct FakeDiagnostics {
    fixture: Fixture,
}

#[async_trait]
impl DiagnosticsStore for FakeDiagnostics {
    async fn status_message_inputs(
        &self,
        include_zoekt: bool,
    ) -> Result<StatusMessageInputs, String> {
        Ok(StatusMessageInputs {
            sync_errors: self.fixture.sync_errors.clone(),
            stats: self.fixture.stats.clone(),
            zoekt: if include_zoekt {
                self.fixture.zoekt
            } else {
                None
            },
        })
    }

    async fn repository_stats(&self) -> RepositoryStatsData {
        RepositoryStatsData {
            stats: Ok(self.fixture.stats.clone()),
            git_dir_bytes: Ok(self.fixture.git_dir_bytes),
            embedded: Ok(3),
            indexed: None,
        }
    }

    async fn background_jobs(
        &self,
        _after: &str,
        _recent_run_count: i32,
    ) -> Result<Vec<BackgroundJobData>, String> {
        Ok(Vec::new())
    }

    async fn gitservers(&self) -> Result<Vec<GitserverInfoData>, String> {
        Ok(self.fixture.servers.clone())
    }

    async fn external_service_node(
        &self,
        id: i64,
    ) -> Result<Option<ExternalServiceNodeData>, String> {
        match self.fixture.external_services.get(&id) {
            Some(Ok(data)) => Ok(Some(data.clone())),
            Some(Err(error)) => Err(error.clone()),
            None => Ok(None),
        }
    }
}

fn canonical_response(response: sg_graphql::exec::ExecResponse) -> (Value, Vec<String>) {
    let value: Value = serde_json::from_str(&response.to_json()).unwrap_or(Value::Null);
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let errors = value
        .get("errors")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    item.get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                })
                .collect()
        })
        .unwrap_or_default();
    (data, errors)
}

#[tokio::test]
async fn diagnostics_graphql_diff_harness() {
    if std::env::var("SG_M9_14C_DIAGNOSTICS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_14C_DIAGNOSTICS_CORPUS").expect("corpus env");
    let out = std::env::var("M9_14C_DIAGNOSTICS_OUT").expect("out env");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");

    for case in cases {
        let actor = if case.internal {
            Actor::Internal
        } else if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let site_admin = case.internal || case.uid == 1;
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &serde_json::Map::new(),
            actor,
        };
        let fields = root_fields(&schema, &request);
        let fixture = fixture(&case.world);
        let store = Arc::new(FakeDiagnostics {
            fixture: fixture.clone(),
        });
        let deps = DiagnosticsDeps {
            store,
            dotcom: fixture.dotcom,
            disable_auto_git_updates: fixture.disable_auto_updates,
            disk_usage_threshold: Some(90),
            live: None,
        };
        let diagnostics = Arc::new(prefetch_diagnostics(&deps, &fields, actor, site_admin).await);

        let mut nested = NestedPrefetch::default();
        nested.diagnostics = Some(diagnostics);
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            site_admin,
        )
        .with_nested(Arc::new(nested))
        .with_nodes(NodeRoots::default());

        let response = execute_with(&schema, request, Some(&roots));
        let (data, errors) = canonical_response(response);
        let envelope = Envelope { data, errors };
        let body = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
