//! M9.14c differential of the embeddings read surface.
//!
//! The Go oracle (`cmd/frontend/graphqlbackend/zz_m9_14c_graphql_oracle_test.go`,
//! `TestM9EmbeddingsGraphQLOracle`) drives the real resolvers against
//! `dbmocks.MockDB`; this harness drives the ported prefetchers against fake
//! stores with the same fixtures. Each side emits one canonical
//! `{data, errors}` JSON file per case.
//!
//! Env:
//!
//! * `SG_M9_14C_EMBEDDINGS_DIFF=1`
//! * `M9_14C_EMBEDDINGS_CORPUS` — JSON `[{name, query, uid, internal, world}]`
//! * `M9_14C_EMBEDDINGS_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::embeddings_graphql::{
    embeddings_enabled, prefetch_embeddings, RepositoryEmbeddingsStore,
};
use sg_db::repositories_connection::{
    RepoListOptions, RepoListOrder, RepoListRecord, RepoPagination,
};
use sg_service_frontend::graphql_resolvers::{
    prefetch_repositories, FrontendRootResolver, NestedPrefetch, NodeRoots, RepoLookup,
    RepositoryData, RepositoryLookup,
};
use sg_service_frontend::repositories::{
    prefetch_repositories_connections, RepositoriesConnectionStore,
};

const SDL: &str = "
schema { query: Query }

type Query {
  repository(name: String): Repository
  repositories(
    first: Int
    cloned: Boolean = true
    notCloned: Boolean = true
    indexed: Boolean = true
    notIndexed: Boolean = true
    embedded: Boolean = true
    notEmbedded: Boolean = true
    orderBy: RepositoryOrderBy = REPOSITORY_NAME
    descending: Boolean = false
  ): NewRepositoryConnection!
}

enum RepositoryOrderBy {
  REPOSITORY_NAME
  REPO_CREATED_AT
  REPOSITORY_CREATED_AT
  SIZE
}

type Repository {
  id: ID!
  name: String!
  embeddingExists: Boolean!
}

type NewRepositoryConnection {
  nodes: [Repository!]!
  totalCount: Int!
  pageInfo: ConnectionPageInfo!
}

type ConnectionPageInfo {
  startCursor: String
  endCursor: String
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
}
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

fn time(offset: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000 + offset).unwrap()
}

/// Fixture repositories: repo-1 embedded, repo-2 not embedded, repo-3
/// embedded whose lookup fails (`id != 2` is the Go oracle's predicate).
const REPOS: [(i32, &str); 3] = [(1, "repo-1"), (2, "repo-2"), (3, "repo-3")];

fn embedded(id: i32) -> bool {
    id != 2
}

fn repository_data(id: i32, name: &str) -> RepositoryData {
    RepositoryData {
        id,
        name: name.to_owned(),
        uri: "u".to_owned(),
        description: String::new(),
        fork: false,
        archived: false,
        private: false,
        stars: 0,
        external_id: String::new(),
        external_service_type: String::new(),
        external_service_id: String::new(),
    }
}

fn record(id: i32, name: &str) -> RepoListRecord {
    let data = repository_data(id, name);
    RepoListRecord {
        id: data.id,
        name: data.name,
        uri: data.uri,
        description: data.description,
        fork: data.fork,
        archived: data.archived,
        private: data.private,
        stars: data.stars,
        created_at: time(0),
        external_id: data.external_id,
        external_service_type: data.external_service_type,
        external_service_id: data.external_service_id,
        repo_size_bytes: 0,
    }
}

fn records() -> Vec<RepoListRecord> {
    REPOS
        .iter()
        .map(|(id, name)| record(*id, name))
        .collect()
}

struct FakeRepos;

#[async_trait]
impl RepositoryLookup for FakeRepos {
    async fn lookup(
        &self,
        name_or_uri: &str,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        Ok(REPOS
            .iter()
            .find(|(_, name)| *name == name_or_uri)
            .map(|(id, name)| RepoLookup::Found(repository_data(*id, name)))
            .unwrap_or(RepoLookup::NotFound))
    }

    async fn by_id(
        &self,
        repo_id: i32,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        Ok(REPOS
            .iter()
            .find(|(id, _)| *id == repo_id)
            .map(|(id, name)| RepoLookup::Found(repository_data(*id, name)))
            .unwrap_or(RepoLookup::NotFound))
    }

    async fn by_hashed_name(
        &self,
        _hashed_name: &str,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        Ok(RepoLookup::NotFound)
    }
}

struct FakeConnections;

impl FakeConnections {
    fn filter(options: &RepoListOptions) -> Vec<RepoListRecord> {
        let mut filtered: Vec<RepoListRecord> = records()
            .into_iter()
            .filter(|record| !(options.no_embedded && embedded(record.id)))
            .filter(|record| !(options.only_embedded && !embedded(record.id)))
            .collect();
        filtered.sort_by(|a, b| a.name.cmp(&b.name));
        filtered
    }
}

#[async_trait]
impl RepositoriesConnectionStore for FakeConnections {
    async fn list(
        &self,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
        options: &RepoListOptions,
        order: RepoListOrder,
        pagination: &RepoPagination,
    ) -> Result<Vec<RepoListRecord>, String> {
        let mut filtered = Self::filter(options);
        if !order.ascending {
            filtered.reverse();
        }
        if let Some(first) = pagination.first {
            let limit = usize::try_from(first).unwrap_or(0);
            filtered.truncate(limit);
        } else if let Some(last) = pagination.last {
            let limit = usize::try_from(last).unwrap_or(0);
            if filtered.len() > limit {
                filtered.drain(..filtered.len() - limit);
            }
        }
        Ok(filtered)
    }

    async fn count(
        &self,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
        options: &RepoListOptions,
    ) -> Result<i64, String> {
        Ok(i64::try_from(Self::filter(options).len()).unwrap_or(i64::MAX))
    }
}

struct FakeEmbeddings;

#[async_trait]
impl RepositoryEmbeddingsStore for FakeEmbeddings {
    async fn repo_embedding_exists(&self, repo_id: i32) -> Result<bool, String> {
        match repo_id {
            1 => Ok(true),
            2 => Ok(false),
            _ => Err("db down".to_owned()),
        }
    }

    async fn count_repo_embeddings(&self) -> Result<i64, String> {
        Ok(3)
    }
}

fn site_config(world: &str) -> Value {
    match world {
        "on" => serde_json::json!({
            "cody": {"enabled": true},
            "embeddings": {"provider": "openai", "accessToken": "x"}
        }),
        "off" => serde_json::json!({"cody": {"enabled": true}}),
        "off-cody" => serde_json::json!({
            "cody": {"enabled": false},
            "embeddings": {"provider": "openai", "accessToken": "x"}
        }),
        other => panic!("unknown embeddings world {other}"),
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
async fn embeddings_graphql_diff_harness() {
    if std::env::var("SG_M9_14C_EMBEDDINGS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_14C_EMBEDDINGS_CORPUS").expect("corpus env");
    let out = std::env::var("M9_14C_EMBEDDINGS_OUT").expect("out env");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let repo_store = FakeRepos;
    let connection_store = FakeConnections;
    let embeddings_store = FakeEmbeddings;

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

        let repositories = prefetch_repositories(&repo_store, None, &fields, actor, site_admin).await;
        let repositories_connections =
            prefetch_repositories_connections(&connection_store, &fields, actor, site_admin).await;

        let mut repo_ids: Vec<i32> = Vec::new();
        for lookup in repositories.values() {
            if let RepoLookup::Found(data) = lookup {
                repo_ids.push(data.id);
            }
        }
        for lookup in repositories_connections.values() {
            if let sg_service_frontend::repositories::RepositoriesConnectionLookup::Found(data) =
                lookup
            {
                if let Ok(page) = &data.page {
                    repo_ids.extend(page.nodes.iter().map(|node| node.id));
                }
            }
        }

        let enabled = embeddings_enabled(&site_config(&case.world), true);
        let embeddings =
            Arc::new(prefetch_embeddings(&embeddings_store, &fields, repo_ids, enabled).await);

        let mut nested = NestedPrefetch::default();
        nested.repo_embeddings = Some(embeddings);
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            repositories,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            site_admin,
        )
        .with_nested(Arc::new(nested))
        .with_nodes(NodeRoots::default())
        .with_repositories_connections(repositories_connections);

        let response = execute_with(&schema, request, Some(&roots));
        let (data, errors) = canonical_response(response);
        let envelope = Envelope { data, errors };
        let body = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
