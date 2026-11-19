#![forbid(unsafe_code)]

//! `sourcegraph.zoekt.configuration.v1.ZoektConfigurationService` on the
//! frontend's internal gRPC listener (M9.70a).
//!
//! Ports the `searchIndexerServer` handlers behind
//! `cmd/frontend/internal/httpapi/search.go`:
//!
//! * `List` — the cloned repositories zoekt may index
//!   (`backend.Repos.ListIndexable`: `ReposListOptions{OnlyCloned: true}`);
//! * `SearchConfiguration` — per-repository `ZoektIndexOptions` built by
//!   [`sg_zoekt::index_options::get_index_options`] (branch rules, symbol
//!   engine map, large files, priorities) plus the site-config fingerprint;
//! * `DocumentRanks` — the ranking vectors; without a wired ranking service
//!   this returns an empty map (zoekt then indexes without offline ranks);
//! * `UpdateIndexStatus` — `zoekt_repos.UpdateIndexStatuses`.
//!
//! Horizontal indexer sharding (`search.Indexers()`) is not part of the
//! single-container deployment, so `List` returns every indexable repository
//! and the request's `indexed_ids` are ignored, matching the baseline when
//! horizontal indexing is disabled.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use sg_config::LiveConfig;
use sg_db::zoekt_repos::{MinimalRepoListEntry, RepositoryBranch};
use sg_zoekt::index_options::{self, RepoIndexOptions, SiteConfiguration};
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use tonic::{Request, Response, Status};

use crate::search_contexts::{repo_revs, RequestContext, SearchContextsDatabase};

use sg_zoekt::proto::sourcegraph::zoekt::configuration::v1 as pb;

/// The state of the site configuration at one point in time
/// (`internal/search/backend/config.go:17-23`).
#[derive(Debug, Clone, Copy, Default)]
struct ConfigFingerprint {
    generated_at: Option<OffsetDateTime>,
    identifier: u64,
}

impl ConfigFingerprint {
    fn new(site: &serde_json::Value, now: OffsetDateTime) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        site.to_string().hash(&mut hasher);
        Self {
            generated_at: Some(now),
            identifier: hasher.finish(),
        }
    }

    fn to_proto(self) -> pb::Fingerprint {
        let generated_at = self.generated_at.map(|at| prost_types::Timestamp {
            seconds: at.unix_timestamp(),
            nanos: at.nanosecond() as i32,
        });
        pb::Fingerprint {
            generated_at,
            identifier: self.identifier,
        }
    }
}

/// One `repo` row` read for `SearchConfiguration`.
struct RepoRow {
    name: String,
    stars: i32,
    fork: bool,
    archived: bool,
    private: bool,
}

/// The gRPC service implementation.
pub struct ZoektConfigurationService {
    pool: PgPool,
    live: Arc<LiveConfig>,
    gitserver: Arc<sg_git::GitserverClient>,
    search_contexts: Arc<dyn SearchContextsDatabase + Send + Sync>,
}

impl ZoektConfigurationService {
    pub fn new(
        pool: PgPool,
        live: Arc<LiveConfig>,
        gitserver_addrs: Vec<String>,
        search_contexts: Arc<dyn SearchContextsDatabase + Send + Sync>,
    ) -> Self {
        Self {
            pool,
            live,
            gitserver: Arc::new(sg_git::GitserverClient::new(gitserver_addrs)),
            search_contexts,
        }
    }

    /// `searchIndexerServer.List` (`search.go:323-352`) with horizontal
    /// indexing disabled.
    async fn list_indexable(&self) -> Result<Vec<i32>, Status> {
        let rows = sqlx::query(
            r#"
SELECT repo.id
FROM repo
JOIN gitserver_repos gr ON gr.repo_id = repo.id
WHERE repo.deleted_at IS NULL
  AND repo.blocked IS NULL
  AND gr.clone_status = 'cloned'
ORDER BY repo.id
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        rows.iter()
            .map(|row| row.try_get::<i32, _>("id").map_err(internal))
            .collect()
    }

    async fn load_repo_rows(
        &self,
        repo_ids: &[i32],
    ) -> Result<HashMap<i32, RepoRow>, Status> {
        if repo_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(
            r#"
SELECT id, name, COALESCE(stars, 0) AS stars, fork, archived, private
FROM repo
WHERE deleted_at IS NULL AND blocked IS NULL AND id = ANY($1)
"#,
        )
        .bind(repo_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        let mut repos = HashMap::with_capacity(rows.len());
        for row in &rows {
            repos.insert(
                row.try_get::<i32, _>("id").map_err(internal)?,
                RepoRow {
                    name: row.try_get("name").map_err(internal)?,
                    stars: row.try_get("stars").map_err(internal)?,
                    fork: row.try_get("fork").map_err(internal)?,
                    archived: row.try_get("archived").map_err(internal)?,
                    private: row.try_get("private").map_err(internal)?,
                },
            );
        }
        Ok(repos)
    }
}

fn internal(error: sqlx::Error) -> Status {
    Status::internal(error.to_string())
}

/// `repoRankFromConfig` (`search.go:30-47`).
fn repo_rank_from_config(site: &serde_json::Value, name: &str) -> f64 {
    let Some(scores) = site
        .get("experimentalFeatures")
        .and_then(|value| value.get("ranking"))
        .and_then(|value| value.get("repoScores"))
        .and_then(|value| value.as_object())
    else {
        return 0.0;
    };
    let score = |key: &str| scores.get(key).and_then(|value| value.as_f64()).unwrap_or(0.0);
    let mut total = 0.0;
    for (index, byte) in name.bytes().enumerate() {
        if byte == b'/' {
            total += score(&name[..index]);
        }
    }
    total += score(name);
    total
}

fn search_index_query_contexts(site: &serde_json::Value) -> bool {
    site.get("search.index.query.contexts")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

#[tonic::async_trait]
impl pb::zoekt_configuration_service_server::ZoektConfigurationService
    for ZoektConfigurationService
{
    async fn list(
        &self,
        _request: Request<pb::ListRequest>,
    ) -> Result<Response<pb::ListResponse>, Status> {
        Ok(Response::new(pb::ListResponse {
            repo_ids: self.list_indexable().await?,
        }))
    }

    async fn search_configuration(
        &self,
        request: Request<pb::SearchConfigurationRequest>,
    ) -> Result<Response<pb::SearchConfigurationResponse>, Status> {
        let request = request.into_inner();
        if request.repo_ids.is_empty() {
            return Err(Status::invalid_argument("at least one repoID required"));
        }
        let repo_ids = request.repo_ids;

        let site = self.live.current().site.clone();
        let raw = site.raw().clone();
        let site_config: SiteConfiguration = serde_json::from_value(raw.clone()).unwrap_or_default();
        let repos = Arc::new(self.load_repo_rows(&repo_ids).await?);

        let context_revisions = Arc::new(
            repo_revs(
                self.search_contexts.as_ref(),
                &RequestContext {
                    actor_uid: 0,
                    internal: true,
                    sourcegraph_dot_com_mode: false,
                },
                &repo_ids,
                search_index_query_contexts(&raw),
            )
            .map_err(|error| Status::internal(error.to_string()))?,
        );

        let gitserver = Arc::clone(&self.gitserver);
        let handle = tokio::runtime::Handle::current();
        let site_for_priority = raw.clone();
        let repos_for_options = Arc::clone(&repos);
        let revisions_for_options = Arc::clone(&context_revisions);

        let options = tokio::task::spawn_blocking(move || {
            let get_repo_index_options = |repo_id: i32| -> Result<RepoIndexOptions, String> {
                let repo = repos_for_options
                    .get(&repo_id)
                    .ok_or_else(|| format!("repo not found: {repo_id}"))?;
                let gitserver = Arc::clone(&gitserver);
                let handle = handle.clone();
                let name = repo.name.clone();
                Ok(RepoIndexOptions {
                    name: repo.name.clone(),
                    repo_id,
                    public: !repo.private,
                    priority: f64::from(repo.stars) + repo_rank_from_config(&site_for_priority, &repo.name),
                    document_ranks_version: String::new(),
                    fork: repo.fork,
                    archived: repo.archived,
                    get_version: Box::new(move |branch: &str| {
                        let result = handle.block_on(gitserver.resolve_revision(
                            &name,
                            branch,
                            false,
                        ));
                        match result {
                            Ok(commit) => Ok(commit),
                            // `GetIndexOptions` wants an empty rev for a
                            // missing rev or empty repo (`search.go:254-262`).
                            Err(error) if error.is_not_found() => Ok(String::new()),
                            Err(error) => Err(error.to_string()),
                        }
                    }),
                })
            };
            let get_search_context_revisions = |repo_id: i32| -> Result<Vec<String>, String> {
                Ok(revisions_for_options
                    .get(&repo_id)
                    .cloned()
                    .unwrap_or_default())
            };
            index_options::get_index_options(
                &site_config,
                get_repo_index_options,
                get_search_context_revisions,
                &repo_ids,
            )
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?;

        let fingerprint = ConfigFingerprint::new(&raw, OffsetDateTime::now_utc());
        Ok(Response::new(pb::SearchConfigurationResponse {
            fingerprint: Some(fingerprint.to_proto()),
            updated_options: options.iter().map(to_proto_options).collect(),
        }))
    }

    async fn document_ranks(
        &self,
        _request: Request<pb::DocumentRanksRequest>,
    ) -> Result<Response<pb::DocumentRanksResponse>, Status> {
        // No ranking service is wired into the single-container frontend; an
        // empty map is the baseline's "no offline ranks" state.
        Ok(Response::new(pb::DocumentRanksResponse {
            paths: HashMap::new(),
            mean_rank: 0.0,
        }))
    }

    async fn update_index_status(
        &self,
        request: Request<pb::UpdateIndexStatusRequest>,
    ) -> Result<Response<pb::UpdateIndexStatusResponse>, Status> {
        let request = request.into_inner();
        let mut indexed: BTreeMap<u32, MinimalRepoListEntry> = BTreeMap::new();
        for repo in request.repositories {
            indexed.insert(
                repo.repo_id,
                MinimalRepoListEntry {
                    has_symbols: false,
                    branches: repo
                        .branches
                        .into_iter()
                        .map(|branch| RepositoryBranch {
                            name: branch.name,
                            version: branch.version,
                        })
                        .collect(),
                    index_time_unix: repo.index_time_unix,
                },
            );
        }
        sg_db::zoekt_repos::update_index_statuses(&self.pool, &indexed)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(pb::UpdateIndexStatusResponse {}))
    }
}

fn to_proto_options(options: &index_options::ZoektIndexOptions) -> pb::ZoektIndexOptions {
    pb::ZoektIndexOptions {
        name: options.name.clone(),
        repo_id: options.repo_id,
        public: options.public,
        fork: options.fork,
        archived: options.archived,
        large_files: options.large_files.clone().unwrap_or_default(),
        symbols: options.symbols,
        branches: options
            .branches
            .iter()
            .map(|branch| pb::ZoektRepositoryBranch {
                name: branch.name.clone(),
                version: branch.version.clone(),
            })
            .collect(),
        priority: options.priority,
        document_ranks_version: options.document_ranks_version.clone(),
        error: options.error.clone(),
        language_map: options
            .language_map
            .as_ref()
            .map(|map| {
                map.iter()
                    .map(|(language, parser)| pb::LanguageMapping {
                        language: language.clone(),
                        ctags: i32::from(*parser),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        shard_concurrency: options.shard_concurrency,
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_rank_sums_directory_prefixes() {
        let site = serde_json::json!({
            "experimentalFeatures": {
                "ranking": { "repoScores": {
                    "github.com": 1.5,
                    "github.com/coreutils": 2.0,
                    "github.com/coreutils/coreutils": 4.0,
                    "other": 9.0
                }}
            }
        });
        assert_eq!(repo_rank_from_config(&site, "github.com/coreutils/coreutils"), 7.5);
        assert_eq!(repo_rank_from_config(&site, "example.com/a"), 0.0);
    }

    #[test]
    fn fingerprint_is_stable_per_configuration() {
        let site = serde_json::json!({"search.largeFiles": ["a"]});
        let at = OffsetDateTime::UNIX_EPOCH;
        let first = ConfigFingerprint::new(&site, at);
        let second = ConfigFingerprint::new(&site, at);
        assert_eq!(first.identifier, second.identifier);
        let other = ConfigFingerprint::new(&serde_json::json!({"search.largeFiles": []}), at);
        assert_ne!(first.identifier, other.identifier);
    }
}
