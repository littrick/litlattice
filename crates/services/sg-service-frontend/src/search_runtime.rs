//! Production composition root for search execution (M9.8im).
//!
//! Mirrors the frontend's process-level assembly of `job.RuntimeClients`
//! (`internal/search/client/client.go`, `cmd/frontend/internal/search`) for
//! the ported job tree:
//!
//! * zoekt: the M9.8ir [`crate::search_env::IndexedJobStreamer`] — the
//!   `Indexed()` stack (`CachedSearcher(ttl) → MeteredSearcher("") →
//!   HorizontalSearcher` over the configured endpoints with the cached indexed
//!   dialer) driven through the M9.8ig blocking bridge;
//! * db: a Postgres [`PrivateRepoLister`] for `privateReposForActor`;
//! * searcher (`index:no`): the M9.8il [`default_searcher_client`] plus the
//!   gitserver revision resolver;
//! * commit/diff: the M9.8ia seams backed by `sg-git` and the user-email
//!   tables;
//! * `max_repos`: `limits.SearchLimits(conf).MaxRepos`.
//!
//! [`assemble`] returns a [`TreeRuntimeClients`] bundle that
//! `TreeJobRunner::with_clients` consumes. Every client is wired
//! unconditionally, mirroring the baseline's always-present
//! `endpoint.ConfBased` maps and `gitserver.DefaultClient`: an empty endpoint
//! list only surfaces at RPC time (e.g. `endpoint.Map(conf-based) is empty` for
//! searcher/symbols, empty results for zoekt), never as the not-ported
//! fallback. The only assembly error left is an unparsable zoekt endpoint.
//!
//! Tests and differential harnesses can construct the same bundle directly
//! ([`SearcherRuntimeClients`] / [`CommitRuntimeClientsOwned`] fields are
//! public), so no endpoint is dialed.
//!
//! See `docs/overviews/m9_8im_search_runtime_assembly_overview.md`.

use std::sync::Arc;

use sqlx::PgPool;
use tokio_stream::StreamExt;

use sg_config::{ServiceConnections, SiteConfiguration};
use sg_db::repos as db_repos;
use sg_git::GitserverClient as GitClient;
use sg_proto::gitserver as pb;
use sg_zoekt::indexed_repo_revs::MinimalRepo;
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListOptions, ListResponse, RepoListField};

use crate::search_commit_job::{searcher_error_from_git, CommitUserDB, CommitUserEmail};
use crate::search_contexts::RequestContext;
use crate::search_contexts_store::PgSearchContextsStore;
use crate::search_env::{IndexedError, IndexedJobStreamer, SearcherGrpcConnectionCache};
use crate::search_job::{JobError, JobStreamer, PrivateRepoLister, ReposListOptions};
use crate::search_repos::{
    GitserverError, Resolver, ResolverGitserver, ResolverRepoPageSource,
    ResolverSearcher, ResolverZoektLister,
};
use crate::search_searcher_client::{
    default_searcher_client, grpc_client_from_endpoints, run_to_completion, SearcherClient,
    SearcherClients, SearcherError, SearcherGitserver, SearcherLimiter, SymbolsClient,
    SymbolsParameters, SymbolsResponse, NOT_PORTED_MESSAGE,
};
use crate::search_stream::{RepoPagerRuntimeClients, TreeRuntimeClients};
use crate::symbols_client::GrpcSymbolsClient;

/// Failure to assemble the production runtime. Empty endpoint lists are not
/// failures (the baseline maps are always present); only an unparsable zoekt
/// endpoint prevents assembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeConfigError {
    /// Historical variant kept for API compatibility; empty zoekt endpoint
    /// lists now assemble successfully and yield empty indexed results.
    MissingZoekt,
    /// A zoekt endpoint could not be turned into a lazy client.
    ZoektEndpoint(String),
}

impl std::fmt::Display for RuntimeConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingZoekt => f.write_str("search: no zoekt endpoints configured"),
            Self::ZoektEndpoint(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RuntimeConfigError {}

/// Owned searcher collaborators (`searcher.SearcherURLs` +
/// `SearcherGRPCConnectionCache` + gitserver + symbols).
pub struct SearcherRuntimeClients {
    pub client: Arc<dyn SearcherClient + Send + Sync>,
    pub limiter: Arc<dyn SearcherLimiter + Send + Sync>,
    pub gitserver: Arc<dyn SearcherGitserver + Send + Sync>,
    pub symbols: Arc<dyn SymbolsClient + Send + Sync>,
    /// `job.RuntimeClients.SearcherGRPCConnectionCache`: the process-wide
    /// searcher connection cache shared by the searcher client (and, in the
    /// baseline, the repos resolver/alert/filter paths). The symbols client
    /// owns a separate `symbolsConnectionCache` instance.
    pub searcher_connection_cache: Arc<SearcherGrpcConnectionCache>,
}

impl SearcherRuntimeClients {
    /// Borrowed view consumed by `search_job::RuntimeClients`.
    pub fn as_clients(&self) -> SearcherClients<'_> {
        SearcherClients {
            client: self.client.as_ref(),
            limiter: self.limiter.as_ref(),
            gitserver: self.gitserver.as_ref(),
            symbols: self.symbols.as_ref(),
        }
    }
}

/// Owned commit-search collaborators (`db.Users`/`db.UserEmails` + gitserver).
pub struct CommitRuntimeClientsOwned {
    pub db: Arc<dyn CommitUserDB + Send + Sync>,
    pub gitserver: Arc<dyn crate::search_commit_job::GitserverClient + Send + Sync>,
    /// Owned leaf collaborators; borrowed into
    /// [`crate::search_commit_job::CommitRuntimeClients::leaf`].
    pub leaf: Option<crate::search_leaf_jobs::LeafJobClients>,
}

impl CommitRuntimeClientsOwned {
    /// Borrowed view consumed by `search_job::RuntimeClients`.
    pub fn as_clients(&self) -> crate::search_commit_job::CommitRuntimeClients<'_> {
        crate::search_commit_job::CommitRuntimeClients {
            db: self.db.as_ref(),
            gitserver: self.gitserver.as_ref(),
            leaf: self.leaf.as_ref(),
        }
    }
}

/// `limits.SearchLimits(conf).MaxRepos`, reduced to the `RuntimeClients`
/// convention: `None` means the baseline default `math.MaxInt32 >> 1`.
pub fn max_repos_from_site(site: &SiteConfiguration) -> Option<i64> {
    let raw = site.raw();
    // `If MaxRepos unset use deprecated value` (`limits.go:32-34`).
    let max_repos = raw
        .get("searchLimits")
        .and_then(|limits| limits.get("maxRepos"))
        .and_then(serde_json::Value::as_i64)
        .filter(|value| *value > 0)
        .or_else(|| {
            raw.get("maxReposToSearch")
                .and_then(serde_json::Value::as_i64)
                .filter(|value| *value > 0)
        });
    // `withDefault` leaves the default implicit; `RuntimeClients` models it as
    // `None`.
    max_repos
}

/// Assemble the production runtime from the frontend's static endpoints.
///
/// `pool` is the frontend primary Postgres pool. `connections` comes from
/// [`crate::service_connections::service_connections`] and `site` from the
/// live configuration (`conf.Get()`).
pub fn assemble(
    pool: &PgPool,
    connections: &ServiceConnections,
    site: &SiteConfiguration,
) -> Result<TreeRuntimeClients, RuntimeConfigError> {
    assemble_inner(pool, connections, site, None)
}

/// [`assemble`] plus an optional test override for the `RepoPagerJob`
/// collaborators. `Some` wins; `None` builds the production
/// [`ResolverRepoPageSource`] like [`assemble`] (M9.10a), so differential
/// harnesses can still substitute fakes.
pub fn assemble_with_repo_pager(
    pool: &PgPool,
    connections: &ServiceConnections,
    site: &SiteConfiguration,
    repo_pager: Option<Arc<RepoPagerRuntimeClients>>,
) -> Result<TreeRuntimeClients, RuntimeConfigError> {
    assemble_inner(pool, connections, site, repo_pager)
}

/// Build `job.RuntimeClients.Searcher`: `searcher.SearcherURLs()` and
/// `symbols.DefaultClient` are always-present `endpoint.ConfBased` maps, so an
/// empty configuration only surfaces at RPC time (`endpoint.Map(conf-based) is
/// empty`), never as the not-ported fallback.
///
/// The process-wide cache (`search.SearcherGRPCConnectionCache()`) is stored on
/// the bundle so later job-tree paths (repos resolver, alert observer,
/// filter-file-contains) can share it like the baseline `job.RuntimeClients`.
fn assemble_searcher_clients(
    connections: &ServiceConnections,
    gitserver: &Arc<GitClient>,
) -> Arc<SearcherRuntimeClients> {
    let searcher_connection_cache = Arc::new(SearcherGrpcConnectionCache::new());
    let searcher_client: Arc<dyn SearcherClient + Send + Sync> =
        match grpc_client_from_endpoints(connections.searchers.clone()) {
            Some(client) => {
                Arc::new(client.with_connection_cache(Arc::clone(&searcher_connection_cache)))
            }
            None => Arc::from(default_searcher_client(Vec::new())),
        };
    Arc::new(SearcherRuntimeClients {
        client: searcher_client,
        limiter: Arc::new(NoopSearcherLimiter),
        gitserver: Arc::new(GitSearcherGitserver {
            client: Arc::clone(gitserver),
        }),
        // `symbols.DefaultClient` is likewise always present. The client
        // creates its own cache instance (`symbolsConnectionCache`).
        symbols: Arc::new(GrpcSymbolsClient::new(connections.symbols.clone())),
        searcher_connection_cache,
    })
}

/// Build `job.RuntimeClients.Commit`: the baseline always wires `db.Users` /
/// `db.UserEmails` + `gitserver.DefaultClient`; a missing git server list
/// surfaces as an RPC error rather than a not-ported leaf.
fn assemble_commit_clients(pool: &PgPool, gitserver: &Arc<GitClient>) -> Arc<CommitRuntimeClientsOwned> {
    Arc::new(CommitRuntimeClientsOwned {
        db: Arc::new(PgCommitUserDB::new(pool.clone())),
        gitserver: Arc::new(GitCommitGitserver {
            client: Arc::clone(gitserver),
        }),
        // Ownership/contributor leaf seams are not wired in the production
        // assembly yet; the leaf jobs then follow the baseline's explicit
        // error/alert paths instead of fabricating results.
        leaf: None,
    })
}

fn assemble_inner(
    pool: &PgPool,
    connections: &ServiceConnections,
    site: &SiteConfiguration,
    explicit_repo_pager: Option<Arc<RepoPagerRuntimeClients>>,
) -> Result<TreeRuntimeClients, RuntimeConfigError> {
    let max_repos = max_repos_from_site(site);
    let zoekt = Arc::new(
        IndexedJobStreamer::from_connections(connections).map_err(|error| match error {
            IndexedError::MissingZoekt => RuntimeConfigError::MissingZoekt,
            IndexedError::Endpoint(message) => RuntimeConfigError::ZoektEndpoint(message),
        })?,
    );
    let db = Arc::new(PgPrivateRepoLister::new(pool.clone()));

    let gitserver = Arc::new(GitClient::new(connections.git_servers.clone()));
    let gitserver_addrs = gitserver_addresses(connections, site);
    let searcher = Some(assemble_searcher_clients(connections, &gitserver));
    let commit = Some(assemble_commit_clients(pool, &gitserver));

    // `repos.Resolver` (M9.10a). The production resolver shares the zoekt
    // streamer, gitserver client and (when configured) searcher client with
    // the rest of the job tree. `explicit_repo_pager` is the test override.
    let repo_pager = match explicit_repo_pager {
        Some(repo_pager) => Some(repo_pager),
        None => {
            let resolver_zoekt = Arc::new(IndexedResolverZoekt {
                streamer: Arc::clone(&zoekt),
            });
            let resolver = Arc::new(Resolver {
                db: Arc::new(PgRepoResolverDb {
                    pool: pool.clone(),
                    search_contexts: Arc::new(PgSearchContextsStore::new(pool.clone())),
                }),
                gitserver: Arc::new(GitResolverGitserver {
                    client: Arc::clone(&gitserver),
                    addresses: Arc::clone(&gitserver_addrs),
                }),
                zoekt: resolver_zoekt.clone(),
                searcher: searcher.as_ref().map(|searcher| {
                    Arc::new(SearcherResolverSearcher {
                        client: Arc::clone(&searcher.client),
                    }) as Arc<dyn ResolverSearcher>
                }),
                max_repos,
            });
            Some(Arc::new(RepoPagerRuntimeClients::new(
                Arc::new(ResolverRepoPageSource::new(resolver)),
                resolver_zoekt,
            )))
        }
    };

    Ok(TreeRuntimeClients {
        db,
        zoekt,
        max_repos,
        searcher,
        commit,
        repo_pager,
    })
}

/// Production collaborators for the exhaustive-search worker
/// (`cmd/worker/internal/search/job.go:76-79`): the real `repos.Resolver`
/// (M9.10a) plus the job-tree runtime.
#[derive(Clone)]
pub struct ExhaustiveRuntime {
    /// `job.RuntimeClients`, reduced to the tree executor's bundle.
    pub clients: TreeRuntimeClients,
    /// `repos.Resolver`, shared with the search-job `RepoResolver` seam.
    pub resolver: Arc<Resolver>,
}

/// Assemble the exhaustive-search execution runtime.
///
/// Unlike [`assemble`], this does not require zoekt endpoints: exhaustive
/// search always runs with `index:no` (`searcher.go:37-39`), so the job tree
/// only needs searcher/commit clients (optional, per configuration) and the
/// resolver. The repo pager is still injected (M9.10a) even though the
/// exhaustive runner assigns each leaf its single resolved repo directly.
pub fn assemble_exhaustive(
    pool: &PgPool,
    connections: &ServiceConnections,
    site: &SiteConfiguration,
) -> ExhaustiveRuntime {
    let max_repos = max_repos_from_site(site);
    let db = Arc::new(PgPrivateRepoLister::new(pool.clone()));
    let gitserver = Arc::new(GitClient::new(connections.git_servers.clone()));
    let gitserver_addrs = gitserver_addresses(connections, site);

    // See `assemble_inner`: the baseline always has searcher/symbols/commit
    // clients, so exhaustive search keeps the same shape.
    let searcher = Some(assemble_searcher_clients(connections, &gitserver));
    let commit = Some(assemble_commit_clients(pool, &gitserver));

    // Exhaustive queries never touch indexed zoekt (`index:no`); the
    // resolver's zoekt lister only backs `repohasfile` filters, which are not
    // reachable here.
    let resolver_zoekt = Arc::new(UnconfiguredResolverZoekt);
    let resolver = Arc::new(Resolver {
        db: Arc::new(PgRepoResolverDb {
            pool: pool.clone(),
            search_contexts: Arc::new(PgSearchContextsStore::new(pool.clone())),
        }),
        gitserver: Arc::new(GitResolverGitserver {
            client: Arc::clone(&gitserver),
            addresses: Arc::clone(&gitserver_addrs),
        }),
        zoekt: resolver_zoekt.clone(),
        searcher: searcher.as_ref().map(|searcher| {
            Arc::new(SearcherResolverSearcher {
                client: Arc::clone(&searcher.client),
            }) as Arc<dyn ResolverSearcher>
        }),
        max_repos,
    });
    let repo_pager = Arc::new(RepoPagerRuntimeClients::new(
        Arc::new(ResolverRepoPageSource::new(Arc::clone(&resolver))),
        resolver_zoekt,
    ));

    ExhaustiveRuntime {
        clients: TreeRuntimeClients {
            db,
            zoekt: Arc::new(UnconfiguredJobStreamer),
            max_repos,
            searcher,
            commit,
            repo_pager: Some(repo_pager),
        },
        resolver,
    }
}

// ---------------------------------------------------------------------------
// `repos.Resolver` production adapters (M9.10a)
// ---------------------------------------------------------------------------

/// `database.DB` reads used by the repository resolver.
struct PgRepoResolverDb {
    pool: PgPool,
    search_contexts: Arc<PgSearchContextsStore>,
}

impl crate::search_repos::RepoResolverDb for PgRepoResolverDb {
    fn resolve_search_context(
        &self,
        spec: &str,
        ctx: &RequestContext,
    ) -> Result<crate::search_contexts::SearchContext, JobError> {
        crate::search_contexts::resolve_search_context_spec(
            self.search_contexts.as_ref(),
            ctx,
            spec,
        )
        .map_err(|error| JobError(error.to_string()))
    }

    fn list_minimal_repos(
        &self,
        options: &db_repos::RepoListOptions,
    ) -> Result<Vec<db_repos::RepoListItem>, JobError> {
        let pool = self.pool.clone();
        let options = options.clone();
        run_to_completion(async move { db_repos::list_minimal_repos(&pool, &options).await })
            .map_err(|error| JobError(error.to_string()))
    }

    fn get_search_context_repository_revisions(
        &self,
        id: i64,
    ) -> Result<Vec<crate::search_contexts::SearchContextRepositoryRevisions>, JobError> {
        use crate::search_contexts::SearchContextsDatabase;
        self.search_contexts
            .get_search_context_repository_revisions(id)
            .map_err(|error| JobError(error.to_string()))
    }

    fn batch_get_repo_commit_changelist(
        &self,
        repos: &[db_repos::RepoChangelistIDs],
    ) -> Result<std::collections::BTreeMap<u32, std::collections::BTreeMap<i64, String>>, JobError>
    {
        let pool = self.pool.clone();
        let repos = repos.to_vec();
        run_to_completion(async move {
            db_repos::batch_get_repo_commit_changelist(&pool, &repos).await
        })
        .map(|map| {
            map.into_iter()
                .map(|(repo_id, revs)| (repo_id as u32, revs))
                .collect()
        })
        .map_err(|error| JobError(error.to_string()))
    }

    fn list_minimal_repos_by_ids(
        &self,
        ids: &[i32],
    ) -> Result<Vec<db_repos::RepoListItem>, JobError> {
        let pool = self.pool.clone();
        let ids = ids.to_vec();
        run_to_completion(async move { db_repos::minimal_repos_by_ids(&pool, &ids).await })
            .map_err(|error| JobError(error.to_string()))
    }
}

/// `gitserver.Client` methods used by the resolver.
struct GitResolverGitserver {
    client: Arc<GitClient>,
    /// `connection.GitserverAddresses`: shard selection for the RPCs `sg-git`
    /// does not expose (currently only `RevAtTime`).
    addresses: Arc<sg_git::addresses::GitserverAddresses>,
}

/// `connection.NewGitserverAddresses(conf.Get())` from the static endpoints and
/// the `experimentalFeatures.gitServerPinnedRepos` site pin map.
fn gitserver_addresses(
    connections: &ServiceConnections,
    site: &SiteConfiguration,
) -> Arc<sg_git::addresses::GitserverAddresses> {
    Arc::new(sg_git::addresses::GitserverAddresses {
        addresses: connections.git_servers.clone(),
        pinned_servers: site.git_server_pinned_repos(),
    })
}

fn gitserver_error(error: sg_git::Error) -> GitserverError {
    match error {
        sg_git::Error::RevisionNotFound { .. }
        | sg_git::Error::RepoNotFound { .. }
        | sg_git::Error::NotFound(_) => GitserverError::Missing,
        other => GitserverError::Fatal(other.to_string()),
    }
}

fn timestamp_of(value: time::OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}

impl ResolverGitserver for GitResolverGitserver {
    fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, GitserverError> {
        let client = Arc::clone(&self.client);
        let repo = repo.to_owned();
        let rev = rev.to_owned();
        run_to_completion(async move { client.resolve_revision(&repo, &rev, false).await })
            .map_err(gitserver_error)
    }

    fn list_refs(&self, repo: &str) -> Result<Vec<String>, JobError> {
        let client = Arc::clone(&self.client);
        let repo = repo.to_owned();
        run_to_completion(async move {
            client
                .list_refs(&repo, false, false, &[], None)
                .await
        })
        .map(|refs| refs.into_iter().map(|git_ref| git_ref.name).collect())
        .map_err(|error| JobError(error.to_string()))
    }

    fn commits_after(
        &self,
        repo: &str,
        rev: &str,
        after: time::OffsetDateTime,
    ) -> Result<usize, GitserverError> {
        let client = Arc::clone(&self.client);
        let repo = repo.to_owned();
        let rev = if rev.is_empty() { "HEAD" } else { rev }.to_owned();
        let options = sg_git::CommitsOptions {
            ranges: vec![rev],
            after: Some(timestamp_of(after)),
            max_commits: 2,
            ..sg_git::CommitsOptions::default()
        };
        run_to_completion(async move { client.commits(&repo, &options).await })
            .map(|commits| commits.len())
            .map_err(gitserver_error)
    }

    /// `gitserver.Client.RevAtTime` (`internal/gitserver/commands.go:631-655`).
    ///
    /// `sg-git` does not expose this RPC, so the shard is resolved through
    /// [`sg_git::addresses::GitserverAddresses`] (site pins + MD5 sharding) and
    /// the proto client is dialed directly. An empty `commit_sha` means
    /// `found=false`; `NotFound` is the baseline's revision-not-found payload,
    /// which `normalizeRepoRefs` treats as a missing rev.
    fn rev_at_time(
        &self,
        repo: &str,
        rev: &str,
        timestamp: time::OffsetDateTime,
    ) -> Result<Option<String>, GitserverError> {
        let Some(addr) = self.addresses.addr_for_repo(repo) else {
            return Err(GitserverError::Fatal(
                "no gitserver address configured".to_owned(),
            ));
        };
        let addr = addr.to_owned();
        let repo = repo.to_owned();
        let rev = rev.to_owned();
        let time = timestamp_of(timestamp);
        run_to_completion(async move {
            let mut client = pb::gitserver_service_client::GitserverServiceClient::connect(
                format!("http://{addr}"),
            )
            .await
            .map_err(|error| GitserverError::Fatal(error.to_string()))?;
            let response = client
                .rev_at_time(pb::RevAtTimeRequest {
                    repo_name: repo,
                    rev_spec: rev.into_bytes(),
                    time: Some(time),
                })
                .await
                .map_err(|status| match status.code() {
                    // `RevisionNotFound` travels as `codes.NotFound`.
                    tonic::Code::NotFound => GitserverError::Missing,
                    _ => GitserverError::Fatal(status.to_string()),
                })?;
            let commit_sha = response.into_inner().commit_sha;
            Ok((!commit_sha.is_empty()).then_some(commit_sha))
        })
    }
}

/// `zoekt.Streamer` for the resolver: `List` on the production indexed stack.
struct IndexedResolverZoekt {
    streamer: Arc<IndexedJobStreamer>,
}

impl ResolverZoektLister for IndexedResolverZoekt {
    fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> Result<ListResponse, JobError> {
        self.streamer
            .streamer()
            .list_blocking(query, opts)
            .map_err(|error| JobError(error.to_string()))
    }
}

/// `zoekt.Streamer.List` consumed by `RepoPagerJob`'s partitioning
/// (`PartitionRepos` lists `Const(true)` with the repos map).
impl crate::search_jobutil_repos::ZoektRepoLister for IndexedResolverZoekt {
    fn list(&self) -> Result<ListResponse, JobError> {
        ResolverZoektLister::list(
            self,
            &Query::Const(true),
            Some(ListOptions {
                field: RepoListField::ReposMap,
            }),
        )
    }
}

/// `searcher.Search` for `filterRepoHasFileContent`.
struct SearcherResolverSearcher {
    client: Arc<dyn SearcherClient + Send + Sync>,
}

impl ResolverSearcher for SearcherResolverSearcher {
    fn search(&self, request: &crate::search_searcher_client::protocol::Request) -> Result<bool, JobError> {
        let mut found = false;
        self.client
            .search(request, &mut |_| found = true)
            .map_err(|error| JobError(error.to_string()))?;
        Ok(found)
    }
}

/// Zoekt streamer placeholder for commit-only job trees. Code-monitor
/// searches only construct commit/diff leaves, so the streamer is never
/// invoked; an explicit error keeps the "not configured" behavior observable
/// instead of panicking.
struct UnconfiguredJobStreamer;

impl JobStreamer for UnconfiguredJobStreamer {
    fn stream_search(
        &self,
        _query: &sg_zoekt::query::Query,
        _options: &sg_zoekt::types::SearchOptions,
        _sender: &mut dyn FnMut(sg_zoekt::types::SearchResult),
    ) -> Result<(), JobError> {
        Err(JobError(
            "zoekt is not configured for this search".to_owned(),
        ))
    }
}

/// Production runtime for commit-only job trees
/// (`internal/codemonitors.search.go`). Unlike [`assemble`], it does not build
/// the indexed zoekt stack (code-monitor plans only use commit/diff leaves plus
/// the repo pager), but the searcher/symbols and commit clients are still
/// always present, matching `search.RuntimeClients()`.
pub fn assemble_commit_search(
    pool: &PgPool,
    connections: &ServiceConnections,
    site: &SiteConfiguration,
) -> TreeRuntimeClients {
    let db = Arc::new(PgPrivateRepoLister::new(pool.clone()));
    let gitserver = Arc::new(GitClient::new(connections.git_servers.clone()));
    let gitserver_addrs = gitserver_addresses(connections, site);
    let searcher = assemble_searcher_clients(connections, &gitserver);
    let commit = Some(assemble_commit_clients(pool, &gitserver));

    // Code-monitor plans use `repoPagerJob{skipPartitioning:true}` and only
    // commit leaves, so the resolver's zoekt lister is never called; the
    // searcher client is still wired for `repo:contains.*` filters that need
    // `index:no` content checks.
    let resolver_zoekt = Arc::new(UnconfiguredResolverZoekt);
    let resolver = Arc::new(Resolver {
        db: Arc::new(PgRepoResolverDb {
            pool: pool.clone(),
            search_contexts: Arc::new(PgSearchContextsStore::new(pool.clone())),
        }),
        gitserver: Arc::new(GitResolverGitserver {
            client: Arc::clone(&gitserver),
            addresses: Arc::clone(&gitserver_addrs),
        }),
        zoekt: resolver_zoekt.clone(),
        searcher: Some(Arc::new(SearcherResolverSearcher {
            client: Arc::clone(&searcher.client),
        }) as Arc<dyn ResolverSearcher>),
        max_repos: max_repos_from_site(site),
    });
    let repo_pager = Arc::new(RepoPagerRuntimeClients::new(
        Arc::new(ResolverRepoPageSource::new(resolver)),
        resolver_zoekt,
    ));

    TreeRuntimeClients {
        db,
        zoekt: Arc::new(UnconfiguredJobStreamer),
        max_repos: max_repos_from_site(site),
        searcher: Some(searcher),
        commit,
        repo_pager: Some(repo_pager),
    }
}

/// Zoekt placeholder for the commit-search resolver (see
/// [`assemble_commit_search`]).
struct UnconfiguredResolverZoekt;

impl ResolverZoektLister for UnconfiguredResolverZoekt {
    fn list(
        &self,
        _query: &Query,
        _opts: Option<ListOptions>,
    ) -> Result<ListResponse, JobError> {
        Err(JobError(
            "zoekt is not configured for this search".to_owned(),
        ))
    }
}

impl crate::search_jobutil_repos::ZoektRepoLister for UnconfiguredResolverZoekt {
    fn list(&self) -> Result<ListResponse, JobError> {
        Err(JobError(
            "zoekt is not configured for this search".to_owned(),
        ))
    }
}

/// `db.Repos().ListMinimalRepos` for `privateReposForActor`.
pub struct PgPrivateRepoLister {
    pool: PgPool,
}

impl PgPrivateRepoLister {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl PrivateRepoLister for PgPrivateRepoLister {
    fn list_minimal_repos(&self, options: &ReposListOptions) -> Result<Vec<MinimalRepo>, JobError> {
        let pool = self.pool.clone();
        let options = options.clone();
        run_to_completion(async move { list_minimal_repos(&pool, &options).await })
            .map_err(|error| JobError(error.to_string()))
    }

    /// `db.Repos().Count` (`internal/database/repos.go:322-335`) for
    /// `computeExcludedRepos`. Applies the same visibility/fork/archive and
    /// pattern filters as [`list_minimal_repos`], but no limit/order.
    fn count_repos(&self, options: &ReposListOptions) -> Result<i64, JobError> {
        let pool = self.pool.clone();
        let options = options.clone();
        run_to_completion(async move { count_repos(&pool, &options).await })
            .map_err(|error| JobError(error.to_string()))
    }

    /// `searchcontexts.ResolveSearchContextSpec(ctx, ...)` for
    /// `computeExcludedRepos`: the request actor's permission predicate is
    /// applied by the shared Postgres store.
    fn resolve_search_context(
        &self,
        spec: &str,
        ctx: &crate::search_contexts::RequestContext,
    ) -> Result<crate::search_contexts::SearchContext, JobError> {
        let store = PgSearchContextsStore::new(self.pool.clone());
        crate::search_contexts::resolve_search_context_spec(&store, ctx, spec)
            .map_err(|error| JobError(error.to_string()))
    }

    /// `needsRepositoryConfiguration`
    /// (`internal/search/alert/observer.go:302-317`): the number of external
    /// services whose kind is a code host
    /// (`database.ExternalServiceKinds`, `external_services.go:205-224`).
    fn count_code_host_external_services(&self) -> Result<i64, JobError> {
        let pool = self.pool.clone();
        run_to_completion(async move { count_code_host_external_services(&pool).await })
            .map_err(|error| JobError(error.to_string()))
    }
}

/// `extsvc.Kind*` strings with `CodeHost: true`
/// (`internal/database/external_services.go:205-224`).
const CODE_HOST_EXTERNAL_SERVICE_KINDS: &[&str] = &[
    "AWSCODECOMMIT",
    "AZUREDEVOPS",
    "BITBUCKETCLOUD",
    "BITBUCKETSERVER",
    "GERRIT",
    "GITHUB",
    "GITLAB",
    "GITOLITE",
    "GOPACKAGES",
    "JVMPACKAGES",
    "NPMPACKAGES",
    "OTHER",
    "PAGURE",
    "PERFORCE",
    "PHABRICATOR",
    "PYTHONPACKAGES",
    "RUSTPACKAGES",
    "RUBYPACKAGES",
];

async fn count_code_host_external_services(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let kinds: Vec<String> = CODE_HOST_EXTERNAL_SERVICE_KINDS
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect();
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM external_services WHERE deleted_at IS NULL AND kind = ANY($1)",
    )
    .bind(&kinds)
    .fetch_one(pool)
    .await
}

async fn count_repos(pool: &PgPool, options: &ReposListOptions) -> Result<i64, sqlx::Error> {
    let mut sql = String::from("SELECT count(*) FROM repo");
    if options.search_context_id != 0 {
        // `repos.go:1198-1202`: join the context's repo list when set.
        sql.push_str(
            " JOIN (SELECT DISTINCT repo_id, search_context_id FROM search_context_repos) dscr \
             ON repo.id = dscr.repo_id",
        );
    }
    sql.push_str(" WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL");
    let mut arguments: Vec<String> = Vec::new();

    if options.only_private {
        sql.push_str(" AND repo.private");
    }
    if options.no_private {
        sql.push_str(" AND NOT repo.private");
    }
    if options.no_forks {
        sql.push_str(" AND NOT repo.fork");
    }
    if options.only_forks {
        sql.push_str(" AND repo.fork");
    }
    if options.no_archived {
        sql.push_str(" AND NOT repo.archived");
    }
    if options.only_archived {
        sql.push_str(" AND repo.archived");
    }
    for pattern in &options.include_patterns {
        arguments.push(pattern.clone());
        sql.push_str(&format!(" AND repo.name ~* ${}", arguments.len()));
    }
    if !options.exclude_pattern.is_empty() {
        arguments.push(options.exclude_pattern.clone());
        sql.push_str(&format!(" AND lower(repo.name) !~* ${}", arguments.len()));
    }
    if options.search_context_id != 0 {
        sql.push_str(&format!(
            " AND dscr.search_context_id = {}",
            options.search_context_id
        ));
    }
    sql.push_str(" AND ");
    sql.push_str(AUTHZ_CONDITION);

    let mut query = sqlx::query_scalar::<_, i64>(&sql);
    for argument in &arguments {
        query = query.bind(argument);
    }
    query.fetch_one(pool).await
}

/// Unrestricted/restricted repo visibility for an anonymous actor
/// (`AuthzQueryConds` with `BypassAuthz=false`, `AuthenticatedUserID=0`).
const AUTHZ_CONDITION: &str = r#"(
    EXISTS (SELECT 1 FROM user_repo_permissions WHERE repo_id = repo.id AND user_id IS NULL)
    OR NOT repo.private
    OR EXISTS (
        SELECT 1 FROM external_services es
        JOIN external_service_repos esr ON (
            esr.external_service_id = es.id AND esr.repo_id = repo.id
            AND es.unrestricted = TRUE AND es.deleted_at IS NULL
        )
    )
    OR EXISTS (
        SELECT 1 FROM user_repo_permissions WHERE repo_id = repo.id AND user_id = 0
    )
)"#;

async fn list_minimal_repos(
    pool: &PgPool,
    options: &ReposListOptions,
) -> Result<Vec<MinimalRepo>, sqlx::Error> {
    let mut sql = String::from(
        "SELECT repo.id, repo.name FROM repo \
         WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL",
    );
    let mut arguments: Vec<String> = Vec::new();

    if options.only_private {
        sql.push_str(" AND repo.private");
    }
    if options.no_forks {
        sql.push_str(" AND NOT repo.fork");
    }
    if options.only_forks {
        sql.push_str(" AND repo.fork");
    }
    if options.no_archived {
        sql.push_str(" AND NOT repo.archived");
    }
    if options.only_archived {
        sql.push_str(" AND repo.archived");
    }
    if !options.exclude_pattern.is_empty() {
        arguments.push(options.exclude_pattern.clone());
        sql.push_str(&format!(" AND lower(repo.name) !~* ${}", arguments.len()));
    }

    sql.push_str(" AND ");
    sql.push_str(AUTHZ_CONDITION);
    sql.push_str(" ORDER BY repo.id");
    if options.limit.is_some() {
        sql.push_str(&format!(" LIMIT ${}", arguments.len() + 1));
    }

    let mut query = sqlx::query(&sql);
    for argument in &arguments {
        query = query.bind(argument);
    }
    if let Some(limit) = options.limit {
        query = query.bind(limit);
    }

    let rows = query.fetch_all(pool).await?;
    let mut repos = Vec::with_capacity(rows.len());
    for row in rows {
        use sqlx::Row;
        let id: i32 = row.try_get("id")?;
        let name: String = row.try_get("name")?;
        repos.push(MinimalRepo {
            id: id as u32,
            name,
        });
    }
    Ok(repos)
}

/// `db.Users().GetByUsername`/`db.UserEmails().ListByUser`.
pub struct PgCommitUserDB {
    pool: PgPool,
}

impl PgCommitUserDB {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl CommitUserDB for PgCommitUserDB {
    fn get_user_id_by_username(&self, username: &str) -> Result<Option<i32>, JobError> {
        let pool = self.pool.clone();
        let username = username.to_owned();
        run_to_completion(async move {
            sg_db::users::find_user_by_username(&pool, &username)
                .await
                .map(|user| user.map(|user| user.id))
        })
        .map_err(|error| JobError(error.to_string()))
    }

    fn list_emails_by_user(&self, user_id: i32) -> Result<Vec<CommitUserEmail>, JobError> {
        let pool = self.pool.clone();
        run_to_completion(async move {
            sg_db::user_emails::list_by_user(&pool, user_id, false)
                .await
                .map(|emails| {
                    emails
                        .into_iter()
                        .map(|email| CommitUserEmail {
                            email: email.email,
                            verified: email.verified_at.is_some(),
                        })
                        .collect()
                })
        })
        .map_err(|error| JobError(error.to_string()))
    }
}

/// `gitserver.Client.Search` for the commit/diff job.
pub struct GitCommitGitserver {
    client: Arc<GitClient>,
}

impl crate::search_commit_job::GitserverClient for GitCommitGitserver {
    fn search(
        &self,
        request: &pb::SearchRequest,
        on_matches: &mut dyn FnMut(Vec<pb::CommitMatch>),
    ) -> Result<bool, SearcherError> {
        let client = Arc::clone(&self.client);
        let repo = request.repo.clone();
        let request = request.clone();
        // The stream callback is not `Send`, so the blocking bridge buffers
        // the single-match batches and flushes them after the future
        // completes. `do_search` sends its stats event only after `search`
        // returns, so the event order is unchanged.
        let (batches, outcome): (Vec<Vec<pb::CommitMatch>>, Result<bool, SearcherError>) =
            run_to_completion(async move {
                let mut batches = Vec::new();
                let mut stream = match client.search(&repo, request).await {
                    Ok(stream) => stream,
                    Err(error) => return (batches, Err(searcher_error_from_git(error))),
                };
                let mut limit_hit = false;
                while let Some(item) = stream.next().await {
                    let response = match item {
                        Ok(response) => response,
                        Err(error) => {
                            return (batches, Err(searcher_error_from_git(error)));
                        }
                    };
                    match response.message {
                        // Baseline `client.Search`: each `Match` is a
                        // single-item batch; `LimitHit` is OR-ed.
                        Some(pb::search_response::Message::Match(matched)) => {
                            batches.push(vec![matched]);
                        }
                        Some(pb::search_response::Message::LimitHit(hit)) => {
                            limit_hit = limit_hit || hit;
                        }
                        None => {
                            return (
                                batches,
                                Err(SearcherError::other("unknown SearchResponse message <nil>")),
                            );
                        }
                    }
                }
                (batches, Ok(limit_hit))
            });

        for batch in batches {
            on_matches(batch);
        }
        outcome
    }

    /// `gitserver.Client.ResolveRevision(repo, rev, EnsureRevision)`.
    fn resolve_revision(
        &self,
        repo: &str,
        rev: &str,
        ensure_revision: bool,
    ) -> Result<String, SearcherError> {
        let client = Arc::clone(&self.client);
        let repo = repo.to_owned();
        let rev = rev.to_owned();
        run_to_completion(async move {
            client
                .resolve_revision(&repo, &rev, ensure_revision)
                .await
                .map_err(searcher_error_from_git)
        })
    }
}

/// `gitserver.Client.ResolveRevision(..., false)` / `GetDefaultBranch(repo, false)`.
pub struct GitSearcherGitserver {
    client: Arc<GitClient>,
}

impl SearcherGitserver for GitSearcherGitserver {
    fn resolve_revision(&self, repo: &str, revspec: &str) -> Result<String, SearcherError> {
        let client = Arc::clone(&self.client);
        let repo = repo.to_owned();
        let revspec = revspec.to_owned();
        run_to_completion(async move {
            client
                .resolve_revision(&repo, &revspec, false)
                .await
                .map_err(searcher_error_from_git)
        })
    }

    fn default_branch(&self, repo: &str) -> Result<String, SearcherError> {
        let client = Arc::clone(&self.client);
        let repo = repo.to_owned();
        run_to_completion(async move {
            client
                .default_branch(&repo)
                .await
                .map_err(searcher_error_from_git)
        })
    }
}

/// Production limiter placeholder.
///
/// The baseline's `MutableLimiter.Acquire` returns a release callback; the
/// ported trait has no `release`, and the ported searcher jobs run serially, so
/// there is nothing to bound. Tests can inject a fake through
/// [`SearcherRuntimeClients::limiter`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSearcherLimiter;

impl SearcherLimiter for NoopSearcherLimiter {
    fn set_limit(&self, _limit: usize) {}

    fn acquire(&self) -> Result<(), SearcherError> {
        Ok(())
    }
}

/// Fallback used when no symbols endpoints are configured; every call reports
/// the existing explicit not-ported error.
#[derive(Debug, Default, Clone, Copy)]
pub struct NotPortedSymbols;

impl SymbolsClient for NotPortedSymbols {
    fn search(&self, _params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        Err(SearcherError::other(NOT_PORTED_MESSAGE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_job::JobStreamer;
    use crate::search_jobutil_tree::JobNode;
    use crate::search_stream::{JobRunner, TreeJobRunner};
    use sg_zoekt::query::Query;
    use sg_zoekt::types::{SearchOptions, SearchResult};
    use std::sync::Mutex;

    fn site(raw: serde_json::Value) -> SiteConfiguration {
        SiteConfiguration::parse(&raw.to_string()).expect("site config")
    }

    #[test]
    fn max_repos_follows_the_baseline_precedence() {
        assert_eq!(max_repos_from_site(&SiteConfiguration::default()), None);
        assert_eq!(
            max_repos_from_site(&site(serde_json::json!({"maxReposToSearch": 42}))),
            Some(42)
        );
        assert_eq!(
            max_repos_from_site(&site(serde_json::json!({
                "searchLimits": {"maxRepos": 7},
                "maxReposToSearch": 42
            }))),
            Some(7)
        );
        // Non-positive values keep the default.
        assert_eq!(
            max_repos_from_site(&site(serde_json::json!({
                "searchLimits": {"maxRepos": 0},
                "maxReposToSearch": -1
            }))),
            None
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_endpoints_still_assemble_every_realtime_client() {
        let pool = PgPool::connect_lazy("postgres://localhost/none").expect("lazy pool");
        // The baseline `endpoint.ConfBased` maps are always present, so the
        // production runtime must be assembled even before service discovery
        // produced any address; the empty-map error is deferred to RPC time.
        let runtime = assemble(
            &pool,
            &ServiceConnections::default(),
            &SiteConfiguration::default(),
        )
        .expect("assemble never fails for empty endpoints");
        assert!(runtime.searcher.is_some(), "searcher client is always wired");
        assert!(runtime.commit.is_some(), "commit client is always wired");
        assert!(runtime.repo_pager.is_some(), "repo pager is always wired");

        // The tree executes against the produced bundle instead of the
        // not-ported fallback; an empty tree simply yields no events/errors.
        let runner = TreeJobRunner::with_clients(runtime);
        let mut tree = JobNode::noop();
        let mut events = Vec::new();
        let output = runner.run(&mut tree, &mut |event| events.push(event));
        assert!(events.is_empty());
        assert!(output.alert.is_none());
        assert!(output.error.is_none());
    }

    fn recording_lister() -> Arc<RecordingLister> {
        Arc::new(RecordingLister {
            options: Mutex::new(None),
        })
    }

    struct RecordingLister {
        options: Mutex<Option<ReposListOptions>>,
    }

    impl PrivateRepoLister for RecordingLister {
        fn list_minimal_repos(
            &self,
            options: &ReposListOptions,
        ) -> Result<Vec<MinimalRepo>, JobError> {
            *self.options.lock().expect("lock") = Some(options.clone());
            Ok(Vec::new())
        }
    }

    struct EmptyStreamer;

    impl JobStreamer for EmptyStreamer {
        fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            _sender: &mut dyn FnMut(SearchResult),
        ) -> Result<(), JobError> {
            Ok(())
        }
    }

    struct FakeSearcher {
        endpoints: Vec<String>,
    }

    impl SearcherClient for FakeSearcher {
        fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
            Ok(self.endpoints.clone())
        }

        fn search(
            &self,
            _request: &crate::search_searcher_client::protocol::Request,
            _on_match: &mut dyn FnMut(crate::search_searcher_client::protocol::FileMatch),
        ) -> Result<bool, SearcherError> {
            Ok(false)
        }
    }

    impl SearcherLimiter for FakeSearcher {
        fn set_limit(&self, _limit: usize) {}

        fn acquire(&self) -> Result<(), SearcherError> {
            Ok(())
        }
    }

    impl SearcherGitserver for FakeSearcher {
        fn resolve_revision(&self, _repo: &str, revspec: &str) -> Result<String, SearcherError> {
            Ok(revspec.to_owned())
        }

        fn default_branch(&self, repo: &str) -> Result<String, SearcherError> {
            Ok(format!("refs/heads/{repo}"))
        }
    }

    impl SymbolsClient for FakeSearcher {
        fn search(&self, _params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
            Ok(SymbolsResponse::default())
        }
    }

    #[test]
    fn clients_are_borrowed_into_the_tree_runtime() {
        // Build the same owned bundle the composition root produces, then run
        // a tree through `TreeJobRunner::with_clients`. The zoekt leaf reads
        // the injected db/zoekt, proving the split exists.
        let lister = recording_lister();
        let runtime = TreeRuntimeClients {
            db: lister.clone(),
            zoekt: Arc::new(EmptyStreamer),
            max_repos: Some(3),
            searcher: Some(Arc::new(SearcherRuntimeClients {
                client: Arc::new(FakeSearcher {
                    endpoints: vec!["http://searcher:1".to_owned()],
                }),
                limiter: Arc::new(NoopSearcherLimiter),
                gitserver: Arc::new(GitSearcherGitserver {
                    client: Arc::new(GitClient::new(Vec::new())),
                }),
                symbols: Arc::new(NotPortedSymbols),
                searcher_connection_cache: Arc::new(SearcherGrpcConnectionCache::new()),
            })),
            commit: None,
            repo_pager: None,
        };
        let runner = TreeJobRunner::with_clients(runtime);
        let mut tree = crate::search_jobutil_tree::JobNode::zoekt_global_text(
            sg_zoekt::global_query::GlobalZoektQuery::new(Query::Const(true), None, true),
            crate::search_stream::zoekt_search::ZoektParameters::default(),
            sg_zoekt::global_query::RepoOptions::default(),
            Vec::new(),
            crate::search_jobutil_tree::TreeRepoOptions::default(),
        );
        let mut events = Vec::new();
        let output = runner.run(&mut tree, &mut |event| events.push(event));
        assert!(
            output.error.is_none(),
            "unexpected error: {:?}",
            output.error
        );
        let options = lister
            .options
            .lock()
            .expect("lock")
            .clone()
            .expect("lister called");
        assert!(options.only_private);
        assert_eq!(options.limit, Some(4), "max_repos + 1");
    }
}
