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
//! `TreeJobRunner::with_clients` consumes. Missing zoekt endpoints are
//! reported as [`RuntimeConfigError::MissingZoekt`]; `service.rs` then keeps
//! `TreeJobRunner::unconfigured()`, whose not-ported error is byte-for-byte
//! unchanged. Searcher and commit clients are optional: when their endpoints
//! are absent the corresponding tree leaves keep their existing not-ported
//! errors.
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
use sg_git::GitserverClient as GitClient;
use sg_proto::gitserver as pb;
use sg_zoekt::indexed_repo_revs::MinimalRepo;

use crate::search_commit_job::{searcher_error_from_git, CommitUserDB, CommitUserEmail};
use crate::search_env::{IndexedError, IndexedJobStreamer, SearcherGrpcConnectionCache};
use crate::search_job::{JobError, PrivateRepoLister, ReposListOptions};
use crate::search_searcher_client::{
    default_searcher_client, grpc_client_from_endpoints, run_to_completion, SearcherClient,
    SearcherClients, SearcherError, SearcherGitserver, SearcherLimiter, SymbolsClient,
    SymbolsParameters, SymbolsResponse, NOT_PORTED_MESSAGE,
};
use crate::search_stream::{RepoPagerRuntimeClients, TreeRuntimeClients};
use crate::symbols_client::default_symbols_client;

/// Failure to assemble the production runtime. The only mandatory endpoint is
/// zoekt; the caller falls back to `TreeJobRunner::unconfigured()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeConfigError {
    /// `ServiceConnections.Zoekts` is empty: indexed search cannot run.
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
}

impl CommitRuntimeClientsOwned {
    /// Borrowed view consumed by `search_job::RuntimeClients`.
    pub fn as_clients(&self) -> crate::search_commit_job::CommitRuntimeClients<'_> {
        crate::search_commit_job::CommitRuntimeClients {
            db: self.db.as_ref(),
            gitserver: self.gitserver.as_ref(),
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
    assemble_with_repo_pager(pool, connections, site, None)
}

/// [`assemble`] plus optional `RepoPagerJob` collaborators. The resolver is
/// not ported, so production passes `None`; a caller that can supply pages (a
/// test or a future resolver port) injects them here. `None` keeps the exact
/// `"RepoPagerJob execution is not ported (M9.8hw)"` fallback.
pub fn assemble_with_repo_pager(
    pool: &PgPool,
    connections: &ServiceConnections,
    site: &SiteConfiguration,
    repo_pager: Option<Arc<RepoPagerRuntimeClients>>,
) -> Result<TreeRuntimeClients, RuntimeConfigError> {
    if connections.zoekts.is_empty() {
        return Err(RuntimeConfigError::MissingZoekt);
    }

    let max_repos = max_repos_from_site(site);
    let zoekt = Arc::new(
        IndexedJobStreamer::from_connections(connections).map_err(|error| match error {
            IndexedError::MissingZoekt => RuntimeConfigError::MissingZoekt,
            IndexedError::Endpoint(message) => RuntimeConfigError::ZoektEndpoint(message),
        })?,
    );
    let db = Arc::new(PgPrivateRepoLister::new(pool.clone()));

    let gitserver = Arc::new(GitClient::new(connections.git_servers.clone()));

    let searcher = if connections.searchers.is_empty() {
        None
    } else {
        // The process-wide cache (`search.SearcherGRPCConnectionCache()`);
        // stored on the bundle so later job-tree paths (repos resolver, alert
        // observer, filter-file-contains) can share it like the baseline
        // `job.RuntimeClients`.
        let searcher_connection_cache = Arc::new(SearcherGrpcConnectionCache::new());
        let searcher_client: Arc<dyn SearcherClient + Send + Sync> =
            match grpc_client_from_endpoints(connections.searchers.clone()) {
                Some(client) => {
                    Arc::new(client.with_connection_cache(Arc::clone(&searcher_connection_cache)))
                }
                None => Arc::from(default_searcher_client(Vec::new())),
            };
        Some(Arc::new(SearcherRuntimeClients {
            client: searcher_client,
            limiter: Arc::new(NoopSearcherLimiter),
            gitserver: Arc::new(GitSearcherGitserver {
                client: Arc::clone(&gitserver),
            }),
            // Production symbols gRPC client when endpoints are configured;
            // otherwise the explicit not-ported error is unchanged. The client
            // creates its own cache instance (`symbolsConnectionCache`).
            symbols: match default_symbols_client(connections.symbols.clone()) {
                Some(client) => Arc::from(client),
                None => Arc::new(NotPortedSymbols),
            },
            searcher_connection_cache,
        }))
    };

    let commit = if connections.git_servers.is_empty() {
        None
    } else {
        Some(Arc::new(CommitRuntimeClientsOwned {
            db: Arc::new(PgCommitUserDB::new(pool.clone())),
            gitserver: Arc::new(GitCommitGitserver {
                client: Arc::clone(&gitserver),
            }),
        }))
    };

    Ok(TreeRuntimeClients {
        db,
        zoekt,
        max_repos,
        searcher,
        commit,
        // `repos.Resolver` is not ported; a caller that can supply pages sets
        // this bundle. `None` keeps the exact RepoPagerJob not-ported error.
        repo_pager,
    })
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
    async fn missing_zoekt_endpoints_keep_the_unconfigured_error() {
        let pool = PgPool::connect_lazy("postgres://localhost/none").expect("lazy pool");
        let error = match assemble(
            &pool,
            &ServiceConnections::default(),
            &SiteConfiguration::default(),
        ) {
            Ok(_) => panic!("zoekt is required"),
            Err(error) => error,
        };
        assert_eq!(error, RuntimeConfigError::MissingZoekt);

        // The fallback runner still produces the pre-M9.8im error byte-for-byte.
        let runner = TreeJobRunner::unconfigured();
        let mut tree = JobNode::noop();
        let mut events = Vec::new();
        let output = runner.run(&mut tree, &mut |event| events.push(event));
        assert!(events.is_empty());
        assert!(output.alert.is_none());
        assert_eq!(
            output.error.map(|error| error.message).as_deref(),
            Some("search execution is not ported yet")
        );
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
