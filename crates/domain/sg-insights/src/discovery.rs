#![forbid(unsafe_code)]

//! Port of `internal/insights/discovery`: the repository iterators used by the
//! code-insights backfill workers.
//!
//! The Go package exposes:
//!
//! * `RepoIterator` — `ForEach(ctx, func(repoName, id) error)`;
//! * `AllReposIterator` — paginates every repository through
//!   `RepoStore.List(LimitOffset{1000, offset})`, caching pages in memory for
//!   `RepositoryListCacheTime` (the clock is injectable so tests can expire the
//!   cache);
//! * `RepoIteratorFromQuery` — builds a repository scope query with
//!   [`repository_scope_query`] and runs it through a
//!   `query.RepoQueryExecutor`;
//! * `ScopedRepoIterator` — resolves a configured list of repository names
//!   through `RepoStore.List(Names)`;
//! * `SeriesRepoIterator::ForSeries` — dispatch: no repositories and no
//!   criteria → all repositories; criteria → query scope; otherwise the
//!   configured list.
//!
//! Differences from the baseline, both intentional:
//!
//! * the Go `actor.WithInternalActor` context wrapper has no Rust counterpart;
//!   the executor/store implementations must already run with internal
//!   privileges;
//! * the Prometheus counters (`success`/`error`) are not ported; they do not
//!   influence the iteration and the worker already records job metrics.
//!
//! The Go `RepoID` is an `int32`; this port uses `i64` so ids can flow through
//! the storage layer unchanged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_insights_query::repository_scope_query;
use time::{Duration, OffsetDateTime};

/// `types.MinimalRepo` (the executor's result type).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinimalRepo {
    pub id: i64,
    pub name: String,
}

/// Go `database.LimitOffset`; also the page cache key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LimitOffset {
    pub limit: i64,
    pub offset: i64,
}

/// The subset of Go `database.ReposListOptions` that discovery uses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReposListOptions {
    pub names: Option<Vec<String>>,
    pub limit_offset: Option<LimitOffset>,
}

/// `types.Repo`, reduced to the fields discovery reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoSummary {
    pub id: i64,
    pub name: String,
}

/// Subset of the API exposed by `database.Repos()`.
#[async_trait]
pub trait RepoStore: Send + Sync {
    async fn list(&self, opts: &ReposListOptions) -> Result<Vec<RepoSummary>, String>;
}

/// Consumer interface of `query.RepoQueryExecutor`.
#[async_trait]
pub trait RepoQueryExecutor: Send + Sync {
    async fn execute_repo_list(&self, query: &str) -> Result<Vec<MinimalRepo>, String>;
}

/// Go `func() time.Time`; injected so the page cache is testable.
///
/// Implemented for any `Fn() -> OffsetDateTime`.
pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

impl<F> Clock for F
where
    F: Fn() -> OffsetDateTime + Send + Sync,
{
    fn now(&self) -> OffsetDateTime {
        self()
    }
}

/// A repository iterator (`discovery.RepoIterator`).
#[derive(Clone)]
pub enum RepoIterator {
    All(Arc<AllReposIterator>),
    Query(Arc<RepoIteratorFromQuery>),
    Scoped(Arc<ScopedRepoIterator>),
}

impl RepoIterator {
    /// The concrete iterator kind, for diagnostics and harness output.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::All(_) => "all",
            Self::Query(_) => "query",
            Self::Scoped(_) => "scoped",
        }
    }

    pub async fn for_each(
        &self,
        each: &mut (dyn FnMut(&str, i64) -> Result<(), String> + Send),
    ) -> Result<(), String> {
        match self {
            Self::All(iterator) => iterator.for_each(each).await,
            Self::Query(iterator) => iterator.for_each(each).await,
            Self::Scoped(iterator) => iterator.for_each(each).await,
        }
    }
}

#[derive(Clone)]
struct CachedPage {
    age: OffsetDateTime,
    results: Vec<RepoSummary>,
}

/// Pages through every repository once per cache window.
///
/// The cache lives behind a mutex (rather than `&mut self`) so the iterator can
/// be shared as an `Arc` between the `SeriesRepoIterator` dispatch and the
/// callers that run it, mirroring the Go pointer-sharing semantics.
pub struct AllReposIterator {
    repo_store: Arc<dyn RepoStore>,
    clock: Arc<dyn Clock>,
    cache_time: Duration,
    cached_pages: Mutex<HashMap<LimitOffset, CachedPage>>,
}

impl AllReposIterator {
    /// `discovery.NewAllReposIterator` without the Prometheus counters.
    pub fn new(
        repo_store: Arc<dyn RepoStore>,
        clock: Arc<dyn Clock>,
        cache_time: Duration,
    ) -> Self {
        Self {
            repo_store,
            clock,
            cache_time,
            cached_pages: Mutex::new(HashMap::new()),
        }
    }

    fn time_since(&self, then: OffsetDateTime) -> Duration {
        self.clock.now() - then
    }

    /// Invokes `each` for every repository, 1,000 at a time.
    ///
    /// * store error → `RepoStore.List: <err>` (pagination stops);
    /// * callback error → `forEach: <err>`.
    pub async fn for_each(
        &self,
        each: &mut (dyn FnMut(&str, i64) -> Result<(), String> + Send),
    ) -> Result<(), String> {
        let mut page = LimitOffset {
            limit: 1000,
            offset: 0,
        };
        loop {
            let repos = self
                .cached_repo_store_list(&page)
                .await
                .map_err(|err| format!("RepoStore.List: {err}"))?;
            if repos.is_empty() {
                return Ok(());
            }
            for repo in &repos {
                each(&repo.name, repo.id).map_err(|err| format!("forEach: {err}"))?;
            }
            page.offset += i64::try_from(repos.len()).unwrap_or(0);
        }
    }

    /// Cached `RepoStore.List(LimitOffset)`; entries younger than
    /// `cache_time` are served without touching the store.
    async fn cached_repo_store_list(&self, page: &LimitOffset) -> Result<Vec<RepoSummary>, String> {
        let cached = self
            .cached_pages
            .lock()
            .expect("discovery page cache poisoned")
            .get(page)
            .cloned();
        if let Some(entry) = cached {
            if self.time_since(entry.age) < self.cache_time {
                return Ok(entry.results);
            }
        }

        let repos = self
            .repo_store
            .list(&ReposListOptions {
                names: None,
                limit_offset: Some(page.clone()),
            })
            .await?;

        self.cached_pages
            .lock()
            .expect("discovery page cache poisoned")
            .insert(
                page.clone(),
                CachedPage {
                    age: self.clock.now(),
                    results: repos.clone(),
                },
            );
        Ok(repos)
    }
}

/// Repositories resolved by running a repository-scope search query.
pub struct RepoIteratorFromQuery {
    scope_query: String,
    repos: Vec<MinimalRepo>,
}

impl RepoIteratorFromQuery {
    /// Builds `RepositoryScopeQuery(query)` and asks the executor for the
    /// matching repositories.
    ///
    /// A query-builder error is wrapped as
    /// `could not build repository scope query: <err>`; executor errors are
    /// returned unwrapped (both match the baseline).
    pub async fn new(query: &str, executor: &dyn RepoQueryExecutor) -> Result<Self, String> {
        let repo_scope_query = repository_scope_query(query)
            .map_err(|err| format!("could not build repository scope query: {err}"))?;
        let repos = executor.execute_repo_list(&repo_scope_query).await?;
        Ok(Self {
            repos,
            scope_query: query.to_owned(),
        })
    }

    /// The original (user) query, kept for parity with the Go struct.
    pub fn scope_query(&self) -> &str {
        &self.scope_query
    }

    pub async fn for_each(
        &self,
        each: &mut (dyn FnMut(&str, i64) -> Result<(), String> + Send),
    ) -> Result<(), String> {
        for repo in &self.repos {
            each(&repo.name, repo.id)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SimpleRepo {
    name: String,
    id: i64,
}

/// Repositories resolved from a configured name list.
pub struct ScopedRepoIterator {
    repos: Vec<SimpleRepo>,
}

impl ScopedRepoIterator {
    pub async fn new(repo_names: &[String], store: &dyn RepoStore) -> Result<Self, String> {
        let repos = load_repo_ids(repo_names, store).await?;
        Ok(Self { repos })
    }

    pub async fn for_each(
        &self,
        each: &mut (dyn FnMut(&str, i64) -> Result<(), String> + Send),
    ) -> Result<(), String> {
        for repo in &self.repos {
            each(&repo.name, repo.id)?;
        }
        Ok(())
    }
}

async fn load_repo_ids(
    repo_names: &[String],
    repo_store: &dyn RepoStore,
) -> Result<Vec<SimpleRepo>, String> {
    let list = repo_store
        .list(&ReposListOptions {
            names: Some(repo_names.to_vec()),
            limit_offset: None,
        })
        .await
        .map_err(|err| format!("repoStore.List: {err}"))?;
    Ok(list
        .into_iter()
        .map(|repo| SimpleRepo {
            name: repo.name,
            id: repo.id,
        })
        .collect())
}

/// The fields of `types.InsightSeries` that drive iterator selection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SeriesScope {
    pub repositories: Vec<String>,
    pub repository_criteria: Option<String>,
}

/// `discovery.NewSeriesRepoIterator`: picks the iterator for a series.
pub struct SeriesRepoIterator {
    all_repo_iterator: Arc<AllReposIterator>,
    repo_store: Arc<dyn RepoStore>,
    repo_query_executor: Arc<dyn RepoQueryExecutor>,
}

impl SeriesRepoIterator {
    pub fn new(
        all_repos_iterator: Arc<AllReposIterator>,
        repo_store: Arc<dyn RepoStore>,
        repo_query_executor: Arc<dyn RepoQueryExecutor>,
    ) -> Self {
        Self {
            all_repo_iterator: all_repos_iterator,
            repo_store,
            repo_query_executor,
        }
    }

    /// Empty repository list and no criteria → the shared all-repos iterator;
    /// criteria → a query iterator; otherwise → a scoped iterator.
    pub async fn for_series(&self, series: &SeriesScope) -> Result<RepoIterator, String> {
        if series.repositories.is_empty() {
            match &series.repository_criteria {
                None => Ok(RepoIterator::All(Arc::clone(&self.all_repo_iterator))),
                Some(criteria) => Ok(RepoIterator::Query(Arc::new(
                    RepoIteratorFromQuery::new(criteria, self.repo_query_executor.as_ref()).await?,
                ))),
            }
        } else {
            Ok(RepoIterator::Scoped(Arc::new(
                ScopedRepoIterator::new(&series.repositories, self.repo_store.as_ref()).await?,
            )))
        }
    }
}
