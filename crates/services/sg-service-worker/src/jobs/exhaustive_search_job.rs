#![forbid(unsafe_code)]

//! Exhaustive search worker routines
//! (`cmd/worker/internal/search/{exhaustive_search,exhaustive_search_repo,
//! exhaustive_search_repo_revision}.go`).
//!
//! Three DB-backed workers drive the `exhaustive_search_*` queues built in
//! [`crate::jobs::exhaustive_search_workers`]:
//!
//! * the job handler expands the initiator's query into
//!   `exhaustive_search_repo_jobs` rows;
//! * the repo handler resolves one repo/rev spec into
//!   `exhaustive_search_repo_revision_jobs` rows;
//! * the revision handler runs one search and streams the matches into a
//!   writer (the baseline's `service.NewJSONWriter`).
//!
//! The search execution itself (`service.FromSearchClient`, i.e. the Go search
//! client and job pipeline) is not ported, so the handlers depend on the
//! injected [`NewSearcher`] / [`MatchWriterFactory`] traits — the same seam the
//! baseline exposes to its tests (`newSearchJobRoutines` takes a
//! `newSearcherFactory`). [`SearchJob`] is registered under
//! `exhaustive-search-job` in `all_jobs()` and assembles the baseline's seven
//! routines. Its production searcher factory is
//! [`FromSearchClient::production`] with the plan implemented by
//! [`crate::jobs::exhaustive_search_searcher::PlanSearchClient`]. Repository
//! resolution and job execution remain explicit not-ported seams until
//! `repos.Resolver` and the exhaustive job runner are ported.
//!
//! The janitor (`search_jobs_janitor`) lives in
//! [`crate::jobs::exhaustive_search_janitor`] and is assembled by
//! [`assemble_search_job_routines`] too, with the blobstore injected.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_db::exhaustive_search::{
    self as ex, Actor, ExhaustiveSearchJob, ExhaustiveSearchJobWorkerStore,
    ExhaustiveSearchRepoJob, ExhaustiveSearchRepoJobWorkerStore, ExhaustiveSearchRepoRevisionJob,
    ExhaustiveSearchRepoRevisionJobWorkerStore, RepositoryRevSpecs, RepositoryRevision,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::env::parse_go_bool;
use crate::framework::dbworker::{
    DbBackedRoutine, DbWorkerHandler, ResetterOptions, ResetterRoutine, WorkerOptions,
    WorkerRoutine,
};
use crate::framework::BackgroundRoutine;
use crate::jobs::exhaustive_search_janitor::{
    new_search_jobs_janitor_routine, storage_config, JobLogsBlobStore, PgJanitorService,
    SearchJobsObjectStorageConfig,
};
use crate::jobs::exhaustive_search_searcher::{FromSearchClient, JobClients};
use crate::jobs::exhaustive_search_service::{
    new_object_storage, ObjectStorageBackend, ObjectStorageLogBlobStore,
    ObjectStorageMatchWriterFactory, ObjectStorageUploadStore, UploadStore,
};
use crate::jobs::exhaustive_search_workers::{
    new_exhaustive_search_job_worker_store, new_repo_search_job_worker_store,
    new_rev_search_job_worker_store,
};
use crate::jobs::{Job, JobConfig, JobDeps};

/// `exhaustive_search_worker`.
pub const SEARCH_WORKER_NAME: &str = "exhaustive_search_worker";
/// `exhaustive_search_worker.Description`.
pub const SEARCH_WORKER_DESCRIPTION: &str = "runs the exhaustive search";
/// `exhaustive_search_repo_worker`.
pub const REPO_WORKER_NAME: &str = "exhaustive_search_repo_worker";
/// `exhaustive_search_repo_worker.Description`.
pub const REPO_WORKER_DESCRIPTION: &str = "runs the exhaustive search on a repository";
/// `exhaustive_search_repo_revision_worker`.
pub const REV_WORKER_NAME: &str = "exhaustive_search_repo_revision_worker";
/// `exhaustive_search_repo_revision_worker.Description`.
pub const REV_WORKER_DESCRIPTION: &str = "runs the exhaustive search on a revision of a repository";

/// `exhaustive_search_worker_resetter`.
pub const SEARCH_RESETTER_NAME: &str = "exhaustive_search_worker_resetter";
/// `exhaustive_search_repo_worker_resetter`.
pub const REPO_RESETTER_NAME: &str = "exhaustive_search_repo_worker_resetter";
/// `exhaustive_search_repo_revision_worker_resetter`.
pub const REV_RESETTER_NAME: &str = "exhaustive_search_repo_revision_worker_resetter";

/// `WorkerOptions.NumHandlers` for all three workers.
pub const NUM_HANDLERS: usize = 5;
/// `WorkerOptions.HeartbeatInterval` for all three workers.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// `ResetterOptions.Interval` for all three resetters.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(60);
/// `config.WorkerInterval`'s default (`NewSearchJob`).
pub const DEFAULT_WORKER_INTERVAL: Duration = Duration::from_secs(1);

/// `config` in `cmd/worker/internal/search/job.go`: the per-worker interval
/// that tests override.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchJobConfig {
    pub worker_interval: Duration,
}

impl Default for SearchJobConfig {
    fn default() -> Self {
        Self {
            worker_interval: DEFAULT_WORKER_INTERVAL,
        }
    }
}

/// `exhaustive.IsEnabled`: `experimentalFeatures.searchJobs` when set, else
/// `!DISABLE_SEARCH_JOBS` (default enabled).
pub fn is_enabled(live_config: &LiveConfig) -> bool {
    if let Some(value) = live_config
        .current()
        .site
        .raw()
        .get("experimentalFeatures")
        .and_then(|features| features.get("searchJobs"))
        .and_then(serde_json::Value::as_bool)
    {
        return value;
    }

    !parse_go_bool(&std::env::var("DISABLE_SEARCH_JOBS").unwrap_or_default()).unwrap_or(false)
}

/// `service.NewSearcher`: parse and minimally resolve a query on behalf of
/// `user_id` (the baseline also checks that `user_id` matches the context's
/// actor; Rust has no actor context, so the implementation is responsible for
/// running as that user).
#[async_trait]
pub trait NewSearcher: Send + Sync {
    async fn new_search(&self, user_id: i32, query: &str) -> Result<Box<dyn SearchQuery>, String>;
}

/// `service.SearchQuery`.
///
/// The baseline returns an iterator from `RepositoryRevSpecs`; the handler
/// consumes it linearly and only surfaces the error at the end, which a
/// materialised `Vec` reproduces.
#[async_trait]
pub trait SearchQuery: Send + Sync {
    async fn repository_rev_specs(&self) -> Result<Vec<RepositoryRevSpecs>, String>;

    async fn resolve_repository_rev_spec(
        &self,
        repo_rev_spec: &RepositoryRevSpecs,
    ) -> Result<Vec<RepositoryRevision>, String>;

    async fn search(
        &self,
        repo_rev: &RepositoryRevision,
        match_writer: &dyn MatchWriter,
    ) -> Result<(), String>;
}

/// `service.MatchWriter`. The baseline takes `result.Match`; the search result
/// types are not ported, so the payload is the JSON form the eventual writer
/// serialises.
pub trait MatchWriter: Send + Sync {
    fn write(&self, r#match: serde_json::Value) -> Result<(), String>;

    fn flush(&self) -> Result<(), String>;
}

/// `service.NewJSONWriter`: the object-storage writer is not ported, so the
/// worker only depends on this factory.
pub trait MatchWriterFactory: Send + Sync {
    fn new_writer(&self, key: &str) -> Result<Box<dyn MatchWriter>, String>;
}

/// `exhaustiveSearchHandler`.
pub struct ExhaustiveSearchHandler {
    pool: PgPool,
    new_searcher: Arc<dyn NewSearcher>,
}

impl ExhaustiveSearchHandler {
    pub fn new(pool: PgPool, new_searcher: Arc<dyn NewSearcher>) -> Self {
        Self { pool, new_searcher }
    }
}

#[async_trait]
impl DbWorkerHandler<ExhaustiveSearchJobWorkerStore> for ExhaustiveSearchHandler {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &ExhaustiveSearchJob,
    ) -> Result<(), String> {
        // actor.WithActor(ctx, actor.FromUser(userID)) has no Rust equivalent;
        // the initiator id is passed to the searcher instead.
        let user_id = record.initiator_id;
        let query = self.new_searcher.new_search(user_id, &record.query).await?;

        let repo_rev_specs = query.repository_rev_specs().await?;

        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;
        for repo_rev_spec in &repo_rev_specs {
            ex::create_exhaustive_search_repo_job_in_tx(
                &mut tx,
                repo_rev_spec.repository,
                record.id,
                &repo_rev_spec.revision_specifiers,
            )
            .await
            .map_err(|err| err.to_string())?;
        }
        tx.commit().await.map_err(|err| err.to_string())
    }
}

/// `exhaustiveSearchRepoHandler`.
pub struct ExhaustiveSearchRepoHandler {
    pool: PgPool,
    new_searcher: Arc<dyn NewSearcher>,
}

impl ExhaustiveSearchRepoHandler {
    pub fn new(pool: PgPool, new_searcher: Arc<dyn NewSearcher>) -> Self {
        Self { pool, new_searcher }
    }
}

#[async_trait]
impl DbWorkerHandler<ExhaustiveSearchRepoJobWorkerStore> for ExhaustiveSearchRepoHandler {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &ExhaustiveSearchRepoJob,
    ) -> Result<(), String> {
        let repo_rev_spec = RepositoryRevSpecs {
            repository: record.repo_id,
            revision_specifiers: record.ref_spec.clone(),
        };

        let parent =
            ex::get_exhaustive_search_job(&self.pool, Actor::internal(), record.search_job_id)
                .await
                .map_err(|err| err.to_string())?;

        let query = self
            .new_searcher
            .new_search(parent.initiator_id, &parent.query)
            .await?;

        let repo_revisions = query.resolve_repository_rev_spec(&repo_rev_spec).await?;

        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;
        for repo_rev in &repo_revisions {
            ex::create_exhaustive_search_repo_revision_job_in_tx(
                &mut tx,
                record.id,
                &repo_rev.revision,
            )
            .await
            .map_err(|err| err.to_string())?;
        }
        tx.commit().await.map_err(|err| err.to_string())
    }
}

/// `exhaustiveSearchRepoRevHandler`.
pub struct ExhaustiveSearchRepoRevisionHandler {
    pool: PgPool,
    new_searcher: Arc<dyn NewSearcher>,
    writers: Arc<dyn MatchWriterFactory>,
}

impl ExhaustiveSearchRepoRevisionHandler {
    pub fn new(
        pool: PgPool,
        new_searcher: Arc<dyn NewSearcher>,
        writers: Arc<dyn MatchWriterFactory>,
    ) -> Self {
        Self {
            pool,
            new_searcher,
            writers,
        }
    }
}

#[async_trait]
impl DbWorkerHandler<ExhaustiveSearchRepoRevisionJobWorkerStore>
    for ExhaustiveSearchRepoRevisionHandler
{
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &ExhaustiveSearchRepoRevisionJob,
    ) -> Result<(), String> {
        let query_repo_rev =
            ex::get_query_repo_rev(&self.pool, record.search_repo_job_id, &record.revision)
                .await
                .map_err(|err| err.to_string())?;

        let query = self
            .new_searcher
            .new_search(query_repo_rev.initiator_id, &query_repo_rev.query)
            .await?;

        let writer = self
            .writers
            .new_writer(&format!("{}-{}", query_repo_rev.id, record.id))?;

        let mut err = query
            .search(&query_repo_rev.repo_rev, writer.as_ref())
            .await
            .err();
        if let Err(flush_err) = writer.flush() {
            err = append_errors(err, Some(flush_err));
        }

        err.map_or(Ok(()), Err)
    }
}

/// `errors.Append(err, closeErr)` for the handler's two-error case: a
/// `MultiError` renders `N errors occurred:` followed by `\n\t* ` entries.
pub(crate) fn append_errors(first: Option<String>, second: Option<String>) -> Option<String> {
    match (first, second) {
        (None, None) => None,
        (Some(err), None) | (None, Some(err)) => Some(err),
        (Some(first), Some(second)) => {
            Some(format!("2 errors occurred:\n\t* {first}\n\t* {second}"))
        }
    }
}

/// `newExhaustiveSearchWorker`.
pub fn new_exhaustive_search_worker(
    store: Arc<ExhaustiveSearchJobWorkerStore>,
    handler: Arc<ExhaustiveSearchHandler>,
    config: &SearchJobConfig,
) -> WorkerRoutine<ExhaustiveSearchJobWorkerStore, ExhaustiveSearchHandler> {
    WorkerRoutine::new(
        store,
        handler,
        WorkerOptions {
            name: SEARCH_WORKER_NAME.to_owned(),
            description: SEARCH_WORKER_DESCRIPTION.to_owned(),
            interval: config.worker_interval,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            num_handlers: NUM_HANDLERS,
            worker_hostname: crate::framework::dbworker::default_hostname(),
        },
    )
}

/// `newExhaustiveSearchRepoWorker`.
pub fn new_exhaustive_search_repo_worker(
    store: Arc<ExhaustiveSearchRepoJobWorkerStore>,
    handler: Arc<ExhaustiveSearchRepoHandler>,
    config: &SearchJobConfig,
) -> WorkerRoutine<ExhaustiveSearchRepoJobWorkerStore, ExhaustiveSearchRepoHandler> {
    WorkerRoutine::new(
        store,
        handler,
        WorkerOptions {
            name: REPO_WORKER_NAME.to_owned(),
            description: REPO_WORKER_DESCRIPTION.to_owned(),
            interval: config.worker_interval,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            num_handlers: NUM_HANDLERS,
            worker_hostname: crate::framework::dbworker::default_hostname(),
        },
    )
}

/// `newExhaustiveSearchRepoRevisionWorker`.
pub fn new_exhaustive_search_repo_revision_worker(
    store: Arc<ExhaustiveSearchRepoRevisionJobWorkerStore>,
    handler: Arc<ExhaustiveSearchRepoRevisionHandler>,
    config: &SearchJobConfig,
) -> WorkerRoutine<ExhaustiveSearchRepoRevisionJobWorkerStore, ExhaustiveSearchRepoRevisionHandler>
{
    WorkerRoutine::new(
        store,
        handler,
        WorkerOptions {
            name: REV_WORKER_NAME.to_owned(),
            description: REV_WORKER_DESCRIPTION.to_owned(),
            interval: config.worker_interval,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            num_handlers: NUM_HANDLERS,
            worker_hostname: crate::framework::dbworker::default_hostname(),
        },
    )
}

/// `newExhaustiveSearchWorkerResetter`.
pub fn new_exhaustive_search_worker_resetter(
    store: Arc<ExhaustiveSearchJobWorkerStore>,
) -> ResetterRoutine {
    ResetterRoutine::new(
        store as Arc<dyn crate::framework::dbworker::DbResetter>,
        ResetterOptions {
            name: SEARCH_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    )
}

/// `newExhaustiveSearchRepoWorkerResetter`.
pub fn new_exhaustive_search_repo_worker_resetter(
    store: Arc<ExhaustiveSearchRepoJobWorkerStore>,
) -> ResetterRoutine {
    ResetterRoutine::new(
        store as Arc<dyn crate::framework::dbworker::DbResetter>,
        ResetterOptions {
            name: REPO_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    )
}

/// `newExhaustiveSearchRepoRevisionWorkerResetter`.
pub fn new_exhaustive_search_repo_revision_worker_resetter(
    store: Arc<ExhaustiveSearchRepoRevisionJobWorkerStore>,
) -> ResetterRoutine {
    ResetterRoutine::new(
        store as Arc<dyn crate::framework::dbworker::DbResetter>,
        ResetterOptions {
            name: REV_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    )
}

/// The result of `searchJob.newSearchJobRoutines`: the seven background
/// routines plus the worker/resetter options the group was assembled with.
/// The options are exposed for the differential harness; the routines are all
/// that `Job::routines` returns.
pub struct SearchJobRoutines {
    pub routines: Vec<BackgroundRoutine>,
    pub worker_options: Vec<WorkerOptions>,
    pub resetter_options: Vec<ResetterOptions>,
}

/// `searchJob.newSearchJobRoutines` after the database, upload store and
/// searcher factory have been resolved: the seven routines (three workers,
/// three resetters, the janitor) in the baseline's order.
pub fn assemble_search_job_routines(
    pool: PgPool,
    new_searcher: Arc<dyn NewSearcher>,
    writer_factory: Arc<dyn MatchWriterFactory>,
    blob_store: Arc<dyn JobLogsBlobStore>,
    config: &SearchJobConfig,
) -> SearchJobRoutines {
    let search_worker_store = Arc::new(new_exhaustive_search_job_worker_store(pool.clone()));
    let repo_worker_store = Arc::new(new_repo_search_job_worker_store(pool.clone()));
    let rev_worker_store = Arc::new(new_rev_search_job_worker_store(pool.clone()));

    let search_worker = Arc::new(new_exhaustive_search_worker(
        Arc::clone(&search_worker_store),
        Arc::new(ExhaustiveSearchHandler::new(
            pool.clone(),
            Arc::clone(&new_searcher),
        )),
        config,
    ));
    let repo_worker = Arc::new(new_exhaustive_search_repo_worker(
        Arc::clone(&repo_worker_store),
        Arc::new(ExhaustiveSearchRepoHandler::new(
            pool.clone(),
            Arc::clone(&new_searcher),
        )),
        config,
    ));
    let rev_worker = Arc::new(new_exhaustive_search_repo_revision_worker(
        Arc::clone(&rev_worker_store),
        Arc::new(ExhaustiveSearchRepoRevisionHandler::new(
            pool.clone(),
            Arc::clone(&new_searcher),
            writer_factory,
        )),
        config,
    ));

    let search_resetter = Arc::new(new_exhaustive_search_worker_resetter(Arc::clone(
        &search_worker_store,
    )));
    let repo_resetter = Arc::new(new_exhaustive_search_repo_worker_resetter(Arc::clone(
        &repo_worker_store,
    )));
    let rev_resetter = Arc::new(new_exhaustive_search_repo_revision_worker_resetter(
        Arc::clone(&rev_worker_store),
    ));

    let worker_options = vec![
        search_worker.options().clone(),
        repo_worker.options().clone(),
        rev_worker.options().clone(),
    ];
    let resetter_options = vec![
        ResetterOptions {
            name: SEARCH_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
        ResetterOptions {
            name: REPO_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
        ResetterOptions {
            name: REV_RESETTER_NAME.to_owned(),
            interval: RESETTER_INTERVAL,
        },
    ];

    let janitor_pool = pool.clone();
    let janitor_service: Arc<dyn crate::jobs::exhaustive_search_janitor::JanitorService> =
        Arc::new(PgJanitorService::new(pool, blob_store));
    let janitor = new_search_jobs_janitor_routine(janitor_pool, janitor_service);

    let routines = vec![
        BackgroundRoutine::DbBacked(search_worker as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(repo_worker as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(rev_worker as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(search_resetter as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(repo_resetter as Arc<dyn DbBackedRoutine>),
        BackgroundRoutine::DbBacked(rev_resetter as Arc<dyn DbBackedRoutine>),
        janitor,
    ];

    SearchJobRoutines {
        routines,
        worker_options,
        resetter_options,
    }
}

/// The routines half of [`assemble_search_job_routines`].
pub fn new_search_job_routines(
    pool: PgPool,
    new_searcher: Arc<dyn NewSearcher>,
    writer_factory: Arc<dyn MatchWriterFactory>,
    blob_store: Arc<dyn JobLogsBlobStore>,
    config: &SearchJobConfig,
) -> Vec<BackgroundRoutine> {
    assemble_search_job_routines(pool, new_searcher, writer_factory, blob_store, config).routines
}

/// The factory the baseline's `searchJob.Routines` receives
/// (`newSearcherFactory`): one searcher for the whole group.
pub type NewSearcherFactory = Arc<dyn Fn() -> Arc<dyn NewSearcher> + Send + Sync>;

/// `searchJob` (`cmd/worker/internal/search/job.go`): the
/// `exhaustive-search-job` group.
///
/// `routines` mirrors `searchJob.Routines`: disabled when
/// [`is_enabled`] is false, otherwise it builds the same lazy object storage
/// as the M9.8id janitor and assembles the seven routines. The production
/// searcher factory is [`FromSearchClient::production`]: `client.Plan` is
/// real, while repository resolution and job execution are explicit
/// not-ported seams. Tests and the differential harness inject a fake through
/// [`SearchJob::with_searcher_factory`].
pub struct SearchJob {
    config: Arc<SearchJobsObjectStorageConfig>,
    new_searcher_factory: NewSearcherFactory,
}

impl SearchJob {
    pub fn new() -> Self {
        Self::with_searcher_factory(Arc::new(|| {
            Arc::new(FromSearchClient::production(JobClients::not_ported()))
        }))
    }

    /// The baseline's `newSearcherFactory` seam. Production uses
    /// [`Self::new`]; tests inject a fake searcher.
    pub fn with_searcher_factory(factory: NewSearcherFactory) -> Self {
        Self {
            config: Arc::new(SearchJobsObjectStorageConfig::default()),
            new_searcher_factory: factory,
        }
    }

    /// The search-jobs object storage config shared with `loadConfigs`.
    pub fn config(&self) -> Arc<SearchJobsObjectStorageConfig> {
        Arc::clone(&self.config)
    }

    /// `searchJob.Routines`: the assembled routines or an empty list when the
    /// feature is disabled.
    pub async fn assemble(&self, deps: &JobDeps) -> Result<SearchJobRoutines, String> {
        if !is_enabled(&deps.live_config) {
            return Ok(SearchJobRoutines {
                routines: Vec::new(),
                worker_options: Vec::new(),
                resetter_options: Vec::new(),
            });
        }

        let backend: Arc<dyn ObjectStorageBackend> =
            new_object_storage(storage_config(&self.config)?).await?;
        let uploads: Arc<dyn UploadStore> =
            Arc::new(ObjectStorageUploadStore::new(Arc::clone(&backend)));
        let writer_factory: Arc<dyn MatchWriterFactory> =
            Arc::new(ObjectStorageMatchWriterFactory::new(uploads));
        let blob_store: Arc<dyn JobLogsBlobStore> =
            Arc::new(ObjectStorageLogBlobStore::new(backend));
        let new_searcher = (self.new_searcher_factory)();

        Ok(assemble_search_job_routines(
            deps.frontend.clone(),
            new_searcher,
            writer_factory,
            blob_store,
            &SearchJobConfig::default(),
        ))
    }
}

impl Default for SearchJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for SearchJob {
    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Ok(self.assemble(deps).await?.routines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn worker_options_match_baseline() {
        let config = SearchJobConfig::default();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool");

        let search_worker = new_exhaustive_search_worker(
            Arc::new(new_exhaustive_search_job_worker_store(pool.clone())),
            Arc::new(ExhaustiveSearchHandler::new(
                pool.clone(),
                Arc::new(UnusedSearcher),
            )),
            &config,
        );
        let options = search_worker.options();
        assert_eq!(options.name, "exhaustive_search_worker");
        assert_eq!(options.description, "runs the exhaustive search");
        assert_eq!(options.num_handlers, 5);
        assert_eq!(options.heartbeat_interval, Duration::from_secs(15));
        assert_eq!(options.interval, Duration::from_secs(1));

        let repo_worker = new_exhaustive_search_repo_worker(
            Arc::new(new_repo_search_job_worker_store(pool.clone())),
            Arc::new(ExhaustiveSearchRepoHandler::new(
                pool.clone(),
                Arc::new(UnusedSearcher),
            )),
            &config,
        );
        assert_eq!(repo_worker.options().name, "exhaustive_search_repo_worker");
        assert_eq!(
            repo_worker.options().description,
            "runs the exhaustive search on a repository"
        );

        let rev_worker = new_exhaustive_search_repo_revision_worker(
            Arc::new(new_rev_search_job_worker_store(pool)),
            Arc::new(ExhaustiveSearchRepoRevisionHandler::new(
                sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgres://unused@localhost/unused")
                    .expect("lazy pool"),
                Arc::new(UnusedSearcher),
                Arc::new(UnusedWriters),
            )),
            &config,
        );
        assert_eq!(
            rev_worker.options().name,
            "exhaustive_search_repo_revision_worker"
        );
        assert_eq!(
            rev_worker.options().description,
            "runs the exhaustive search on a revision of a repository"
        );
    }

    #[test]
    fn appends_errors_like_multi_error() {
        assert_eq!(append_errors(None, None), None);
        assert_eq!(append_errors(Some("a".into()), None), Some("a".into()));
        assert_eq!(append_errors(None, Some("b".into())), Some("b".into()));
        assert_eq!(
            append_errors(Some("a".into()), Some("b".into())),
            Some("2 errors occurred:\n\t* a\n\t* b".into())
        );
    }

    struct UnusedSearcher;

    #[async_trait]
    impl NewSearcher for UnusedSearcher {
        async fn new_search(
            &self,
            _user_id: i32,
            _query: &str,
        ) -> Result<Box<dyn SearchQuery>, String> {
            Err("unused".into())
        }
    }

    struct UnusedWriters;

    impl MatchWriterFactory for UnusedWriters {
        fn new_writer(&self, _key: &str) -> Result<Box<dyn MatchWriter>, String> {
            Err("unused".into())
        }
    }

    struct UnusedBlobs;

    #[async_trait]
    impl JobLogsBlobStore for UnusedBlobs {
        async fn upload(&self, _key: &str, _contents: Vec<u8>) -> Result<i64, String> {
            Err("unused".into())
        }

        async fn delete(&self, _key: &str) -> Result<(), String> {
            Err("unused".into())
        }
    }

    #[tokio::test]
    async fn assembles_the_seven_baseline_routines() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool");
        let assembled = assemble_search_job_routines(
            pool,
            Arc::new(UnusedSearcher),
            Arc::new(UnusedWriters),
            Arc::new(UnusedBlobs),
            &SearchJobConfig::default(),
        );

        let names: Vec<&str> = assembled.routines.iter().map(|r| r.name()).collect();
        assert_eq!(
            names,
            vec![
                "exhaustive_search_worker",
                "exhaustive_search_repo_worker",
                "exhaustive_search_repo_revision_worker",
                "exhaustive_search_worker_resetter",
                "exhaustive_search_repo_worker_resetter",
                "exhaustive_search_repo_revision_worker_resetter",
                "search_jobs_janitor",
            ]
        );
        assert_eq!(assembled.worker_options.len(), 3);
        assert_eq!(assembled.resetter_options.len(), 3);
        for options in &assembled.worker_options {
            assert_eq!(options.num_handlers, NUM_HANDLERS);
            assert_eq!(options.heartbeat_interval, HEARTBEAT_INTERVAL);
            assert_eq!(options.interval, DEFAULT_WORKER_INTERVAL);
        }
        for options in &assembled.resetter_options {
            assert_eq!(options.interval, RESETTER_INTERVAL);
        }
        assert_eq!(
            assembled.routines.last().map(|r| r.interval()),
            Some(crate::jobs::exhaustive_search_janitor::JANITOR_INTERVAL)
        );
    }
}
