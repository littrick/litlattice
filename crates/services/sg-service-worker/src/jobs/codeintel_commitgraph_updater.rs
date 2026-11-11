#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/uploads_commitgraph.go` +
//! `internal/codeintel/uploads/internal/background/commitgraph/*`: the
//! `codeintel-commitgraph-updater` job.
//!
//! The job owns `uploads.CommitGraphConfigInst` (`commitgraph.Config`) and
//! returns one periodic routine built by `NewCommitGraphUpdater`:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.commitgraph-updater` | `CODEINTEL_UPLOAD_COMMITGRAPH_UPDATER_INTERVAL` (10s) | decorate the commit graph of dirty repositories |
//!
//! Each invocation runs `UpdateAllDirtyCommitGraphs`:
//!
//! 1. `GetDirtyRepositories` lists repositories whose dirty token moved past
//!    the update token; an error aborts the whole pass with
//!    `uploadSvc.DirtyRepositories: <err>`.
//! 2. For each repository an advisory lock is taken (`blocking=false`); if it
//!    is already held the repository is skipped silently (`errors.Wrap(nil)`
//!    is nil in the baseline, so an unacquired lock is not an error).
//! 3. `GetCommitAndDateForOldestUpload` selects the boundary commit. Without
//!    a completed upload the graph is empty (but refs are still listed and
//!    written, which clears stale rows).
//! 4. The site config's `experimentalFeatures.commitGraphUpdates
//!    .defaultBranchOnly` regexes choose between `all-refs-since`
//!    (`AllRefs: true`, `After: committerDate - 1s`, topological order) and
//!    `head-topo-only` (`Range: oldest..defaultBranch`, topological order).
//! 5. `ListRefs(headsOnly, tagsOnly)` is indexed by commit id and the raw
//!    commit graph is handed to the store, which decorates it and persists
//!    `lsif_nearest_uploads`, links, tip visibility and the dirty token.
//!
//! Only the store boundary is seamed: `CommitGraphStore`,
//! `CommitGraphGitserver`, `CommitGraphLocker` and `CommitGraphConfigSource`
//! are traits so the differential harness can drive the exact control flow
//! with fakes. Production gitserver (sg-git) and locker (PostgreSQL advisory
//! locks) are wired; the store's `update_uploads_visible_to_commits` is not
//! ported yet and returns an explicit error (see the module's README note in
//! `dev/rs/docs/overviews/m9_8in_commitgraph_updater_overview.md` §4).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use prost_types::Timestamp;
use sg_codeintel::commitgraph::{self, matches_default_branch_only, CommitGraph};
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::{ConfigError, EnvReader};
use sg_git::{Commit, CommitsOptions, CommitsOrder, GitRef};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `uploadCommitGraphUpdaterJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `commitgraph.NewCommitGraphUpdater`'s routine metadata.
pub const ROUTINE_NAME: &str = "codeintel.commitgraph-updater";
pub const ROUTINE_DESCRIPTION: &str = "updates the visibility commit graph for dirty repos";

/// `commitgraph.Config` environment variables.
pub const INTERVAL_VAR: &str = "CODEINTEL_UPLOAD_COMMITGRAPH_UPDATER_INTERVAL";
pub const MAX_AGE_FOR_NON_STALE_BRANCHES_VARS: &[&str] = &[
    "CODEINTEL_UPLOAD_COMMITGRAPH_MAX_AGE_FOR_NON_STALE_BRANCHES",
    "PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_BRANCHES",
];
pub const MAX_AGE_FOR_NON_STALE_TAGS_VARS: &[&str] = &[
    "CODEINTEL_UPLOAD_COMMITGRAPH_MAX_AGE_FOR_NON_STALE_TAGS",
    "PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_TAGS",
];
pub const UPDATE_TASK_INTERVAL_VARS: &[&str] = &[
    "CODEINTEL_UPLOAD_COMMITGRAPH_UPDATE_TASK_INTERVAL",
    "PRECISE_CODE_INTEL_COMMIT_GRAPH_UPDATE_TASK_INTERVAL",
];

const INTERVAL_DESCRIPTION: &str = "How frequently to run the upload commitgraph updater routine.";
const MAX_AGE_FOR_NON_STALE_BRANCHES_DESCRIPTION: &str =
    "The age after which a branch should be considered stale. Code intelligence indexes will be evicted from stale branches.";
const MAX_AGE_FOR_NON_STALE_TAGS_DESCRIPTION: &str =
    "The age after which a tagged commit should be considered stale. Code intelligence indexes will be evicted from stale tagged commits.";
const UPDATE_TASK_INTERVAL_DESCRIPTION: &str =
    "The frequency with which to run periodic codeintel commit graph update tasks.";

/// `commitgraph.Config` (`uploads.CommitGraphConfigInst`).
#[derive(Debug, Default)]
pub struct CommitGraphConfig {
    interval: Mutex<Duration>,
    max_age_for_non_stale_branches: Mutex<Duration>,
    max_age_for_non_stale_tags: Mutex<Duration>,
    commit_graph_update_task_interval: Mutex<Duration>,
    errs: Mutex<Vec<String>>,
}

impl CommitGraphConfig {
    pub fn interval(&self) -> Duration {
        lock(&self.interval)
    }

    pub fn max_age_for_non_stale_branches(&self) -> Duration {
        lock(&self.max_age_for_non_stale_branches)
    }

    pub fn max_age_for_non_stale_tags(&self) -> Duration {
        lock(&self.max_age_for_non_stale_tags)
    }

    pub fn commit_graph_update_task_interval(&self) -> Duration {
        lock(&self.commit_graph_update_task_interval)
    }
}

fn lock(value: &Mutex<Duration>) -> Duration {
    *value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl JobConfig for CommitGraphConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        let branches_var = choose_fallback(MAX_AGE_FOR_NON_STALE_BRANCHES_VARS);
        let tags_var = choose_fallback(MAX_AGE_FOR_NON_STALE_TAGS_VARS);
        let task_interval_var = choose_fallback(UPDATE_TASK_INTERVAL_VARS);

        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            get_interval(env, INTERVAL_VAR, "10s", INTERVAL_DESCRIPTION, &mut errs);
        *self
            .max_age_for_non_stale_branches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            &branches_var,
            "2160h",
            MAX_AGE_FOR_NON_STALE_BRANCHES_DESCRIPTION,
            &mut errs,
        );
        *self
            .max_age_for_non_stale_tags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            &tags_var,
            "8760h",
            MAX_AGE_FOR_NON_STALE_TAGS_DESCRIPTION,
            &mut errs,
        );
        *self
            .commit_graph_update_task_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            &task_interval_var,
            "10s",
            UPDATE_TASK_INTERVAL_DESCRIPTION,
            &mut errs,
        );

        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::new(combine_errors(&errs).unwrap_or_default()))
        }
    }
}

/// `env.ChooseFallbackVariableName`: the first name whose environment variable
/// is non-empty; otherwise the first name.
fn choose_fallback(names: &[&str]) -> String {
    names
        .iter()
        .find(|name| {
            std::env::var(name)
                .map(|value| !value.is_empty())
                .unwrap_or(false)
        })
        .copied()
        .unwrap_or(names[0])
        .to_owned()
}

/// `uploads/shared.DirtyRepository`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyRepository {
    pub repository_id: i64,
    pub repository_name: String,
    pub dirty_token: i64,
}

/// `store.CommitWithDate` (`api.CommitID` + `time.Time`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitWithDate {
    pub commit: CommitID,
    pub committer_date: Timestamp,
}

/// `api.CommitID` (a string in the baseline).
pub type CommitID = String;

/// `map[string][]gitdomain.Ref`.
pub type RefsByCommit = BTreeMap<String, Vec<GitRef>>;

/// The store seam: `uploads/internal/store.Store`'s commit-graph subset.
// The Go signature carries this many parameters; grouping them would diverge
// from the ported call sequence.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait CommitGraphStore: Send + Sync {
    /// `Store.GetDirtyRepositories`.
    async fn get_dirty_repositories(&self) -> Result<Vec<DirtyRepository>, String>;

    /// `Store.GetCommitAndDateForOldestUpload`. `None` is `core.None`; a
    /// `Some` with a missing commit date is an error (`backfillIncompleteError`).
    async fn get_commit_and_date_for_oldest_upload(
        &self,
        repository_id: i64,
    ) -> Result<Option<CommitWithDate>, String>;

    /// `Store.UpdateUploadsVisibleToCommits`; `now == None` is the baseline's
    /// zero `time.Time` (the SQL uses `transaction_timestamp()`).
    async fn update_uploads_visible_to_commits(
        &self,
        repository_id: i64,
        commit_graph: &CommitGraph,
        refs: &RefsByCommit,
        max_age_for_non_stale_branches: Duration,
        max_age_for_non_stale_tags: Duration,
        dirty_token: i64,
        now: Option<Timestamp>,
    ) -> Result<(), String>;
}

/// The gitserver seam: `gitserver.Client`'s commit-graph subset.
#[async_trait]
pub trait CommitGraphGitserver: Send + Sync {
    /// `ListRefs(repo, ListRefsOpts{HeadsOnly: true, TagsOnly: true})`.
    async fn list_refs(
        &self,
        repo: &str,
        heads_only: bool,
        tags_only: bool,
    ) -> Result<Vec<GitRef>, String>;

    /// `GetDefaultBranch(repo, short)` -> `(refName, commit)`.
    async fn get_default_branch(&self, repo: &str, short: bool)
        -> Result<(String, String), String>;

    /// `Commits(repo, options)`.
    async fn commits(&self, repo: &str, options: &CommitsOptions) -> Result<Vec<Commit>, String>;
}

/// `locker.UnlockFunc`: receives the handler error and may replace it.
#[async_trait]
pub trait LockGuard: Send {
    async fn unlock(self: Box<Self>, error: Option<String>) -> Option<String>;
}

/// The advisory-lock seam (`database/locker.Locker`).
#[async_trait]
pub trait CommitGraphLocker: Send + Sync {
    /// `Locker.Lock(ctx, key, blocking)`: `Ok(None)` means "already held".
    async fn lock(&self, key: i64, blocking: bool) -> Result<Option<Box<dyn LockGuard>>, String>;
}

/// The site config seam: `conf.SiteConfig().ExperimentalFeatures
/// .CommitGraphUpdates.DefaultBranchOnly`.
pub trait CommitGraphConfigSource: Send + Sync {
    fn default_branch_only(&self) -> Vec<String>;
}

/// Production config source: read the live `LiveConfig` at each invocation,
/// like the baseline's `conf.SiteConfig()`.
pub struct LiveConfigSource {
    live: Arc<LiveConfig>,
}

impl LiveConfigSource {
    pub fn new(live: Arc<LiveConfig>) -> Self {
        Self { live }
    }
}

impl CommitGraphConfigSource for LiveConfigSource {
    fn default_branch_only(&self) -> Vec<String> {
        default_branch_only_from_site(&self.live.current().site)
    }
}

/// `siteConfig.ExperimentalFeatures.CommitGraphUpdates.DefaultBranchOnly`.
pub fn default_branch_only_from_site(site: &SiteConfiguration) -> Vec<String> {
    site.raw()
        .get("experimentalFeatures")
        .and_then(|features| features.get("commitGraphUpdates"))
        .and_then(|updates| updates.get("defaultBranchOnly"))
        .and_then(serde_json::Value::as_array)
        .map(|patterns| {
            patterns
                .iter()
                .filter_map(|pattern| pattern.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// `mapRefsToCommits`: index refs by the commit they point at.
pub fn map_refs_to_commits(refs: &[GitRef]) -> RefsByCommit {
    let mut commits_by_ref: RefsByCommit = BTreeMap::new();
    for git_ref in refs {
        commits_by_ref
            .entry(git_ref.target_commit.clone())
            .or_default()
            .push(git_ref.clone());
    }
    commits_by_ref
}

/// `commitgraph.CommitGraphRefreshStrategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitGraphRefreshStrategy {
    HeadTopoOnly,
    AllRefsSince,
}

/// The `commitGraphUpdater` struct: `UpdateAllDirtyCommitGraphs` and its
/// private helpers behind the four seams.
pub struct CommitGraphUpdater {
    store: Arc<dyn CommitGraphStore>,
    locker: Arc<dyn CommitGraphLocker>,
    gitserver: Arc<dyn CommitGraphGitserver>,
    config: Arc<dyn CommitGraphConfigSource>,
}

impl CommitGraphUpdater {
    pub fn new(
        store: Arc<dyn CommitGraphStore>,
        locker: Arc<dyn CommitGraphLocker>,
        gitserver: Arc<dyn CommitGraphGitserver>,
        config: Arc<dyn CommitGraphConfigSource>,
    ) -> Self {
        Self {
            store,
            locker,
            gitserver,
            config,
        }
    }

    /// `commitGraphUpdater.UpdateAllDirtyCommitGraphs`.
    pub async fn update_all_dirty_commit_graphs(
        &self,
        max_age_for_non_stale_branches: Duration,
        max_age_for_non_stale_tags: Duration,
    ) -> Result<(), Vec<String>> {
        let dirty_repositories = self
            .store
            .get_dirty_repositories()
            .await
            .map_err(|err| vec![format!("uploadSvc.DirtyRepositories: {err}")])?;

        let mut errors = Vec::new();
        for dirty_repository in dirty_repositories {
            if let Err(err) = self
                .lock_and_update_uploads_visible_to_commits(
                    &dirty_repository,
                    max_age_for_non_stale_branches,
                    max_age_for_non_stale_tags,
                )
                .await
            {
                errors.push(err);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// `lockAndUpdateUploadsVisibleToCommits`.
    async fn lock_and_update_uploads_visible_to_commits(
        &self,
        dirty_repository: &DirtyRepository,
        max_age_for_non_stale_branches: Duration,
        max_age_for_non_stale_tags: Duration,
    ) -> Result<(), String> {
        let guard = match self
            .locker
            .lock(dirty_repository.repository_id, false)
            .await
        {
            Ok(Some(guard)) => guard,
            // `errors.Wrap(nil, "locker.Lock")` is nil: a lock held by another
            // process is not an error.
            Ok(None) => return Ok(()),
            Err(err) => return Err(format!("locker.Lock: {err}")),
        };

        let result = self
            .update_uploads_visible_to_commits(
                dirty_repository,
                max_age_for_non_stale_branches,
                max_age_for_non_stale_tags,
            )
            .await;
        let error = result.err();
        guard.unlock(error).await.map_or(Ok(()), Err)
    }

    /// The locked body: fetch the graph, list refs, hand both to the store.
    async fn update_uploads_visible_to_commits(
        &self,
        dirty_repository: &DirtyRepository,
        max_age_for_non_stale_branches: Duration,
        max_age_for_non_stale_tags: Duration,
    ) -> Result<(), String> {
        let commit_graph = self
            .get_commit_graph(
                dirty_repository.repository_id,
                &dirty_repository.repository_name,
            )
            .await?;

        let refs = self
            .gitserver
            .list_refs(&dirty_repository.repository_name, true, true)
            .await
            .map_err(|err| format!("gitserver.ListRefs: {err}"))?;

        self.store
            .update_uploads_visible_to_commits(
                dirty_repository.repository_id,
                &commit_graph,
                &map_refs_to_commits(&refs),
                max_age_for_non_stale_branches,
                max_age_for_non_stale_tags,
                dirty_repository.dirty_token,
                None,
            )
            .await
            .map_err(|err| format!("uploadSvc.UpdateUploadsVisibleToCommits: {err}"))
    }

    /// `commitGraphUpdater.getCommitGraph`.
    async fn get_commit_graph(
        &self,
        repository_id: i64,
        repo: &str,
    ) -> Result<CommitGraph, String> {
        let oldest_upload = self
            .store
            .get_commit_and_date_for_oldest_upload(repository_id)
            .await?;
        let Some(commit_with_date) = oldest_upload else {
            return Ok(commitgraph::parse_commit_graph(&[]));
        };

        let mut strategy = CommitGraphRefreshStrategy::AllRefsSince;
        let mut default_branch_ref = String::new();

        if matches_default_branch_only(&self.config.default_branch_only(), repo) {
            if let Ok((ref_name, _commit)) = self.gitserver.get_default_branch(repo, false).await {
                default_branch_ref = ref_name;
                strategy = CommitGraphRefreshStrategy::HeadTopoOnly;
            }
        }

        let options = match strategy {
            CommitGraphRefreshStrategy::HeadTopoOnly => CommitsOptions {
                ranges: vec![format!(
                    "{}..{}",
                    commit_with_date.commit, default_branch_ref
                )],
                order: Some(CommitsOrder::TopoDate),
                ..CommitsOptions::default()
            },
            CommitGraphRefreshStrategy::AllRefsSince => CommitsOptions {
                all_refs: true,
                order: Some(CommitsOrder::TopoDate),
                // `--since` is exclusive; subtract one second so the commit
                // where the oldest upload is defined is included.
                after: Some(Timestamp {
                    seconds: commit_with_date.committer_date.seconds - 1,
                    nanos: commit_with_date.committer_date.nanos,
                }),
                ..CommitsOptions::default()
            },
        };

        let commits = self
            .gitserver
            .commits(repo, &options)
            .await
            .map_err(|err| format!("gitserver.Commits: {err}"))?;

        let nodes: Vec<commitgraph::CommitNode> = commits
            .iter()
            .map(|commit| commitgraph::CommitNode {
                commit: commit.oid.clone(),
                parents: commit.parents.clone(),
            })
            .collect();

        Ok(commitgraph::parse_commit_graph(&nodes))
    }
}

/// `cmd/worker/internal/codeintel.NewCommitGraphUpdaterJob`.
#[derive(Debug, Default)]
pub struct CodeIntelCommitGraphUpdater {
    config: Arc<CommitGraphConfig>,
}

impl CodeIntelCommitGraphUpdater {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(&self) -> &CommitGraphConfig {
        &self.config
    }
}

#[async_trait]
impl Job for CodeIntelCommitGraphUpdater {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![self.config.clone()]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let unified = deps.live_config.current();
        let gitserver = Arc::new(GitserverAdapter {
            client: Arc::new(sg_git::GitserverClient::new(
                unified.service_connections.git_servers.clone(),
            )),
        });
        let store = Arc::new(PgStore {
            frontend: deps.frontend.clone(),
        });
        let locker = Arc::new(PgLocker {
            frontend: deps.frontend.clone(),
        });
        let config = Arc::new(LiveConfigSource::new(deps.live_config.clone()));

        let updater = Arc::new(CommitGraphUpdater::new(store, locker, gitserver, config));
        let routine = PeriodicRoutine::new(Arc::new(UpdaterHandler {
            updater,
            max_age_for_non_stale_branches: self.config.max_age_for_non_stale_branches(),
            max_age_for_non_stale_tags: self.config.max_age_for_non_stale_tags(),
        }))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(self.config.interval());

        Ok(vec![routine.into()])
    }
}

/// `goroutine.HandlerFunc` around `UpdateAllDirtyCommitGraphs`.
struct UpdaterHandler {
    updater: Arc<CommitGraphUpdater>,
    max_age_for_non_stale_branches: Duration,
    max_age_for_non_stale_tags: Duration,
}

#[async_trait]
impl Handler for UpdaterHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        self.updater
            .update_all_dirty_commit_graphs(
                self.max_age_for_non_stale_branches,
                self.max_age_for_non_stale_tags,
            )
            .await
            .map_err(|errors| HandlerError::new(errors.join("\n")))
    }
}

/// Production `gitserver.Client` adapter over [`sg_git::GitserverClient`].
struct GitserverAdapter {
    client: Arc<sg_git::GitserverClient>,
}

#[async_trait]
impl CommitGraphGitserver for GitserverAdapter {
    async fn list_refs(
        &self,
        repo: &str,
        heads_only: bool,
        tags_only: bool,
    ) -> Result<Vec<GitRef>, String> {
        self.client
            .list_refs(repo, heads_only, tags_only, &[], None)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_default_branch(
        &self,
        repo: &str,
        short: bool,
    ) -> Result<(String, String), String> {
        let (ref_name, commit) = self
            .client
            .default_branch_commit(repo)
            .await
            .map_err(|err| err.to_string())?;
        let ref_name = if short {
            ref_name
                .strip_prefix("refs/heads/")
                .map(str::to_owned)
                .unwrap_or(ref_name)
        } else {
            ref_name
        };
        Ok((ref_name, commit))
    }

    async fn commits(&self, repo: &str, options: &CommitsOptions) -> Result<Vec<Commit>, String> {
        self.client
            .commits(repo, options)
            .await
            .map_err(|err| err.to_string())
    }
}

/// Production store adapter. The read paths are ported; the graph write path
/// (`UpdateUploadsVisibleToCommits`) is not (see §4 of the slice overview).
struct PgStore {
    frontend: PgPool,
}

#[async_trait]
impl CommitGraphStore for PgStore {
    async fn get_dirty_repositories(&self) -> Result<Vec<DirtyRepository>, String> {
        let rows = sqlx::query(
            "SELECT ldr.repository_id, repo.name, ldr.dirty_token
               FROM lsif_dirty_repositories ldr
                 INNER JOIN repo ON repo.id = ldr.repository_id
               WHERE ldr.dirty_token > ldr.update_token
                 AND repo.deleted_at IS NULL
                 AND repo.blocked IS NULL",
        )
        .fetch_all(&self.frontend)
        .await
        .map_err(|err| err.to_string())?;

        rows.into_iter()
            .map(|row| {
                Ok(DirtyRepository {
                    repository_id: i64::from(
                        row.try_get::<i32, _>("repository_id")
                            .map_err(|err| err.to_string())?,
                    ),
                    repository_name: row.try_get("name").map_err(|err| err.to_string())?,
                    dirty_token: i64::from(
                        row.try_get::<i32, _>("dirty_token")
                            .map_err(|err| err.to_string())?,
                    ),
                })
            })
            .collect()
    }

    async fn get_commit_and_date_for_oldest_upload(
        &self,
        repository_id: i64,
    ) -> Result<Option<CommitWithDate>, String> {
        let row = sqlx::query(
            "SELECT
                u.commit,
                cd.committed_at
             FROM lsif_uploads u
             LEFT JOIN codeintel_commit_dates cd
               ON cd.repository_id = u.repository_id
              AND cd.commit_bytea = decode(u.commit, 'hex')
             WHERE
                u.repository_id = $1 AND
                u.state = 'completed' AND
                (cd.committed_at != '-infinity' OR cd.committed_at IS NULL)
             ORDER BY cd.committed_at NULLS FIRST
             LIMIT 1",
        )
        .bind(repository_id as i32)
        .fetch_optional(&self.frontend)
        .await
        .map_err(|err| err.to_string())?;

        let Some(row) = row else {
            return Ok(None);
        };

        let commit: String = row.try_get("commit").map_err(|err| err.to_string())?;
        let committed_at: Option<sqlx::types::time::OffsetDateTime> =
            row.try_get("committed_at").map_err(|err| err.to_string())?;
        let Some(committed_at) = committed_at else {
            return Err(format!(
                "repository {repository_id} has not yet completed its backfill of commit dates"
            ));
        };

        Ok(Some(CommitWithDate {
            commit,
            committer_date: Timestamp {
                seconds: committed_at.unix_timestamp(),
                nanos: committed_at.nanosecond() as i32,
            },
        }))
    }

    async fn update_uploads_visible_to_commits(
        &self,
        _repository_id: i64,
        _commit_graph: &CommitGraph,
        _refs: &RefsByCommit,
        _max_age_for_non_stale_branches: Duration,
        _max_age_for_non_stale_tags: Duration,
        _dirty_token: i64,
        _now: Option<Timestamp>,
    ) -> Result<(), String> {
        Err("store.UpdateUploadsVisibleToCommits is not ported to the Rust worker yet".to_owned())
    }
}

/// `locker.StringKey("codeintel")`: FNV-1 32-bit modulo `math.MaxInt32`.
const fn fnv1_hash_string32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 2_166_136_261;
    let mut index = 0;
    while index < bytes.len() {
        hash = hash.wrapping_mul(16_777_619);
        hash ^= bytes[index] as u32;
        index += 1;
    }
    hash
}

pub const LOCKER_NAMESPACE: i32 = (fnv1_hash_string32(b"codeintel") % 2_147_483_647) as i32;

/// Production advisory locker: a transaction holds the lock until `unlock`.
///
/// The baseline's `locker.Lock` wraps a transaction and maps `UnlockFunc` to
/// `tx.Done`: commit on success, rollback on an error. Rollback errors are
/// folded into a `MultiError`; this port keeps the handler error (the rollback
/// error is not surfaced), which is a documentation-level difference only.
struct PgLocker {
    frontend: PgPool,
}

#[async_trait]
impl CommitGraphLocker for PgLocker {
    async fn lock(&self, key: i64, blocking: bool) -> Result<Option<Box<dyn LockGuard>>, String> {
        let mut transaction = self.frontend.begin().await.map_err(|err| err.to_string())?;

        let locked: bool = if blocking {
            sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
                .bind(LOCKER_NAMESPACE)
                .bind(key as i32)
                .execute(&mut *transaction)
                .await
                .map_err(|err| err.to_string())?;
            true
        } else {
            sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1, $2)")
                .bind(LOCKER_NAMESPACE)
                .bind(key as i32)
                .fetch_one(&mut *transaction)
                .await
                .map_err(|err| err.to_string())?
        };

        if !locked {
            transaction
                .rollback()
                .await
                .map_err(|err| err.to_string())?;
            return Ok(None);
        }

        Ok(Some(Box::new(PgLockGuard {
            transaction: Some(transaction),
        })))
    }
}

struct PgLockGuard {
    transaction: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

#[async_trait]
impl LockGuard for PgLockGuard {
    async fn unlock(mut self: Box<Self>, error: Option<String>) -> Option<String> {
        let transaction = self
            .transaction
            .take()
            .expect("lock guard transaction is taken once");
        match error {
            None => transaction.commit().await.err().map(|err| err.to_string()),
            Some(error) => {
                let _ = transaction.rollback().await;
                Some(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::PoisonError;

    /// Tests that mutate process environment variables must not overlap.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = CodeIntelCommitGraphUpdater::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = CommitGraphConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.interval(), Duration::from_secs(10));
        assert_eq!(
            config.max_age_for_non_stale_branches(),
            Duration::from_secs(2160 * 60 * 60)
        );
        assert_eq!(
            config.max_age_for_non_stale_tags(),
            Duration::from_secs(8760 * 60 * 60)
        );
        assert_eq!(
            config.commit_graph_update_task_interval(),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn config_rejects_malformed_values() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = CommitGraphConfig::default();
        let mut env = EnvReader::new();
        std::env::set_var(INTERVAL_VAR, "not-a-duration");
        std::env::set_var("PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_BRANCHES", "nope");
        config.load(&mut env);
        std::env::remove_var(INTERVAL_VAR);
        std::env::remove_var("PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_BRANCHES");
        assert!(config.validate().is_err());
    }

    #[test]
    fn fallback_variable_names_are_ordered() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(
            choose_fallback(MAX_AGE_FOR_NON_STALE_TAGS_VARS),
            "CODEINTEL_UPLOAD_COMMITGRAPH_MAX_AGE_FOR_NON_STALE_TAGS"
        );
        std::env::set_var("PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_TAGS", "1h");
        assert_eq!(
            choose_fallback(MAX_AGE_FOR_NON_STALE_TAGS_VARS),
            "PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_TAGS"
        );
        std::env::remove_var("PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_TAGS");
    }

    #[test]
    fn map_refs_to_commits_keeps_ref_order() {
        let git_ref = |name: &str, commit: &str| GitRef {
            name: name.to_owned(),
            short_name: String::new(),
            target_commit: commit.to_owned(),
            ref_oid: commit.to_owned(),
            created_at: None,
            ref_type: sg_git::RefType::Branch,
            is_head: false,
        };
        let refs = vec![
            git_ref("refs/heads/main", "c1"),
            git_ref("refs/tags/v1", "c1"),
            git_ref("refs/heads/dev", "c2"),
        ];
        let mapped = map_refs_to_commits(&refs);
        assert_eq!(mapped["c1"].len(), 2);
        assert_eq!(mapped["c1"][0].name, "refs/heads/main");
        assert_eq!(mapped["c1"][1].name, "refs/tags/v1");
        assert_eq!(mapped["c2"][0].name, "refs/heads/dev");
    }

    #[test]
    fn default_branch_only_reads_the_site_config_shape() {
        let site = SiteConfiguration::parse(
            r#"{"experimentalFeatures":{"commitGraphUpdates":{"defaultBranchOnly":["github.com/acme/.*"]}}}"#,
        )
        .expect("parse");
        assert_eq!(
            default_branch_only_from_site(&site),
            vec!["github.com/acme/.*".to_owned()]
        );
        assert!(default_branch_only_from_site(&SiteConfiguration::default()).is_empty());
    }

    #[test]
    fn locker_namespace_matches_fnv1() {
        assert_eq!(LOCKER_NAMESPACE, 1_285_144_674);
    }
}
