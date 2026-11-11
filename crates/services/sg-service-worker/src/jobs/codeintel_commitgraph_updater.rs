#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/uploads_commitgraph.go`: the
//! `codeintel-commitgraph-updater` job.
//!
//! The job owns `uploads.CommitGraphConfigInst` (`commitgraph.Config`) and
//! returns the single routine built by `uploads.NewCommitGraphUpdater`:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.commitgraph-updater` | `CODEINTEL_UPLOAD_COMMITGRAPH_UPDATER_INTERVAL` (10s) | recompute the visibility commit graph for dirty repositories |
//!
//! Each invocation lists the dirty repositories, and for every one:
//!
//! 1. takes the per-repository Postgres advisory lock in the `codeintel`
//!    locker namespace (non-blocking; a repository locked by another process
//!    is skipped);
//! 2. resolves the oldest completed upload's commit date
//!    (`GetCommitAndDateForOldestUpload`) and pulls that repository's recent
//!    commit history from gitserver — either every ref since that date
//!    (`AllRefsSince`), or, for repositories matched by
//!    `experimentalFeatures.commitGraphUpdates.defaultBranchOnly`, only the
//!    default branch (`HeadTopoOnly`);
//! 3. parses the history into a commit graph, lists the branch/tag refs, and
//!    calls `UpdateUploadsVisibleToCommits`.
//!
//! Errors for one repository do not stop the others; they are accumulated
//! like `errors.Append` and returned at the end.
//!
//! Not ported: the `observation` spans / RED metrics (metrics slice) and the
//! `actor.WithInternalActor` context (the SQL has no authz condition; see
//! `sg_db::codeintel_commitgraph`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_codeintel::commitgraph::{parse_commit_graph, CommitGraph, CommitNode};
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_commitgraph::{self, RefInfo, RefType as DbRefType};
use sg_git::{CommitsOptions, CommitsOrder, GitserverClient, RefType as GitRefType};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `uploads.CommitGraphConfigInst`'s routine metadata.
pub const ROUTINE_NAME: &str = "codeintel.commitgraph-updater";
pub const ROUTINE_DESCRIPTION: &str = "updates the visibility commit graph for dirty repos";

/// `commitgraph.Config` environment variables.
pub const INTERVAL_VAR: &str = "CODEINTEL_UPLOAD_COMMITGRAPH_UPDATER_INTERVAL";
const INTERVAL_DESCRIPTION: &str = "How frequently to run the upload commitgraph updater routine.";
pub const MAX_AGE_FOR_NON_STALE_BRANCHES_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_COMMITGRAPH_MAX_AGE_FOR_NON_STALE_BRANCHES",
    "PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_BRANCHES",
];
const MAX_AGE_FOR_NON_STALE_BRANCHES_DESCRIPTION: &str =
    "The age after which a branch should be considered stale. Code intelligence indexes will be evicted from stale branches.";
pub const MAX_AGE_FOR_NON_STALE_TAGS_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_COMMITGRAPH_MAX_AGE_FOR_NON_STALE_TAGS",
    "PRECISE_CODE_INTEL_MAX_AGE_FOR_NON_STALE_TAGS",
];
const MAX_AGE_FOR_NON_STALE_TAGS_DESCRIPTION: &str =
    "The age after which a tagged commit should be considered stale. Code intelligence indexes will be evicted from stale tagged commits.";
pub const COMMIT_GRAPH_UPDATE_TASK_INTERVAL_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_COMMITGRAPH_UPDATE_TASK_INTERVAL",
    "PRECISE_CODE_INTEL_COMMIT_GRAPH_UPDATE_TASK_INTERVAL",
];
const COMMIT_GRAPH_UPDATE_TASK_INTERVAL_DESCRIPTION: &str =
    "The frequency with which to run periodic codeintel commit graph update tasks.";

/// `commitgraph.Config` (`uploads.CommitGraphConfigInst`).
#[derive(Debug, Default)]
pub struct CommitGraphConfig {
    interval: std::sync::Mutex<Duration>,
    max_age_for_non_stale_branches: std::sync::Mutex<Duration>,
    max_age_for_non_stale_tags: std::sync::Mutex<Duration>,
    commit_graph_update_task_interval: std::sync::Mutex<Duration>,
    errs: std::sync::Mutex<Vec<String>>,
}

impl CommitGraphConfig {
    pub fn interval(&self) -> Duration {
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn max_age_for_non_stale_branches(&self) -> Duration {
        *self
            .max_age_for_non_stale_branches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn max_age_for_non_stale_tags(&self) -> Duration {
        *self
            .max_age_for_non_stale_tags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Loaded and validated because the baseline config declares it; the
    /// commit graph updater itself only reads the interval and the two
    /// maximum ages.
    pub fn commit_graph_update_task_interval(&self) -> Duration {
        *self
            .commit_graph_update_task_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobConfig for CommitGraphConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        let max_age_for_non_stale_branches_name =
            choose_fallback(&MAX_AGE_FOR_NON_STALE_BRANCHES_VARS);
        let max_age_for_non_stale_tags_name = choose_fallback(&MAX_AGE_FOR_NON_STALE_TAGS_VARS);
        let commit_graph_update_task_interval_name =
            choose_fallback(&COMMIT_GRAPH_UPDATE_TASK_INTERVAL_VARS);

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
            &max_age_for_non_stale_branches_name,
            "2160h",
            MAX_AGE_FOR_NON_STALE_BRANCHES_DESCRIPTION,
            &mut errs,
        );
        *self
            .max_age_for_non_stale_tags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            &max_age_for_non_stale_tags_name,
            "8760h",
            MAX_AGE_FOR_NON_STALE_TAGS_DESCRIPTION,
            &mut errs,
        );
        *self
            .commit_graph_update_task_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_interval(
            env,
            &commit_graph_update_task_interval_name,
            "10s",
            COMMIT_GRAPH_UPDATE_TASK_INTERVAL_DESCRIPTION,
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

/// `env.ChooseFallbackVariableName`: the first name whose environment
/// variable is non-empty; otherwise the first name.
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

/// `cmd/worker/internal/codeintel.NewCommitGraphUpdaterJob`.
#[derive(Debug, Default)]
pub struct CommitGraphUpdater {
    config: Arc<CommitGraphConfig>,
}

impl CommitGraphUpdater {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(&self) -> &CommitGraphConfig {
        &self.config
    }
}

#[async_trait]
impl Job for CommitGraphUpdater {
    fn description(&self) -> String {
        String::new()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![self.config.clone()]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        // `codeintel.InitServices`' `GitserverClient` is built from
        // `ServiceConnections.GitServers`.
        let unified = deps.live_config.current();
        let git = Arc::new(GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));

        let routine = PeriodicRoutine::new(Arc::new(CommitGraphHandler {
            frontend: deps.frontend.clone(),
            gitserver: git,
            live_config: deps.live_config.clone(),
            max_age_for_non_stale_branches: std::time::Duration::from_secs(
                self.config.max_age_for_non_stale_branches().as_secs(),
            ),
            max_age_for_non_stale_tags: std::time::Duration::from_secs(
                self.config.max_age_for_non_stale_tags().as_secs(),
            ),
        }))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(self.config.interval());

        Ok(vec![routine.into()])
    }
}

/// `commitGraphUpdater`'s periodic handler.
struct CommitGraphHandler {
    frontend: PgPool,
    gitserver: Arc<GitserverClient>,
    live_config: Arc<LiveConfig>,
    max_age_for_non_stale_branches: std::time::Duration,
    max_age_for_non_stale_tags: std::time::Duration,
}

#[async_trait]
impl Handler for CommitGraphHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let site = self.live_config.current().site.clone();
        update_all_dirty_commit_graphs(
            &self.frontend,
            &self.gitserver,
            &site,
            time_duration(self.max_age_for_non_stale_branches),
            time_duration(self.max_age_for_non_stale_tags),
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// Convert the `std::time::Duration` values the config holds (Go's
/// `time.Duration`) into signed `time::Duration` for `sg-db`.
fn time_duration(duration: std::time::Duration) -> time::Duration {
    time::Duration::seconds(i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
}

/// `UpdateAllDirtyCommitGraphs`.
pub async fn update_all_dirty_commit_graphs(
    pool: &PgPool,
    gitserver: &GitserverClient,
    site: &SiteConfiguration,
    max_age_for_non_stale_branches: time::Duration,
    max_age_for_non_stale_tags: time::Duration,
) -> Result<(), String> {
    let dirty_repositories = codeintel_commitgraph::get_dirty_repositories(pool)
        .await
        .map_err(|err| format!("uploadSvc.DirtyRepositories: {err}"))?;

    let mut update_err: Option<String> = None;
    for dirty_repository in dirty_repositories {
        if let Err(err) = lock_and_update_uploads_visible_to_commits(
            pool,
            gitserver,
            site,
            dirty_repository.repository_id,
            &dirty_repository.repository_name,
            dirty_repository.dirty_token,
            max_age_for_non_stale_branches,
            max_age_for_non_stale_tags,
        )
        .await
        {
            update_err = Some(match update_err {
                None => err,
                // `errors.Append` joins the messages with newlines.
                Some(previous) => format!("{previous}\n{err}"),
            });
        }
    }

    match update_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// `lockAndUpdateUploadsVisibleToCommits`: hold the `codeintel` advisory lock
/// for the repository for the duration of the update.
#[allow(clippy::too_many_arguments)]
async fn lock_and_update_uploads_visible_to_commits(
    pool: &PgPool,
    gitserver: &GitserverClient,
    site: &SiteConfiguration,
    repository_id: i32,
    repository_name: &str,
    dirty_token: i32,
    max_age_for_non_stale_branches: time::Duration,
    max_age_for_non_stale_tags: time::Duration,
) -> Result<(), String> {
    // `locker.NewWith(store.Handle(), "codeintel")`.
    let namespace = string_key("codeintel");
    let mut lock_tx = pool
        .begin()
        .await
        .map_err(|err| format!("locker.Lock: {err}"))?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1, $2)")
        .bind(namespace)
        .bind(repository_id)
        .fetch_one(&mut *lock_tx)
        .await
        .map_err(|err| format!("locker.Lock: {err}"))?;
    if !locked {
        // The baseline commits the empty lock transaction.
        let _ = lock_tx.commit().await;
        return Ok(());
    }

    let result = update_while_locked(
        pool,
        gitserver,
        site,
        repository_id,
        repository_name,
        dirty_token,
        max_age_for_non_stale_branches,
        max_age_for_non_stale_tags,
    )
    .await;

    match result {
        Ok(()) => lock_tx
            .commit()
            .await
            .map_err(|err| format!("locker.Lock: {err}")),
        Err(err) => {
            // `unlock(err)` rolls the lock transaction back and returns the
            // original error.
            let _ = lock_tx.rollback().await;
            Err(err)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn update_while_locked(
    pool: &PgPool,
    gitserver: &GitserverClient,
    site: &SiteConfiguration,
    repository_id: i32,
    repository_name: &str,
    dirty_token: i32,
    max_age_for_non_stale_branches: time::Duration,
    max_age_for_non_stale_tags: time::Duration,
) -> Result<(), String> {
    // Construct a view of the git graph that we will later decorate with
    // upload information.
    let commit_graph =
        get_commit_graph(pool, gitserver, site, repository_id, repository_name).await?;

    let refs = gitserver
        .list_refs(repository_name, true, true, &[], None)
        .await
        .map_err(|err| format!("gitserver.ListRefs: {err}"))?;

    codeintel_commitgraph::update_uploads_visible_to_commits(
        pool,
        repository_id,
        &commit_graph,
        &map_refs_to_commits(refs),
        max_age_for_non_stale_branches,
        max_age_for_non_stale_tags,
        dirty_token,
        None,
    )
    .await
    .map_err(|err| format!("uploadSvc.UpdateUploadsVisibleToCommits: {err}"))?;

    Ok(())
}

/// `mapRefsToCommits`: index a set of refs by commit ID.
fn map_refs_to_commits(refs: Vec<sg_git::GitRef>) -> HashMap<String, Vec<RefInfo>> {
    let mut commits_by_ref: HashMap<String, Vec<RefInfo>> = HashMap::with_capacity(refs.len());
    for git_ref in refs {
        commits_by_ref
            .entry(git_ref.target_commit)
            .or_default()
            .push(RefInfo {
                short_name: git_ref.short_name,
                ref_type: match git_ref.ref_type {
                    GitRefType::Branch => DbRefType::Branch,
                    GitRefType::Tag => DbRefType::Tag,
                    GitRefType::Unknown => DbRefType::Unknown,
                },
                created_at: git_ref.created_at.and_then(|timestamp| {
                    OffsetDateTime::from_unix_timestamp(timestamp.seconds)
                        .ok()
                        .map(|date| date + time::Duration::nanoseconds(i64::from(timestamp.nanos)))
                }),
                is_head: git_ref.is_head,
            });
    }
    commits_by_ref
}

/// `CommitGraphRefreshStrategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitGraphRefreshStrategy {
    HeadTopoOnly,
    AllRefsSince,
}

/// `getCommitGraph`: the recent commits of every branch, extending back to
/// the oldest completed upload's commit date (or only the default branch
/// when site configuration opts the repository in).
async fn get_commit_graph(
    pool: &PgPool,
    gitserver: &GitserverClient,
    site: &SiteConfiguration,
    repository_id: i32,
    repository_name: &str,
) -> Result<CommitGraph, String> {
    let oldest = codeintel_commitgraph::get_commit_and_date_for_oldest_upload(pool, repository_id)
        .await
        .map_err(|err| err.to_string())?;
    let Some(commit_with_date) = oldest else {
        return Ok(parse_commit_graph(&[]));
    };

    let patterns = site.commit_graph_updates_default_branch_only();
    let mut strategy = CommitGraphRefreshStrategy::AllRefsSince;
    let mut default_branch_ref = String::new();
    let patterns_match = patterns.iter().any(|pattern| {
        regex::Regex::new(pattern)
            .map(|regex| regex.is_match(repository_name))
            .unwrap_or(false)
    });
    if patterns_match {
        if let Ok(ref_name) = gitserver.default_branch(repository_name).await {
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
            // `--since` is exclusive, but the commit defining the oldest
            // upload must be included. The flag has second resolution.
            after: Some(timestamp_minus_one_second(commit_with_date.committer_date)),
            ..CommitsOptions::default()
        },
    };

    let commits = gitserver
        .commits(repository_name, &options)
        .await
        .map_err(|err| format!("gitserver.Commits: {err}"))?;

    let nodes: Vec<CommitNode> = commits
        .into_iter()
        .map(|commit| CommitNode {
            id: commit.oid,
            parents: commit.parents,
        })
        .collect();

    Ok(parse_commit_graph(&nodes))
}

/// `timestamppb.New(commitWithDate.CommitterDate.Add(-time.Second))`.
fn timestamp_minus_one_second(date: OffsetDateTime) -> prost_types::Timestamp {
    let date = date - time::Duration::seconds(1);
    prost_types::Timestamp {
        seconds: date.unix_timestamp(),
        nanos: i32::try_from(date.nanosecond()).unwrap_or(0),
    }
}

/// `locker.StringKey`: `int32(fnv1.HashString32(s) % math.MaxInt32)`.
///
/// `segmentio/fasthash/fnv1.HashString32` is 32-bit FNV-1 (multiply, then
/// XOR each byte).
fn string_key(value: &str) -> i32 {
    let mut hash: u32 = 2_166_136_261;
    for byte in value.bytes() {
        hash = hash.wrapping_mul(16_777_619);
        hash ^= u32::from(byte);
    }
    (hash % (i32::MAX as u32)) as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// The config tests mutate process environment variables; cargo runs
    /// tests in parallel threads, so they must not overlap.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = CommitGraphUpdater::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let config = CommitGraphConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.interval(), Duration::from_secs(10));
        assert_eq!(
            config.max_age_for_non_stale_branches(),
            Duration::from_secs(2160 * 3600)
        );
        assert_eq!(
            config.max_age_for_non_stale_tags(),
            Duration::from_secs(8760 * 3600)
        );
        assert_eq!(
            config.commit_graph_update_task_interval(),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn string_key_matches_go_fnv1() {
        // Explicit FNV-1 over "codeintel": multiply then XOR.
        assert_eq!(string_key("codeintel"), 1_285_144_674);
    }

    #[test]
    fn refs_are_indexed_by_commit() {
        let refs = vec![
            sg_git::GitRef {
                name: "refs/heads/main".to_owned(),
                short_name: "main".to_owned(),
                target_commit: "c1".to_owned(),
                ref_oid: "c1".to_owned(),
                created_at: Some(prost_types::Timestamp {
                    seconds: 1_600_000_000,
                    nanos: 0,
                }),
                ref_type: GitRefType::Branch,
                is_head: true,
            },
            sg_git::GitRef {
                name: "refs/tags/v1".to_owned(),
                short_name: "v1".to_owned(),
                target_commit: "c1".to_owned(),
                ref_oid: "t1".to_owned(),
                created_at: None,
                ref_type: GitRefType::Tag,
                is_head: false,
            },
        ];

        let by_commit = map_refs_to_commits(refs);
        let refs = by_commit.get("c1").expect("c1");
        assert_eq!(refs.len(), 2);
        assert!(refs[0].is_head);
        assert_eq!(refs[0].ref_type, DbRefType::Branch);
        assert!(refs[0].created_at.is_some());
        assert_eq!(refs[1].ref_type, DbRefType::Tag);
        assert!(refs[1].created_at.is_none());
    }
}
