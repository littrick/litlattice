#![forbid(unsafe_code)]

//! `cmd/worker/internal/perforce` + `internal/perforce`:
//! the `perforce-changelist-mapper` job.
//!
//! A single `PeriodicGoroutine` (`SRC_PERFORCE_CHANGELIST_MAPPER_INTERVAL`,
//! default 1m) walks the Perforce repositories in batches
//! (`SRC_PERFORCE_CHANGELIST_MAPPER_REPO_BATCH_SIZE`, default 100) and indexes
//! the changelist ids embedded in imported commit messages into
//! `repo_commits_changelists`:
//!
//! 1. `experimentalFeatures.perforceChangelistMapping` must be `enabled`;
//! 2. `Repos.ListMinimalRepos(ExternalServiceType=perforce, MinLastChanged,
//!    LimitOffset)` selects the batch on every page; after the first page
//!    `MinLastChanged` is the time the page started;
//! 3. per repository, `GetLatestForRepo` yields the newest mapped changelist
//!    row (or none for a new repository, in which case the whole history is
//!    walked); `GetDefaultBranch` supplies HEAD and the walk stops when HEAD
//!    is already mapped;
//! 4. `extractChangelistsFromCommits` pages `Commits` (50 per page, either
//!    `HEAD` or `<last>..<head>`) and parses every message with the
//!    git-p4/p4-fusion trailer;
//! 5. the mappings are inserted in one transaction (the unique
//!    `(repo_id, perforce_changelist_id)` constraint makes a partially
//!    successful batch impossible).
//!
//! Not ported: the RED metrics/observation operation and the debug/info logs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_core::{ConfigError, EnvReader};
use sg_git::{CommitsOptions, GitserverClient};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `perforceChangelistMappingJob.Description`.
pub const JOB_DESCRIPTION: &str =
    "Background job indexing mapping data from Perforce changelist numbers to imported Git commits";

/// `perforceChangelistMapper`'s `goroutine.WithName`/`WithDescription`.
pub const ROUTINE_NAME: &str = "perforce-changelist-mapper";
pub const ROUTINE_DESCRIPTION: &str =
    "Background job indexing mapping data from Perforce changelist numbers to imported Git commits";

/// `Config.Interval` / `Config.RepositoryBatchSize`.
pub const INTERVAL_VAR: &str = "SRC_PERFORCE_CHANGELIST_MAPPER_INTERVAL";
pub const REPOSITORY_BATCH_SIZE_VAR: &str = "SRC_PERFORCE_CHANGELIST_MAPPER_REPO_BATCH_SIZE";
const INTERVAL_DESCRIPTION: &str = "How frequently to run the Perforce changelist mapper routine.";
const REPOSITORY_BATCH_SIZE_DESCRIPTION: &str =
    "The number of repositories to load at once when processing for Perforce changelist mappings.";

/// `extsvc.TypePerforce`.
pub const PERFORCE_SERVICE_TYPE: &str = "perforce";

/// `config.Interval` and `config.RepositoryBatchSize`.
#[derive(Debug, Default)]
pub struct MapperConfig {
    interval: Mutex<Duration>,
    repository_batch_size: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl MapperConfig {
    pub fn interval(&self) -> Duration {
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn repository_batch_size(&self) -> i64 {
        *self
            .repository_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobConfig for MapperConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            get_interval(env, INTERVAL_VAR, "1m", INTERVAL_DESCRIPTION, &mut errs);
        *self
            .repository_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = get_int(
            env,
            REPOSITORY_BATCH_SIZE_VAR,
            "100",
            REPOSITORY_BATCH_SIZE_DESCRIPTION,
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

/// `env.GetInt`.
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match raw.parse::<i64>() {
        Ok(value) => value,
        Err(_) => {
            errors.push(format!(
                "parsing environment variable {name:?}. Expected valid integer, got {raw:?}"
            ));
            0
        }
    }
}

/// `perforceChangelistMapper.GitserverClient`: the two gitserver calls the
/// mapper makes. The production implementation is [`SgGitClient`].
#[async_trait]
pub trait PerforceGitserver: Send + Sync {
    /// `GetDefaultBranch(repo, false)` → `(refName, commitID)`.
    async fn default_branch(&self, repo: &str) -> Result<(String, String), String>;

    /// `Commits(repo, CommitsOptions{Ranges, N, Skip})`.
    async fn commits(
        &self,
        repo: &str,
        ranges: &[String],
        n: usize,
        skip: usize,
    ) -> Result<Vec<MappingCommit>, String>;
}

/// `gitdomain.Commit` fields the mapper reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingCommit {
    pub id: String,
    pub message: String,
}

/// Production adapter over [`GitserverClient`].
pub struct SgGitClient {
    client: GitserverClient,
}

impl SgGitClient {
    pub fn new(client: GitserverClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl PerforceGitserver for SgGitClient {
    async fn default_branch(&self, repo: &str) -> Result<(String, String), String> {
        self.client
            .default_branch_commit(repo)
            .await
            .map_err(|err| err.to_string())
    }

    async fn commits(
        &self,
        repo: &str,
        ranges: &[String],
        n: usize,
        skip: usize,
    ) -> Result<Vec<MappingCommit>, String> {
        let options = CommitsOptions {
            ranges: ranges.to_vec(),
            max_commits: u32::try_from(n).unwrap_or(u32::MAX),
            skip: u32::try_from(skip).unwrap_or(u32::MAX),
            ..CommitsOptions::default()
        };
        self.client
            .commits(repo, &options)
            .await
            .map_err(|err| err.to_string())
            .map(|commits| {
                commits
                    .into_iter()
                    .map(|commit| MappingCommit {
                        id: commit.oid,
                        message: commit.message,
                    })
                    .collect()
            })
    }
}

/// `types.MinimalRepo` subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinimalRepo {
    pub id: i32,
    pub name: String,
}

/// `types.PerforceChangelist`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerforceChangelist {
    pub commit_sha: String,
    pub changelist_id: i64,
}

/// `types.RepoCommit` subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoCommit {
    pub commit_sha: String,
}

/// `perforceChangelistMapper.Handle`.
pub async fn perforce_changelist_mapper(
    frontend: &PgPool,
    gitserver: &dyn PerforceGitserver,
    live_config: &LiveConfig,
    batch_size: i64,
) -> Result<(), String> {
    let site = live_config.current().site.clone();
    if !site.perforce_changelist_mapping_enabled() {
        return Ok(());
    }

    // `lastCheck` starts zero, which omits the MinLastChanged predicate.
    let mut last_check: Option<OffsetDateTime> = None;
    let mut page: i64 = 0;
    let mut errs: Vec<String> = Vec::new();
    loop {
        let next_last_check = OffsetDateTime::now_utc();
        let repos = list_perforce_repos(
            frontend,
            last_check,
            batch_size,
            page.saturating_mul(batch_size),
        )
        .await?;
        last_check = Some(next_last_check);

        for repo in &repos {
            if let Err(err) = process_repo(frontend, gitserver, repo).await {
                errs.push(err);
            }
        }

        if i64::try_from(repos.len()).unwrap_or(i64::MAX) != batch_size {
            break;
        }
        page += 1;
    }

    match combine_errors(&errs) {
        Some(message) => Err(message),
        None => Ok(()),
    }
}

/// `Repos.ListMinimalRepos` with only the options the mapper sets: the
/// default `deleted_at`/`blocked` conditions, the external service type,
/// `MinLastChanged`, and `ORDER BY id LIMIT/OFFSET`.
pub async fn list_perforce_repos(
    frontend: &PgPool,
    min_last_changed: Option<OffsetDateTime>,
    limit: i64,
    offset: i64,
) -> Result<Vec<MinimalRepo>, String> {
    let sql = match min_last_changed {
        None => {
            r#"
SELECT repo.id, repo.name::text AS name
FROM repo
WHERE repo.deleted_at IS NULL
  AND repo.blocked IS NULL
  AND repo.external_service_type = $1
ORDER BY repo.id
LIMIT $2 OFFSET $3
"#
        }
        Some(_) => {
            r#"
SELECT repo.id, repo.name::text AS name
FROM repo
WHERE repo.deleted_at IS NULL
  AND repo.blocked IS NULL
  AND repo.external_service_type = $1
  AND (
    COALESCE(repo.updated_at, repo.created_at) >= $4
    OR EXISTS (
      SELECT 1 FROM gitserver_repos gr
      WHERE gr.repo_id = repo.id AND gr.last_changed >= $4
    )
    OR EXISTS (
      SELECT 1 FROM search_context_repos scr
      LEFT JOIN search_contexts sc ON scr.search_context_id = sc.id
      WHERE scr.repo_id = repo.id AND sc.updated_at >= $4
    )
    OR EXISTS (
      SELECT 1
      FROM codeintel_path_ranks pr
      JOIN codeintel_ranking_progress crp ON crp.graph_key = pr.graph_key
      WHERE pr.repository_id = repo.id
        AND crp.id = (
          SELECT pl.id
          FROM codeintel_ranking_progress pl
          WHERE pl.reducer_completed_at IS NOT NULL
          ORDER BY pl.reducer_completed_at DESC
          LIMIT 1
        )
        AND crp.reducer_completed_at >= $4
    )
  )
ORDER BY repo.id
LIMIT $2 OFFSET $3
"#
        }
    };

    let rows = if let Some(min_last_changed) = min_last_changed {
        sqlx::query(sql)
            .bind(PERFORCE_SERVICE_TYPE)
            .bind(limit)
            .bind(offset)
            .bind(min_last_changed)
            .fetch_all(frontend)
            .await
    } else {
        sqlx::query(sql)
            .bind(PERFORCE_SERVICE_TYPE)
            .bind(limit)
            .bind(offset)
            .fetch_all(frontend)
            .await
    }
    .map_err(|err| err.to_string())?;

    rows.into_iter()
        .map(|row| {
            Ok(MinimalRepo {
                id: row
                    .try_get("id")
                    .map_err(|err: sqlx::Error| err.to_string())?,
                name: row
                    .try_get("name")
                    .map_err(|err: sqlx::Error| err.to_string())?,
            })
        })
        .collect()
}

/// `processRepo`.
pub async fn process_repo(
    frontend: &PgPool,
    gitserver: &dyn PerforceGitserver,
    repo: &MinimalRepo,
) -> Result<(), String> {
    let commits = get_commits_to_insert(frontend, gitserver, repo)
        .await
        .map_err(|err| format!("failed to map perforce changelists: {err}"))?;
    insert_changelists(frontend, repo.id, &commits)
        .await
        .map_err(|err| format!("failed to insert perforce changelist mappings: {err}"))
}

/// `getCommitsToInsert`.
pub async fn get_commits_to_insert(
    frontend: &PgPool,
    gitserver: &dyn PerforceGitserver,
    repo: &MinimalRepo,
) -> Result<Vec<PerforceChangelist>, String> {
    let latest = get_latest_for_repo(frontend, repo.id).await;

    let latest = match latest {
        Ok(latest) => latest,
        Err(err) => {
            // `errors.Is(err, sql.ErrNoRows)`: the repo has no mapping yet.
            if err == NO_ROWS {
                // The baseline returns the (possibly empty) partial result
                // together with the wrapped walk error.
                let results = extract_changelists_from_commits(gitserver, &repo.name, "", "").await;
                return match results {
                    Ok(results) => Ok(results),
                    Err(err) => Err(format!(
                        "failed to import new repo (perforce changelists will have limited functionality): {err}"
                    )),
                };
            }
            return Err(format!("RepoCommits.GetLatestForRepo: {err}"));
        }
    };

    let (_, head_sha) = gitserver
        .default_branch(&repo.name)
        .await
        .map_err(|err| format!("GetDefaultBranch: {err}"))?;

    if let Some(latest) = &latest {
        if latest.commit_sha == head_sha {
            return Ok(Vec::new());
        }
    }

    let last_mapped = latest
        .as_ref()
        .map(|row| row.commit_sha.as_str())
        .unwrap_or("");
    extract_changelists_from_commits(gitserver, &repo.name, last_mapped, &head_sha)
        .await
        .map_err(|err| {
            format!("failed to import existing repo's commits after HEAD: {head_sha:?}: {err}")
        })
}

/// Sentinel used to distinguish "no row" from other SQL errors, mirroring
/// Go's `sql.ErrNoRows`.
const NO_ROWS: &str = "no rows in result set";

/// `GetLatestForRepo`.
pub async fn get_latest_for_repo(
    frontend: &PgPool,
    repo_id: i32,
) -> Result<Option<RepoCommit>, String> {
    // Erratum: the baseline query lists `perforce_changelist_id` followed by
    // `created_at` without a comma, which Postgres parses as an alias
    // (`perforce_changelist_id AS created_at`). The result is still four
    // columns and the row scanner reads the changelist id; reproduce it.
    let row = sqlx::query(
        r#"
SELECT
	id,
	repo_id,
	commit_sha,
	perforce_changelist_id
	created_at
FROM
	repo_commits_changelists
WHERE
	repo_id = $1
ORDER BY
	perforce_changelist_id DESC
LIMIT 1
"#,
    )
    .bind(repo_id)
    .fetch_optional(frontend)
    .await
    .map_err(|err| err.to_string())?;

    let Some(row) = row else {
        return Err(NO_ROWS.to_owned());
    };
    let sha: Vec<u8> = row
        .try_get("commit_sha")
        .map_err(|err: sqlx::Error| err.to_string())?;
    Ok(Some(RepoCommit {
        commit_sha: hex_encode(&sha),
    }))
}

/// `extractChangelistsFromCommits`.
pub async fn extract_changelists_from_commits(
    gitserver: &dyn PerforceGitserver,
    repo: &str,
    last_mapped_commit: &str,
    head_sha: &str,
) -> Result<Vec<PerforceChangelist>, String> {
    let ranges = if last_mapped_commit.is_empty() {
        vec!["HEAD".to_owned()]
    } else {
        vec![format!("{last_mapped_commit}..{head_sha}")]
    };

    const PAGE_SIZE: usize = 50;
    let mut mapped = Vec::new();
    let mut page: usize = 0;
    loop {
        let commits = gitserver
            .commits(repo, &ranges, PAGE_SIZE, page * PAGE_SIZE)
            .await?;
        for commit in &commits {
            let changelist_id = parse_changelist_id(&commit.message)?;
            mapped.push(PerforceChangelist {
                commit_sha: commit.id.clone(),
                changelist_id,
            });
        }
        if commits.len() < PAGE_SIZE {
            break;
        }
        page += 1;
    }
    Ok(mapped)
}

/// `parseChangelistID`.
pub fn parse_changelist_id(commit_message: &str) -> Result<i64, String> {
    let parsed = get_p4_changelist_id(commit_message)?;
    let changelist_id = parsed
        .parse::<i64>()
        .map_err(|err| format!("failed to parse changelist ID to int64: {err}"))?;
    if changelist_id < 0 {
        return Err("changelist ID cannot be negative".to_owned());
    }
    Ok(changelist_id)
}

/// `perforce.GetP4ChangelistID`:
/// `\[(?:git-p4|p4-fusion): depot-paths? = "(.*?)"\: change = (\d+)\]`.
///
/// Hand-rolled instead of pulling in the `regex` crate; the differential
/// harness compares it against Go's `lazyregexp` implementation.
pub fn get_p4_changelist_id(body: &str) -> Result<String, String> {
    for (index, _) in body.char_indices() {
        let rest = &body[index..];
        let Some(rest) = rest
            .strip_prefix("[git-p4: depot-path")
            .or_else(|| rest.strip_prefix("[p4-fusion: depot-path"))
        else {
            continue;
        };
        // `depot-paths?`: an optional trailing `s`.
        let rest = rest.strip_prefix('s').unwrap_or(rest);
        let Some(rest) = rest.strip_prefix(" = \"") else {
            continue;
        };
        // `(.*?)"\: change = (\d+)\]`: the first closing quote followed by
        // the trailer wins (non-greedy). Go's `.` does not match a newline,
        // so the depot path cannot span lines.
        let line_end = rest.find('\n').unwrap_or(rest.len());
        let mut search = &rest[..line_end];
        while let Some(position) = search.find("\": change = ") {
            let after = &search[position + "\": change = ".len()..];
            let digits = after.bytes().take_while(u8::is_ascii_digit).count();
            if digits > 0 && after.as_bytes().get(digits) == Some(&b']') {
                return Ok(after[..digits].to_owned());
            }
            search = &search[position + 1..];
        }
    }
    Err(format!(
        "failed to retrieve changelist ID from commit body: {body:?}"
    ))
}

/// `BatchInsertCommitSHAsWithPerforceChangelistID`: one transaction, one row
/// per mapping. `commit_sha` is `dbutil.CommitBytea` (hex text in Go, raw
/// bytes in the database).
pub async fn insert_changelists(
    frontend: &PgPool,
    repo_id: i32,
    commits: &[PerforceChangelist],
) -> Result<(), String> {
    let mut tx = frontend.begin().await.map_err(|err| err.to_string())?;
    for commit in commits {
        let sha = hex_decode(&commit.commit_sha)?;
        sqlx::query(
            "INSERT INTO repo_commits_changelists (repo_id, commit_sha, perforce_changelist_id) VALUES ($1, $2, $3)",
        )
        .bind(repo_id)
        .bind(sha)
        .bind(commit.changelist_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    }
    tx.commit().await.map_err(|err| err.to_string())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit((byte >> 4) as u32, 16).unwrap_or('0'));
        out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap_or('0'));
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("encoding/hex: odd length hex string".to_owned());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        let high = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| format!("encoding/hex: invalid byte: {:?}", pair[0] as char))?;
        let low = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("encoding/hex: invalid byte: {:?}", pair[1] as char))?;
        out.push(((high << 4) | low) as u8);
    }
    Ok(out)
}

/// `perforceChangelistMapper.Handle` as a periodic handler.
pub struct MapperHandler {
    frontend: PgPool,
    gitserver: Arc<dyn PerforceGitserver>,
    live_config: Arc<LiveConfig>,
    batch_size: i64,
}

impl MapperHandler {
    pub fn new(
        frontend: PgPool,
        gitserver: Arc<dyn PerforceGitserver>,
        live_config: Arc<LiveConfig>,
        batch_size: i64,
    ) -> Self {
        Self {
            frontend,
            gitserver,
            live_config,
            batch_size,
        }
    }
}

#[async_trait]
impl Handler for MapperHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        perforce_changelist_mapper(
            &self.frontend,
            self.gitserver.as_ref(),
            &self.live_config,
            self.batch_size,
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `cmd/worker/internal/perforce.NewPerforceChangelistMappingJob`.
#[derive(Debug, Default)]
pub struct PerforceChangelistMappingJob {
    config: Arc<MapperConfig>,
}

impl PerforceChangelistMappingJob {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(&self) -> &MapperConfig {
        &self.config
    }
}

#[async_trait]
impl Job for PerforceChangelistMappingJob {
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
        let git = SgGitClient::new(GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ));

        let routine = PeriodicRoutine::new(Arc::new(MapperHandler::new(
            deps.frontend.clone(),
            Arc::new(git),
            deps.live_config.clone(),
            self.config.repository_batch_size(),
        )))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(self.config.interval());

        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_p4_and_p4_fusion_trailers() {
        assert_eq!(
            get_p4_changelist_id(r#"[git-p4: depot-paths = "//test-perms/": change = 83725]"#),
            Ok("83725".to_owned())
        );
        assert_eq!(
            get_p4_changelist_id(r#"[p4-fusion: depot-paths = "//depot/": change = 80972]"#),
            Ok("80972".to_owned())
        );
        assert_eq!(
            get_p4_changelist_id(r#"[git-p4: depot-path = "//depot/": change = 7]"#),
            Ok("7".to_owned())
        );
    }

    #[test]
    fn finds_the_trailer_inside_a_larger_body() {
        assert_eq!(
            get_p4_changelist_id(
                "subject\n\nbody [p4-fusion: depot-paths = \"//a b/\": change = 42] tail"
            ),
            Ok("42".to_owned())
        );
    }

    #[test]
    fn rejects_missing_or_invalid_trailers() {
        assert_eq!(
            get_p4_changelist_id("no trailer here"),
            Err(
                "failed to retrieve changelist ID from commit body: \"no trailer here\"".to_owned()
            )
        );
        assert_eq!(
            get_p4_changelist_id(r#"[git-p4: depot-paths = "//depot/": change = abc]"#),
            Err("failed to retrieve changelist ID from commit body: \"[git-p4: depot-paths = \\\"//depot/\\\": change = abc]\"".to_owned())
        );
    }

    #[test]
    fn parse_changelist_requires_a_non_negative_integer() {
        assert_eq!(
            parse_changelist_id(r#"[git-p4: depot-paths = "//d/": change = 12]"#),
            Ok(12)
        );
        // `\d+` never captures the sign, so negative ids fail earlier.
        assert_eq!(
            parse_changelist_id(r#"[git-p4: depot-paths = "//d/": change = -5]"#),
            Err(
                "failed to retrieve changelist ID from commit body: \"[git-p4: depot-paths = \\\"//d/\\\": change = -5]\""
                    .to_owned()
            )
        );
    }

    #[test]
    fn hex_round_trips_commit_shas() {
        let bytes = [0x00, 0xab, 0xff, 0x10];
        assert_eq!(hex_encode(&bytes), "00abff10");
        assert_eq!(hex_decode("00abff10"), Ok(bytes.to_vec()));
        assert!(hex_decode("abc").is_err());
        assert!(hex_decode("zz").is_err());
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = PerforceChangelistMappingJob::new();
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert_eq!(job.configs().len(), 1);
        assert_eq!(ROUTINE_NAME, "perforce-changelist-mapper");
    }
}
