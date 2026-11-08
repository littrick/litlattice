#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/uploads_expirer.go` +
//! `internal/codeintel/uploads/internal/background/expirer`: the
//! `codeintel-upload-expirer` job.
//!
//! The job owns `uploads.ExpirerConfigInst` (`expirer.Config`) and returns the
//! single routine built by `uploads.NewExpirationTasks` →
//! `background.NewExpirationTasks` → `expirer.NewUploadExpirer`:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.upload-expirer` | `CODEINTEL_UPLOAD_EXPIRER_INTERVAL` (30s) | mark uploads not protected by any data-retention policy as expired |
//!
//! Each invocation (`HandleExpiredUploadsBatch`):
//!
//! 1. `SetRepositoriesForRetentionScan(repositoryProcessDelay,
//!    repositoryBatchSize)` claims a batch of repositories with completed
//!    uploads, a fresh commit graph (`update_token = dirty_token`) and no
//!    scan within the process delay, stamping
//!    `lsif_last_retention_scan`;
//! 2. for every claimed repository, `buildCommitMap` pages through
//!    `GetConfigurationPolicies(RepositoryID, ForDataRetention=true)` and
//!    asks the [`PolicyMatcher`](super::policies_matcher::PolicyMatcher) (`RetentionExtractor`,
//!    `includeTipOfDefaultBranch=true`, `filterByCreatedDate=false`) which
//!    commits are described by a retention policy;
//! 3. `GetUploads(state=completed, InCommitGraph, AllowExpired=false,
//!    OldestFirst, LastRetentionScanBefore=now-uploadProcessDelay)` pages
//!    through the candidate uploads with `UploadBatchSize`, and
//!    `isUploadProtectedByPolicy` walks every commit visible to each upload
//!    (paginated `GetCommitsVisibleToUpload` with `CommitBatchSize`) looking
//!    for a match whose policy duration is unbounded or younger than the
//!    upload;
//! 4. `UpdateUploadRetention` stamps `last_retention_scan_at` on the
//!    protected ids and sets `expired = TRUE` on the rest.
//!
//! Error semantics copied from the baseline: a repository failure is
//! collected and the remaining repositories are still processed; any failure
//! inside one repository (including one upload's visibility check) stops that
//! repository's remaining work; and — a baseline quirk — a failing
//! `UpdateUploadRetention` is swallowed when no per-upload error was
//! collected (`errors.Wrap(nil, ...)` is `nil`).
//!
//! Not ported: the `src_codeintel_background_{repositories,upload_records,
//! commits}_scanned_total` / `..._expired_total` counters (metrics slice),
//! the RED metrics/spans and log lines. The baseline runs with the internal
//! actor; the Rust SQL has no authz condition.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_policies::{self, ConfigurationPolicy, GetConfigurationPoliciesOptions};
use sg_db::codeintel_uploads::{self, ExpirationUpload};
use sg_db::repos::RepoStore;
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::policies_matcher::{Extractor, GitserverGit, MatcherGit, PolicyMatch, PolicyMatcher};
use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `uploadExpirerJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `expirer.NewUploadExpirer`'s routine metadata.
pub const ROUTINE_NAME: &str = "codeintel.upload-expirer";
pub const ROUTINE_DESCRIPTION: &str = "marks uploads as expired based on retention policies";

/// `env.ChooseFallbackVariableName` candidates, in priority order.
pub const COMMIT_BATCH_SIZE_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_EXPIRER_COMMIT_BATCH_SIZE",
    "PRECISE_CODE_INTEL_RETENTION_COMMIT_BATCH_SIZE",
];
pub const POLICY_BATCH_SIZE_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_EXPIRER_POLICY_BATCH_SIZE",
    "PRECISE_CODE_INTEL_RETENTION_POLICY_BATCH_SIZE",
];
pub const REPOSITORY_BATCH_SIZE_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_EXPIRER_REPOSITORY_BATCH_SIZE",
    "PRECISE_CODE_INTEL_RETENTION_REPOSITORY_BATCH_SIZE",
];
pub const REPOSITORY_PROCESS_DELAY_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_EXPIRER_REPOSITORY_PROCESS_DELAY",
    "PRECISE_CODE_INTEL_RETENTION_REPOSITORY_PROCESS_DELAY",
];
pub const UPLOAD_BATCH_SIZE_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_EXPIRER_UPLOAD_BATCH_SIZE",
    "PRECISE_CODE_INTEL_RETENTION_UPLOAD_BATCH_SIZE",
];
pub const UPLOAD_PROCESS_DELAY_VARS: [&str; 2] = [
    "CODEINTEL_UPLOAD_EXPIRER_UPLOAD_PROCESS_DELAY",
    "PRECISE_CODE_INTEL_RETENTION_UPLOAD_PROCESS_DELAY",
];
pub const INTERVAL_VAR: &str = "CODEINTEL_UPLOAD_EXPIRER_INTERVAL";

const COMMIT_BATCH_SIZE_DESCRIPTION: &str =
    "The number of commits to process per upload at a time.";
const INTERVAL_DESCRIPTION: &str = "How frequently to run the upload expirer routine.";
const POLICY_BATCH_SIZE_DESCRIPTION: &str =
    "The number of policies to consider for expiration at a time.";
const REPOSITORY_BATCH_SIZE_DESCRIPTION: &str =
    "The number of repositories to consider for expiration at a time.";
const REPOSITORY_PROCESS_DELAY_DESCRIPTION: &str =
    "The minimum frequency that the same repository's uploads can be considered for expiration.";
const UPLOAD_BATCH_SIZE_DESCRIPTION: &str =
    "The number of uploads to consider for expiration at a time.";
const UPLOAD_PROCESS_DELAY_DESCRIPTION: &str =
    "The minimum frequency that the same upload record can be considered for expiration.";

/// `expirer.Config` (`uploads.ExpirerConfigInst`).
#[derive(Debug, Default)]
pub struct ExpirerConfig {
    commit_batch_size: Mutex<i64>,
    expirer_interval: Mutex<Duration>,
    policy_batch_size: Mutex<i64>,
    repository_batch_size: Mutex<i64>,
    repository_process_delay: Mutex<Duration>,
    upload_batch_size: Mutex<i64>,
    upload_process_delay: Mutex<Duration>,
    errs: Mutex<Vec<String>>,
}

macro_rules! getter {
    ($name:ident, $field:ident, $ty:ty) => {
        pub fn $name(&self) -> $ty {
            *self
                .$field
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    };
}

impl ExpirerConfig {
    getter!(commit_batch_size, commit_batch_size, i64);
    getter!(expirer_interval, expirer_interval, Duration);
    getter!(policy_batch_size, policy_batch_size, i64);
    getter!(repository_batch_size, repository_batch_size, i64);
    getter!(repository_process_delay, repository_process_delay, Duration);
    getter!(upload_batch_size, upload_batch_size, i64);
    getter!(upload_process_delay, upload_process_delay, Duration);
}

impl JobConfig for ExpirerConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();

        let commit_batch_size_name = choose_fallback(&COMMIT_BATCH_SIZE_VARS);
        let policy_batch_size_name = choose_fallback(&POLICY_BATCH_SIZE_VARS);
        let repository_batch_size_name = choose_fallback(&REPOSITORY_BATCH_SIZE_VARS);
        let repository_process_delay_name = choose_fallback(&REPOSITORY_PROCESS_DELAY_VARS);
        let upload_batch_size_name = choose_fallback(&UPLOAD_BATCH_SIZE_VARS);
        let upload_process_delay_name = choose_fallback(&UPLOAD_PROCESS_DELAY_VARS);

        *self.lock_commit_batch_size() = get_int(
            env,
            &commit_batch_size_name,
            "100",
            COMMIT_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.lock_expirer_interval() =
            get_interval(env, INTERVAL_VAR, "30s", INTERVAL_DESCRIPTION, &mut errs);
        *self.lock_policy_batch_size() = get_int(
            env,
            &policy_batch_size_name,
            "100",
            POLICY_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.lock_repository_batch_size() = get_int(
            env,
            &repository_batch_size_name,
            "100",
            REPOSITORY_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.lock_repository_process_delay() = get_interval(
            env,
            &repository_process_delay_name,
            "24h",
            REPOSITORY_PROCESS_DELAY_DESCRIPTION,
            &mut errs,
        );
        *self.lock_upload_batch_size() = get_int(
            env,
            &upload_batch_size_name,
            "100",
            UPLOAD_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        *self.lock_upload_process_delay() = get_interval(
            env,
            &upload_process_delay_name,
            "24h",
            UPLOAD_PROCESS_DELAY_DESCRIPTION,
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

impl ExpirerConfig {
    fn lock_commit_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.commit_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_expirer_interval(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.expirer_interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_policy_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.policy_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_repository_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.repository_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_repository_process_delay(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.repository_process_delay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_upload_batch_size(&self) -> std::sync::MutexGuard<'_, i64> {
        self.upload_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn lock_upload_process_delay(&self) -> std::sync::MutexGuard<'_, Duration> {
        self.upload_process_delay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The subset of [`ExpirerConfig`] the expiration pass reads; a plain snapshot
/// so the async code does not hold the config locks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpirerSettings {
    pub commit_batch_size: i64,
    pub policy_batch_size: i64,
    pub repository_batch_size: i64,
    pub repository_process_delay: Duration,
    pub upload_batch_size: i64,
    pub upload_process_delay: Duration,
}

impl ExpirerConfig {
    pub fn settings(&self) -> ExpirerSettings {
        ExpirerSettings {
            commit_batch_size: self.commit_batch_size(),
            policy_batch_size: self.policy_batch_size(),
            repository_batch_size: self.repository_batch_size(),
            repository_process_delay: self.repository_process_delay(),
            upload_batch_size: self.upload_batch_size(),
            upload_process_delay: self.upload_process_delay(),
        }
    }
}

/// `BaseConfig.GetInt`: `strconv.ParseInt(raw, 10, 64)`, record
/// `invalid int %q for %s: %s` and return 0 on failure.
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match parse_go_int(&raw) {
        Ok(value) => value,
        Err(kind) => {
            errors.push(format!("invalid int {raw:?} for {name}: {kind}"));
            0
        }
    }
}

/// `strconv.ParseInt(raw, 10, 64)`; the error text is the `NumError`'s
/// `Error()` (`strconv.ParseInt: parsing %q: <err>`).
fn parse_go_int(raw: &str) -> Result<i64, String> {
    match raw.parse::<i64>() {
        Ok(value) => Ok(value),
        Err(err) => {
            let reason = match err.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            Err(format!("strconv.ParseInt: parsing {raw:?}: {reason}"))
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

/// `uploadExpirerJob`.
pub struct UploadExpirer {
    config: Arc<ExpirerConfig>,
}

impl UploadExpirer {
    pub fn new() -> Self {
        Self {
            config: Arc::new(ExpirerConfig::default()),
        }
    }

    pub fn config(&self) -> &ExpirerConfig {
        &self.config
    }
}

impl Default for UploadExpirer {
    fn default() -> Self {
        Self::new()
    }
}

struct ExpirerDeps {
    codeintel: PgPool,
    frontend: PgPool,
    git: Arc<GitserverGit>,
    settings: ExpirerSettings,
}

#[async_trait]
impl Job for UploadExpirer {
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
        // `codeintel.InitServices`: the code-intel database and the gitserver
        // client come from the frontend's service connections.
        let unified = deps.live_config.current();
        if unified
            .service_connections
            .code_intel_postgres_dsn
            .is_empty()
        {
            return Err("code_intel_postgres_dsn is not configured by the frontend".to_owned());
        }
        let codeintel = sg_db::connect(&unified.service_connections.code_intel_postgres_dsn, 10)
            .await
            .map_err(|err| format!("connecting to the codeintel database: {err}"))?;
        let git = Arc::new(GitserverGit::new(Arc::new(sg_git::GitserverClient::new(
            unified.service_connections.git_servers.clone(),
        ))));

        let deps = Arc::new(ExpirerDeps {
            codeintel,
            frontend: deps.frontend.clone(),
            git,
            settings: self.config.settings(),
        });

        let routine = PeriodicRoutine::new(Arc::new(ExpirerHandler { deps }))
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(self.config.expirer_interval());

        Ok(vec![routine.into()])
    }
}

/// `expirer`'s handler (the periodic goroutine body).
struct ExpirerHandler {
    deps: Arc<ExpirerDeps>,
}

#[async_trait]
impl Handler for ExpirerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let matcher = PolicyMatcher::new(&*self.deps.git, Extractor::Retention, true, false);
        handle_expired_uploads_batch(
            &self.deps.codeintel,
            &self.deps.frontend,
            &matcher,
            &self.deps.settings,
            OffsetDateTime::now_utc(),
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `expirer.HandleExpiredUploadsBatch` with `timeutil.Now()` supplied by the
/// caller (the handler passes `now`).
pub async fn handle_expired_uploads_batch<G: MatcherGit + ?Sized>(
    codeintel: &PgPool,
    frontend: &PgPool,
    matcher: &PolicyMatcher<'_, G>,
    settings: &ExpirerSettings,
    now: OffsetDateTime,
) -> Result<(), String> {
    let repositories = codeintel_uploads::set_repositories_for_retention_scan(
        codeintel,
        settings.repository_process_delay,
        settings.repository_batch_size,
        now,
    )
    .await
    .map_err(|err| format!("uploadSvc.SelectRepositoriesForRetentionScan: {err}"))?;

    if repositories.is_empty() {
        // All repositories updated recently enough.
        return Ok(());
    }

    let mut errs = Vec::new();
    for repository_id in repositories {
        if let Err(err) =
            handle_repository(codeintel, frontend, matcher, settings, now, repository_id).await
        {
            errs.push(err);
        }
    }

    if errs.is_empty() {
        Ok(())
    } else {
        Err(combine_errors(&errs).unwrap_or_default())
    }
}

async fn handle_repository<G: MatcherGit + ?Sized>(
    codeintel: &PgPool,
    frontend: &PgPool,
    matcher: &PolicyMatcher<'_, G>,
    settings: &ExpirerSettings,
    now: OffsetDateTime,
    repository_id: i32,
) -> Result<(), String> {
    // The map should never be empty: global retention policies protect all
    // data visible from a tag or branch tip for at least a short time.
    let commit_map =
        build_commit_map(codeintel, frontend, matcher, settings, now, repository_id).await?;

    // Assigned outside the loop so a mis-configured `upload_process_delay`
    // shorter than one batch does not make the loop spin forever.
    let last_retention_scan_before = now - settings.upload_process_delay;

    loop {
        let uploads = codeintel_uploads::get_uploads_for_expiration(
            codeintel,
            repository_id,
            settings.upload_batch_size,
            last_retention_scan_before,
        )
        .await
        .map_err(|err| err.to_string())?;
        if uploads.is_empty() {
            return Ok(());
        }

        handle_uploads(codeintel, &commit_map, &uploads, settings, now).await?;
    }
}

/// `buildCommitMap`: pages through the retention policies that apply to one
/// repository and returns the matcher's commit map.
async fn build_commit_map<G: MatcherGit + ?Sized>(
    codeintel: &PgPool,
    frontend: &PgPool,
    matcher: &PolicyMatcher<'_, G>,
    settings: &ExpirerSettings,
    now: OffsetDateTime,
    repository_id: i32,
) -> Result<BTreeMap<String, Vec<PolicyMatch>>, String> {
    // `repoStore.Get(id)`: soft-deleted repositories are `RepoNotFoundErr`;
    // blocked repositories are returned together with a `BlockedRepoError`.
    let repo_store = RepoStore::new(frontend.clone());
    let repo = repo_store
        .get_by_id(repository_id)
        .await
        .map_err(|err| err.to_string())?
        .ok_or_else(|| format!("repo not found: {repository_id}"))?;
    if repo.blocked.is_some() {
        return Err(format!("repo is blocked: {repository_id}"));
    }
    let repo_name = repo.name;

    let mut offset: i64 = 0;
    let mut policies: Vec<ConfigurationPolicy> = Vec::new();
    loop {
        let (policy_batch, total_count) = codeintel_policies::get_configuration_policies(
            codeintel,
            &GetConfigurationPoliciesOptions {
                repository_id,
                for_data_retention: Some(true),
                limit: settings.policy_batch_size,
                offset,
                ..Default::default()
            },
        )
        .await
        .map_err(|err| format!("policySvc.GetConfigurationPolicies: {err}"))?;

        offset += policy_batch.len() as i64;
        let done = policy_batch.is_empty() || offset >= total_count;
        policies.extend(policy_batch);
        if done {
            break;
        }
    }

    matcher
        .commits_described_by_policy(repository_id, &repo_name, &policies, now, &[])
        .await
}

/// `handleUploads`: classifies one batch and updates the retention columns.
async fn handle_uploads(
    codeintel: &PgPool,
    commit_map: &BTreeMap<String, Vec<PolicyMatch>>,
    uploads: &[ExpirationUpload],
    settings: &ExpirerSettings,
    now: OffsetDateTime,
) -> Result<(), String> {
    let mut protected_upload_ids = Vec::with_capacity(uploads.len());
    let mut expired_upload_ids = Vec::with_capacity(uploads.len());
    let mut errs: Vec<String> = Vec::new();

    for upload in uploads {
        match is_upload_protected_by_policy(codeintel, commit_map, upload, settings, now).await {
            Ok(true) => protected_upload_ids.push(upload.id),
            Ok(false) => expired_upload_ids.push(upload.id),
            // Collect errors but keep processing the rest of the batch; the
            // failed records are re-checked on the next scan.
            Err(err) => errs.push(err),
        }
    }

    if codeintel_uploads::update_upload_retention(
        codeintel,
        &mut protected_upload_ids,
        &mut expired_upload_ids,
    )
    .await
    .is_err()
    {
        // Baseline quirk (`job_expirer.go`): the failure is wrapped from the
        // *collected* error, not from `updateErr`:
        //
        //     if updateErr := errors.Wrap(err, "uploadSvc.UpdateUploadRetention"); err == nil {
        //         err = updateErr
        //     } else {
        //         err = errors.Append(err, updateErr)
        //     }
        //
        // With a nil `err`, `errors.Wrap(nil, ...)` is nil and the update
        // failure is silently swallowed; with a non-nil `err`, the appended
        // entry repeats `err` behind the update message and the real
        // `updateErr` is never reported. Both behaviours are reproduced here.
        if !errs.is_empty() {
            let combined = combine_errors(&errs).unwrap_or_default();
            errs.push(format!("uploadSvc.UpdateUploadRetention: {combined}"));
        }
    }

    match combine_errors(&errs) {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// `isUploadProtectedByPolicy`: walks every commit visible to the upload,
/// page by page, and returns `true` as soon as a matching policy is unbounded
/// or younger than the upload.
async fn is_upload_protected_by_policy(
    codeintel: &PgPool,
    commit_map: &BTreeMap<String, Vec<PolicyMatch>>,
    upload: &ExpirationUpload,
    settings: &ExpirerSettings,
    now: OffsetDateTime,
) -> Result<bool, String> {
    let mut token: Option<String> = None;
    let mut first = true;

    while first || token.is_some() {
        first = false;

        let (commits, next_token) = codeintel_uploads::get_commits_visible_to_upload(
            codeintel,
            i64::from(upload.id),
            settings.commit_batch_size,
            token.as_deref(),
        )
        .await
        .map_err(|err| format!("uploadSvc.CommitsVisibleToUpload: {err}"))?;
        token = next_token;

        for commit in &commits {
            let Some(policy_matches) = commit_map.get(commit) else {
                continue;
            };
            for policy_match in policy_matches {
                match policy_match.policy_duration {
                    None => return Ok(true),
                    Some(duration) => {
                        if now - upload.uploaded_at < duration {
                            return Ok(true);
                        }
                    }
                }
            }
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::PoisonError;

    /// The tests below read process environment variables; serialize them
    /// like the other job config tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = UploadExpirer::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
        assert_eq!(ROUTINE_NAME, "codeintel.upload-expirer");
        assert_eq!(
            ROUTINE_DESCRIPTION,
            "marks uploads as expired based on retention policies"
        );
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = ExpirerConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.commit_batch_size(), 100);
        assert_eq!(config.expirer_interval(), Duration::from_secs(30));
        assert_eq!(config.policy_batch_size(), 100);
        assert_eq!(config.repository_batch_size(), 100);
        assert_eq!(
            config.repository_process_delay(),
            Duration::from_secs(24 * 3600)
        );
        assert_eq!(config.upload_batch_size(), 100);
        assert_eq!(
            config.upload_process_delay(),
            Duration::from_secs(24 * 3600)
        );
    }

    #[test]
    fn fallback_variable_names_are_honoured() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        std::env::set_var("PRECISE_CODE_INTEL_RETENTION_UPLOAD_BATCH_SIZE", "7");
        let config = ExpirerConfig::default();
        config.load(&mut EnvReader::new());
        std::env::remove_var("PRECISE_CODE_INTEL_RETENTION_UPLOAD_BATCH_SIZE");
        assert!(config.validate().is_ok());
        assert_eq!(config.upload_batch_size(), 7);
    }

    #[test]
    fn invalid_configuration_is_reported() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = ExpirerConfig::default();
        std::env::set_var(INTERVAL_VAR, "soon");
        config.load(&mut EnvReader::new());
        std::env::remove_var(INTERVAL_VAR);
        let err = config.validate().expect_err("invalid interval");
        assert_eq!(
            err.to_string(),
            "invalid duration \"soon\" for CODEINTEL_UPLOAD_EXPIRER_INTERVAL: time: invalid duration \"soon\""
        );
    }
}
