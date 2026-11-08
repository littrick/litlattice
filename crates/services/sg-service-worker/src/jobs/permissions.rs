#![forbid(unsafe_code)]

//! `cmd/worker/internal/permissions`: the `permission-sync-job-cleaner` and
//! `permission-sync-job-scheduler` jobs.
//!
//! The cleaner runs one routine, `auth.permission_sync_job_cleaner`, whose
//! interval and history size both come from site configuration and are
//! re-read on every iteration (the baseline registers `conf.Watch` once and
//! stores the interval in a package variable):
//!
//! * `permissions.syncJobCleanupInterval` (seconds, `<= 0` → 60s default);
//! * `permissions.syncJobsHistorySize` (default 5 when unset).
//!
//! The scheduler (`perms_syncer_scheduler.go`) runs
//! `auth.permission_sync_job_scheduler` every
//! `permissions.syncScheduleInterval` seconds (default 15s). Each tick is a
//! no-op while permission syncing is disabled (no authorization provider
//! configured, no ACLs license, or `disableAutoCodeHostSyncs`); otherwise it
//! schedules four groups of sync jobs in `getSchedule` order: users without
//! permissions, private repositories without permissions, the users with the
//! oldest permissions, and the private repositories with the oldest
//! permissions. Job creation honours the baseline's duplicate check
//! (cancel a queued lower-priority job).
//!
//! Porting notes (docs/modules/06_worker.md §51):
//!
//! * `providers.ProvidersFromConfig` is reduced to the provider *count* the
//!   scheduler needs: per-kind predicates over the decrypted service config,
//!   with per-provider construction errors treated as "no provider".
//! * The ACLs license check reads the site license key with
//!   [`sg_licensing::check_acls`]; the Redis "license valid" flag is not
//!   consulted (same divergence as the repo-updater syncer, see §13).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_db::external_services::ExternalServiceStore;
use sg_db::OffsetDateTime;
use sg_encryption::maybe_decrypt;
use sg_keyring::Keyring;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::{Job, JobDeps};
use crate::framework::periodic::{GetInterval, Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

pub const ROUTINE_NAME: &str = "auth.permission_sync_job_cleaner";
pub const ROUTINE_DESCRIPTION: &str = "Cleans up completed or failed permissions sync jobs";
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_JOBS_TO_KEEP: i32 = 5;

/// `permissionSyncJobCleaner`.
pub struct PermissionSyncJobCleaner;

#[async_trait]
impl Job for PermissionSyncJobCleaner {
    fn description(&self) -> String {
        ROUTINE_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let live_config = Arc::clone(&deps.live_config);
        let interval_config = Arc::clone(&live_config);
        let get_interval: GetInterval =
            Arc::new(move || cleanup_interval(&interval_config.current().site));

        Ok(vec![PeriodicRoutine::new(
            Arc::new(PermissionSyncJobCleanerHandler {
                pool: deps.frontend.clone(),
                live_config,
            }) as Arc<dyn Handler>,
        )
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval_func(get_interval)
        .into()])
    }
}

struct PermissionSyncJobCleanerHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

#[async_trait]
impl Handler for PermissionSyncJobCleanerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let jobs_to_keep = jobs_to_keep(&self.live_config.current().site);
        sg_db::permission_sync_jobs::clean_jobs(&self.pool, jobs_to_keep)
            .await
            .map(|_| ())
            .map_err(|err| HandlerError::new(err.to_string()))
    }
}

/// `loadCleanupIntervalFromConf`: the site setting is in seconds; values
/// `<= 0` (including the unset zero value) fall back to the one-minute
/// default.
pub fn cleanup_interval(site: &SiteConfiguration) -> Duration {
    match raw_int(site, "permissions.syncJobCleanupInterval") {
        Some(seconds) if seconds > 0 => Duration::from_secs(seconds as u64),
        _ => DEFAULT_CLEANUP_INTERVAL,
    }
}

/// `cleanJobs`: `jobsToKeep` defaults to 5 and is only overridden when the
/// pointer in the site configuration is non-nil.
pub fn jobs_to_keep(site: &SiteConfiguration) -> i32 {
    raw_int(site, "permissions.syncJobsHistorySize")
        .map(|value| value as i32)
        .unwrap_or(DEFAULT_JOBS_TO_KEEP)
}

fn raw_int(site: &SiteConfiguration, key: &str) -> Option<i64> {
    // The schema stores these as flat keys with dots, e.g.
    // `"permissions.syncJobCleanupInterval": 30`.
    site.raw().get(key).and_then(serde_json::Value::as_i64)
}

// ---------------------------------------------------------------------------
// permission-sync-job-scheduler (`perms_syncer_scheduler.go`)
// ---------------------------------------------------------------------------

/// `(*permissionSyncJobScheduler).Description`.
pub const SCHEDULER_DESCRIPTION: &str = "Schedule permission sync jobs for users and repositories.";
/// `defaultScheduleInterval`.
pub const DEFAULT_SCHEDULE_INTERVAL: Duration = Duration::from_secs(15);
/// `goroutine.WithName("auth.permission_sync_job_scheduler")`.
pub const SCHEDULER_ROUTINE_NAME: &str = "auth.permission_sync_job_scheduler";

/// `database.ReasonUserNoPermissions`.
pub const REASON_USER_NO_PERMS: &str = "REASON_USER_NO_PERMS";
/// `database.ReasonUserOutdatedPermissions`.
pub const REASON_USER_OUTDATED_PERMS: &str = "REASON_USER_OUTDATED_PERMS";
/// `database.ReasonRepoNoPermissions`.
pub const REASON_REPO_NO_PERMS: &str = "REASON_REPO_NO_PERMS";
/// `database.ReasonRepoOutdatedPermissions`.
pub const REASON_REPO_OUTDATED_PERMS: &str = "REASON_REPO_OUTDATED_PERMS";

/// `database.LowPriorityPermissionsSync`.
pub const LOW_PRIORITY_PERMISSIONS_SYNC: i32 = 0;
/// `database.MediumPriorityPermissionsSync`.
pub const MEDIUM_PRIORITY_PERMISSIONS_SYNC: i32 = 5;

/// `oldestUserPermissionsBatchSize` / `oldestRepoPermissionsBatchSize`.
pub const DEFAULT_OLDEST_PERMS_BATCH_SIZE: i64 = 10;
/// `auth.SyncUserBackoff` / `SyncRepoBackoff` default.
pub const DEFAULT_PERMISSIONS_SYNC_BACKOFF: Duration = Duration::from_secs(60);

/// The external service kinds `ProvidersFromConfig` lists.
const AUTHZ_SERVICE_KINDS: &[&str] = &[
    "AZUREDEVOPS",
    "BITBUCKETCLOUD",
    "BITBUCKETSERVER",
    "GERRIT",
    "GITHUB",
    "GITLAB",
    "PERFORCE",
];

/// `auth.Backoff`: [`Backoff::Conf`] reads the two backoff site settings
/// (`auth.ConfBackoff`, the scheduler's value); [`Backoff::Zero`] disables the
/// cutoff (`auth.ZeroBackoff`, used by tests of `getSchedule`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    Conf,
    Zero,
}

impl Backoff {
    /// `SyncUserBackoff`.
    pub fn user(self, site: &SiteConfiguration) -> Duration {
        match self {
            Self::Zero => Duration::ZERO,
            Self::Conf => backoff_seconds(site, "permissions.syncUsersBackoffSeconds"),
        }
    }

    /// `SyncRepoBackoff`.
    pub fn repo(self, site: &SiteConfiguration) -> Duration {
        match self {
            Self::Zero => Duration::ZERO,
            Self::Conf => backoff_seconds(site, "permissions.syncReposBackoffSeconds"),
        }
    }
}

/// `scheduledUser`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledUser {
    pub user_id: i32,
    pub reason: &'static str,
    pub priority: i32,
    pub no_perms: bool,
}

/// `scheduledRepo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledRepo {
    pub repo_id: i32,
    pub reason: &'static str,
    pub priority: i32,
    pub no_perms: bool,
}

/// `schedule`.
#[derive(Debug, Default)]
pub struct Schedule {
    pub users: Vec<ScheduledUser>,
    pub repos: Vec<ScheduledRepo>,
}

/// `permissionSyncJobScheduler` + its periodic handler.
pub struct PermissionSyncJobScheduler;

/// `(*permissionSyncJobScheduler).Routines`'s handler.
pub struct PermissionSyncJobSchedulerHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    keyring: Arc<Keyring>,
    acls: AclsCheck,
}

/// `licensing.Check(licensing.FeatureACLs)` as an injectable predicate; the
/// default reads the site license key.
pub type AclsCheck = Arc<dyn Fn(&SiteConfiguration) -> bool + Send + Sync>;

impl PermissionSyncJobSchedulerHandler {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>, keyring: Arc<Keyring>) -> Self {
        Self {
            pool,
            live_config,
            keyring,
            acls: Arc::new(|site| sg_licensing::check_acls(&site.license_key())),
        }
    }

    /// Test seam replacing the license check (`licensing.MockCheckFeature`).
    pub fn with_acls_check(mut self, acls: AclsCheck) -> Self {
        self.acls = acls;
        self
    }
}

#[async_trait]
impl Job for PermissionSyncJobScheduler {
    fn description(&self) -> String {
        SCHEDULER_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let live_config = Arc::clone(&deps.live_config);
        let interval_config = Arc::clone(&live_config);
        let get_interval: GetInterval =
            Arc::new(move || schedule_interval(&interval_config.current().site));

        Ok(vec![PeriodicRoutine::new(
            Arc::new(PermissionSyncJobSchedulerHandler::new(
                deps.frontend.clone(),
                live_config,
                Arc::clone(&deps.keyring),
            )) as Arc<dyn Handler>,
        )
        .with_name(SCHEDULER_ROUTINE_NAME)
        .with_description(SCHEDULER_DESCRIPTION)
        .with_interval_func(get_interval)
        .into()])
    }
}

#[async_trait]
impl Handler for PermissionSyncJobSchedulerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let site = self.live_config.current().site.clone();
        let acls = (self.acls)(&site);
        // The baseline always builds the provider list first, then ORs the
        // three disabled conditions (`permissionSyncingDisabled`).
        let providers = count_authz_providers(&self.pool, &site, &self.keyring).await;
        if permission_syncing_disabled(&site, providers, acls) {
            tracing::debug!("scheduler disabled due to permission syncing disabled");
            return Ok(());
        }

        schedule_jobs(&self.pool, &site, Backoff::Conf)
            .await
            .map(|_| ())
            .map_err(HandlerError::new)
    }
}

/// `loadScheduleIntervalFromConf`: seconds, `<= 0` → 15s.
pub fn schedule_interval(site: &SiteConfiguration) -> Duration {
    match raw_int(site, "permissions.syncScheduleInterval") {
        Some(seconds) if seconds > 0 => Duration::from_secs(seconds as u64),
        _ => DEFAULT_SCHEDULE_INTERVAL,
    }
}

fn backoff_seconds(site: &SiteConfiguration, key: &str) -> Duration {
    match raw_int(site, key) {
        Some(seconds) if seconds > 0 => Duration::from_secs(seconds as u64),
        _ => DEFAULT_PERMISSIONS_SYNC_BACKOFF,
    }
}

/// `oldestUserPermissionsBatchSize`: a pointer setting; unset or negative
/// keeps the default 10.
pub fn oldest_user_permissions_batch_size(site: &SiteConfiguration) -> i64 {
    match raw_int(site, "permissions.syncOldestUsers") {
        Some(value) if value >= 0 => value,
        _ => DEFAULT_OLDEST_PERMS_BATCH_SIZE,
    }
}

/// `oldestRepoPermissionsBatchSize`.
pub fn oldest_repo_permissions_batch_size(site: &SiteConfiguration) -> i64 {
    match raw_int(site, "permissions.syncOldestRepos") {
        Some(value) if value >= 0 => value,
        _ => DEFAULT_OLDEST_PERMS_BATCH_SIZE,
    }
}

/// `getSchedule`: four lists in the documented order. Each query error is
/// wrapped exactly like the baseline (`errors.Wrap`).
pub async fn get_schedule(
    pool: &PgPool,
    site: &SiteConfiguration,
    backoff: Backoff,
) -> Result<Schedule, String> {
    let mut schedule = Schedule::default();

    let users_with_no_perms = sg_db::permission_sync_jobs::user_ids_with_no_perms(
        pool,
        site.authz_enforce_for_site_admins(),
    )
    .await
    .map_err(|err| format!("schedule users with no permissions: {err}"))?;
    schedule.users.extend(
        users_with_no_perms
            .into_iter()
            .map(|user_id| ScheduledUser {
                user_id,
                reason: REASON_USER_NO_PERMS,
                priority: MEDIUM_PRIORITY_PERMISSIONS_SYNC,
                no_perms: true,
            }),
    );

    let repos_with_no_perms = sg_db::permission_sync_jobs::repo_ids_with_no_perms(pool)
        .await
        .map_err(|err| format!("schedule repositories with no permissions: {err}"))?;
    schedule.repos.extend(
        repos_with_no_perms
            .into_iter()
            .map(|repo_id| ScheduledRepo {
                repo_id,
                reason: REASON_REPO_NO_PERMS,
                priority: MEDIUM_PRIORITY_PERMISSIONS_SYNC,
                no_perms: true,
            }),
    );

    let user_limit = oldest_user_permissions_batch_size(site);
    if user_limit > 0 {
        let users_with_oldest_perms = sg_db::permission_sync_jobs::user_ids_with_oldest_perms(
            pool,
            user_limit,
            cutoff(backoff.user(site)),
        )
        .await
        .map_err(|err| format!("load users with oldest permissions: {err}"))?;
        schedule.users.extend(
            users_with_oldest_perms
                .into_iter()
                .map(|user_id| ScheduledUser {
                    user_id,
                    reason: REASON_USER_OUTDATED_PERMS,
                    priority: LOW_PRIORITY_PERMISSIONS_SYNC,
                    no_perms: false,
                }),
        );
    }

    let repo_limit = oldest_repo_permissions_batch_size(site);
    if repo_limit > 0 {
        let repos_with_oldest_perms = sg_db::permission_sync_jobs::repo_ids_with_oldest_perms(
            pool,
            repo_limit,
            cutoff(backoff.repo(site)),
        )
        .await
        .map_err(|err| format!("scan repositories with oldest permissions: {err}"))?;
        schedule.repos.extend(
            repos_with_oldest_perms
                .into_iter()
                .map(|repo_id| ScheduledRepo {
                    repo_id,
                    reason: REASON_REPO_OUTDATED_PERMS,
                    priority: LOW_PRIORITY_PERMISSIONS_SYNC,
                    no_perms: false,
                }),
        );
    }

    Ok(schedule)
}

/// `getCutoffClause`: a zero age means no cutoff.
fn cutoff(age: Duration) -> Option<OffsetDateTime> {
    if age.is_zero() {
        None
    } else {
        Some(OffsetDateTime::now_utc() - age)
    }
}

/// `scheduleJobs`: create a sync job per scheduled user/repository. A failed
/// insert is logged and skipped, like the baseline; the return value is the
/// number of *scheduled* entries (not successful inserts).
pub async fn schedule_jobs(
    pool: &PgPool,
    site: &SiteConfiguration,
    backoff: Backoff,
) -> Result<usize, String> {
    let schedule = get_schedule(pool, site, backoff).await?;
    let now = OffsetDateTime::now_utc();

    tracing::debug!(
        users = schedule.users.len(),
        repos = schedule.repos.len(),
        "scheduling permission syncs"
    );

    for user in &schedule.users {
        if let Err(err) = sg_db::permission_sync_jobs::create_user_sync_job(
            pool,
            user.user_id,
            user.reason,
            user.priority,
            user.no_perms,
            now,
        )
        .await
        {
            tracing::error!(
                user_id = user.user_id,
                error = %err,
                "failed to create sync job for user"
            );
        }
    }

    for repo in &schedule.repos {
        if let Err(err) = sg_db::permission_sync_jobs::create_repo_sync_job(
            pool,
            repo.repo_id,
            repo.reason,
            repo.priority,
            repo.no_perms,
            now,
        )
        .await
        {
            tracing::error!(
                repository_id = repo.repo_id,
                error = %err,
                "failed to create sync job for repository"
            );
        }
    }

    Ok(schedule.users.len() + schedule.repos.len())
}

/// `permissionSyncingDisabled`: no authorization provider, no ACLs license,
/// or `disableAutoCodeHostSyncs`.
pub fn permission_syncing_disabled(site: &SiteConfiguration, providers: usize, acls: bool) -> bool {
    providers == 0 || !acls || site.disable_auto_code_host_syncs()
}

/// `providers.ProvidersFromConfig`'s provider count, which is all the
/// scheduler uses.
pub async fn count_authz_providers(
    pool: &PgPool,
    site: &SiteConfiguration,
    keyring: &Keyring,
) -> usize {
    authz_provider_urns(pool, site, keyring).await.len()
}

/// `providers.ProvidersFromConfig`'s returned providers, reduced to their
/// URNs (the differential harness compares this list). Services whose config
/// cannot be decrypted/parsed, and per-kind construction errors, contribute
/// nothing (the baseline collects them as "serious problems").
///
/// Azure DevOps is the exception to "one provider per service": its
/// constructor merges every `enforcePermissions` connection into a single
/// provider whose URN is the constant `azuredevops:authzprovider`
/// (`internal/authz/providers/azuredevops/provider.go`).
pub async fn authz_provider_urns(
    pool: &PgPool,
    site: &SiteConfiguration,
    keyring: &Keyring,
) -> Vec<String> {
    let store = ExternalServiceStore::new(pool.clone());
    let services = match store.list_by_kinds(AUTHZ_SERVICE_KINDS).await {
        Ok(services) => services,
        Err(err) => {
            tracing::error!(error = %err, "Could not list external services");
            return Vec::new();
        }
    };

    let mut providers = Vec::new();
    let mut azuredevops = false;
    for service in services {
        let plain = match maybe_decrypt(
            keyring.external_service_key().as_deref(),
            &service.config,
            &service.encryption_key_id,
        )
        .await
        {
            Ok(plain) => plain,
            Err(err) => {
                tracing::error!(
                    id = service.id,
                    error = %err,
                    "Could not parse config of external service"
                );
                continue;
            }
        };
        let config: Value = match serde_json::from_str(&plain) {
            Ok(config) => config,
            Err(err) => {
                tracing::error!(
                    id = service.id,
                    error = %err,
                    "Could not parse config of external service"
                );
                continue;
            }
        };
        if service.kind == "AZUREDEVOPS" {
            azuredevops |= azuredevops_configured(&config);
        } else if authz_provider_configured(&service.kind, &config, site) {
            providers.push(sg_extsvc::urn(&service.kind, service.id));
        }
    }
    if azuredevops {
        providers.push("azuredevops:authzprovider".to_owned());
    }
    providers
}

/// The per-kind `newAuthzProvider` predicates (docs §51.3).
fn authz_provider_configured(kind: &str, config: &Value, site: &SiteConfiguration) -> bool {
    match kind {
        // `github.newAuthzProvider`: `Authorization != nil` (the URL/github-app
        // construction errors are not modelled).
        "GITHUB" => authorization(config).is_some(),
        // `gitlab.newAuthzProvider`: the identity provider must be usable, and
        // the OAuth variant needs a matching `auth.providers` entry.
        "GITLAB" => gitlab_provider_configured(config, site),
        // `bitbucketserver.newAuthzProvider`.
        "BITBUCKETSERVER" => bitbucketserver_provider_configured(config),
        // `bitbucketcloud`, `gerrit` and `perforce` only require authorization.
        "BITBUCKETCLOUD" | "GERRIT" | "PERFORCE" => authorization(config).is_some(),
        _ => false,
    }
}

/// `azuredevops.NewAuthzProviders`: a connection is merged into the shared
/// provider only when `enforcePermissions` is true.
fn azuredevops_configured(config: &Value) -> bool {
    config
        .get("enforcePermissions")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// `c.Authorization != nil`.
fn authorization(config: &Value) -> Option<&Value> {
    config.get("authorization").filter(|value| !value.is_null())
}

fn gitlab_provider_configured(config: &Value, site: &SiteConfiguration) -> bool {
    let Some(auth) = authorization(config) else {
        return false;
    };
    // `schema.IdentityProvider` is a tagged union: the `type` discriminant
    // ("oauth" / "username") is what populates the Go field.
    match auth
        .get("identityProvider")
        .and_then(identity_provider_type)
    {
        Some("oauth") => gitlab_oauth_has_authn_provider(config, site),
        Some("username") => true,
        _ => false,
    }
}

fn identity_provider_type(idp: &Value) -> Option<&str> {
    idp.get("type").and_then(Value::as_str)
}

/// The OAuth identity provider requires an `auth.providers` entry of type
/// `gitlab` with the same hostname (the authn URL defaults to
/// `https://gitlab.com`).
fn gitlab_oauth_has_authn_provider(config: &Value, site: &SiteConfiguration) -> bool {
    let gl_url = config
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(gl_host) = hostname(gl_url) else {
        return false;
    };
    site.raw()
        .get("auth.providers")
        .and_then(Value::as_array)
        .is_some_and(|providers| {
            providers.iter().any(|provider| {
                provider.get("type").and_then(Value::as_str) == Some("gitlab") && {
                    let authn_url = provider
                        .get("url")
                        .and_then(Value::as_str)
                        .filter(|url| !url.is_empty())
                        .unwrap_or("https://gitlab.com");
                    hostname(authn_url).as_deref() == Some(gl_host.as_str())
                }
            })
        })
}

fn bitbucketserver_provider_configured(config: &Value) -> bool {
    let Some(auth) = authorization(config) else {
        return false;
    };
    if auth.get("oauth2").and_then(Value::as_bool).unwrap_or(false) {
        return true;
    }
    match auth.get("identityProvider") {
        Some(idp) if !idp.is_null() => identity_provider_type(idp) == Some("username"),
        _ => false,
    }
}

fn hostname(url: &str) -> Option<String> {
    Url::parse(url).ok()?.host_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn cleanup_interval_matches_the_baseline_defaults() {
        assert_eq!(cleanup_interval(&site("{}")), DEFAULT_CLEANUP_INTERVAL);
        assert_eq!(
            cleanup_interval(&site(r#"{"permissions.syncJobCleanupInterval": 0}"#)),
            DEFAULT_CLEANUP_INTERVAL
        );
        assert_eq!(
            cleanup_interval(&site(r#"{"permissions.syncJobCleanupInterval": -5}"#)),
            DEFAULT_CLEANUP_INTERVAL
        );
        assert_eq!(
            cleanup_interval(&site(r#"{"permissions.syncJobCleanupInterval": 30}"#)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn jobs_to_keep_matches_the_baseline_defaults() {
        assert_eq!(jobs_to_keep(&site("{}")), 5);
        assert_eq!(
            jobs_to_keep(&site(r#"{"permissions.syncJobsHistorySize": 2}"#)),
            2
        );
        assert_eq!(
            jobs_to_keep(&site(r#"{"permissions.syncJobsHistorySize": 9}"#)),
            9
        );
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        assert_eq!(
            PermissionSyncJobCleaner.description(),
            "Cleans up completed or failed permissions sync jobs"
        );
    }

    #[tokio::test]
    async fn job_constructs_the_baseline_routine() {
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };
        let routines = PermissionSyncJobCleaner
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), DEFAULT_CLEANUP_INTERVAL);
        assert_eq!(routines[0].initial_delay(), Duration::ZERO);
    }

    #[test]
    fn schedule_interval_matches_the_baseline_defaults() {
        assert_eq!(schedule_interval(&site("{}")), DEFAULT_SCHEDULE_INTERVAL);
        assert_eq!(
            schedule_interval(&site(r#"{"permissions.syncScheduleInterval": 0}"#)),
            DEFAULT_SCHEDULE_INTERVAL
        );
        assert_eq!(
            schedule_interval(&site(r#"{"permissions.syncScheduleInterval": -1}"#)),
            DEFAULT_SCHEDULE_INTERVAL
        );
        assert_eq!(
            schedule_interval(&site(r#"{"permissions.syncScheduleInterval": 30}"#)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn oldest_batch_sizes_match_the_baseline_defaults() {
        for site_json in ["{}", r#"{"permissions.syncOldestUsers": -1}"#] {
            assert_eq!(
                oldest_user_permissions_batch_size(&site(site_json)),
                DEFAULT_OLDEST_PERMS_BATCH_SIZE
            );
        }
        assert_eq!(
            oldest_user_permissions_batch_size(&site(r#"{"permissions.syncOldestUsers": 0}"#)),
            0
        );
        assert_eq!(
            oldest_user_permissions_batch_size(&site(r#"{"permissions.syncOldestUsers": 3}"#)),
            3
        );
        assert_eq!(
            oldest_repo_permissions_batch_size(&site("{}")),
            DEFAULT_OLDEST_PERMS_BATCH_SIZE
        );
        assert_eq!(
            oldest_repo_permissions_batch_size(&site(r#"{"permissions.syncOldestRepos": 0}"#)),
            0
        );
    }

    #[test]
    fn backoff_matches_the_baseline_defaults() {
        let empty = site("{}");
        assert_eq!(Backoff::Zero.user(&empty), Duration::ZERO);
        assert_eq!(Backoff::Zero.repo(&empty), Duration::ZERO);
        assert_eq!(Backoff::Conf.user(&empty), DEFAULT_PERMISSIONS_SYNC_BACKOFF);
        assert_eq!(Backoff::Conf.repo(&empty), DEFAULT_PERMISSIONS_SYNC_BACKOFF);

        let configured = site(
            r#"{"permissions.syncUsersBackoffSeconds": 90,
                "permissions.syncReposBackoffSeconds": 0}"#,
        );
        assert_eq!(Backoff::Conf.user(&configured), Duration::from_secs(90));
        assert_eq!(
            Backoff::Conf.repo(&configured),
            DEFAULT_PERMISSIONS_SYNC_BACKOFF
        );
    }

    #[test]
    fn permission_syncing_disabled_matches_the_baseline() {
        let enabled = site("{}");
        assert!(permission_syncing_disabled(&enabled, 0, true));
        assert!(!permission_syncing_disabled(&enabled, 1, true));
        assert!(permission_syncing_disabled(&enabled, 1, false));

        let disabled = site(r#"{"disableAutoCodeHostSyncs": true}"#);
        assert!(permission_syncing_disabled(&disabled, 1, true));
    }

    #[test]
    fn provider_predicates_follow_the_baseline_constructors() {
        let config = |json: &str| -> Value { serde_json::from_str(json).expect("config") };

        // GitHub/Bitbucket Cloud/Gerrit/Perforce: authorization non-null.
        assert!(authz_provider_configured(
            "GITHUB",
            &config(r#"{"authorization":{}}"#),
            &site("{}")
        ));
        assert!(!authz_provider_configured(
            "GITHUB",
            &config(r#"{"token":"t"}"#),
            &site("{}")
        ));
        assert!(authz_provider_configured(
            "BITBUCKETCLOUD",
            &config(r#"{"authorization":{}}"#),
            &site("{}")
        ));
        assert!(authz_provider_configured(
            "GERRIT",
            &config(r#"{"authorization":{}}"#),
            &site("{}")
        ));
        assert!(authz_provider_configured(
            "PERFORCE",
            &config(r#"{"authorization":{}}"#),
            &site("{}")
        ));

        // GitLab username identity provider needs no `auth.providers` entry.
        assert!(authz_provider_configured(
            "GITLAB",
            &config(
                r#"{"url":"https://gitlab.example.com",
                    "authorization":{"identityProvider":{"type":"username"}}}"#
            ),
            &site("{}")
        ));
        // GitLab OAuth: the authn provider hostname must match.
        let matching =
            site(r#"{"auth.providers":[{"type":"gitlab","url":"https://gitlab.example.com"}]}"#);
        let oauth = config(
            r#"{"url":"https://gitlab.example.com",
                "authorization":{"identityProvider":{"type":"oauth"}}}"#,
        );
        assert!(authz_provider_configured("GITLAB", &oauth, &matching));
        assert!(!authz_provider_configured(
            "GITLAB",
            &oauth,
            &site(r#"{"auth.providers":[{"type":"gitlab","url":"https://other.example.com"}]}"#)
        ));
        assert!(!authz_provider_configured("GITLAB", &oauth, &site("{}")));
        // GitLab: no identity provider is an error (no provider).
        assert!(!authz_provider_configured(
            "GITLAB",
            &config(r#"{"url":"https://gitlab.example.com","authorization":{}}"#),
            &site("{}")
        ));

        // Bitbucket Server: oauth2 or a username identity provider.
        assert!(authz_provider_configured(
            "BITBUCKETSERVER",
            &config(r#"{"authorization":{"oauth2":true}}"#),
            &site("{}")
        ));
        assert!(authz_provider_configured(
            "BITBUCKETSERVER",
            &config(r#"{"authorization":{"identityProvider":{"type":"username"}}}"#),
            &site("{}")
        ));
        assert!(!authz_provider_configured(
            "BITBUCKETSERVER",
            &config(r#"{"authorization":{}}"#),
            &site("{}")
        ));

        // Azure DevOps: enforcePermissions gates the shared provider.
        assert!(azuredevops_configured(&config(
            r#"{"enforcePermissions":true}"#
        )));
        assert!(!azuredevops_configured(&config(
            r#"{"enforcePermissions":false}"#
        )));
        assert!(!azuredevops_configured(&config(r#"{}"#)));
    }

    #[test]
    fn scheduler_metadata_matches_the_baseline() {
        assert_eq!(SCHEDULER_ROUTINE_NAME, "auth.permission_sync_job_scheduler");
        assert_eq!(
            SCHEDULER_DESCRIPTION,
            PermissionSyncJobScheduler.description()
        );
    }

    #[tokio::test]
    async fn scheduler_constructs_the_baseline_routine() {
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };
        let routines = PermissionSyncJobScheduler
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), SCHEDULER_ROUTINE_NAME);
        assert_eq!(routines[0].description(), SCHEDULER_DESCRIPTION);
        assert_eq!(routines[0].interval(), DEFAULT_SCHEDULE_INTERVAL);
        assert_eq!(routines[0].initial_delay(), Duration::ZERO);
    }
}
