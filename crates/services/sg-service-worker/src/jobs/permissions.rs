#![forbid(unsafe_code)]

//! `cmd/worker/internal/permissions/perms_syncer_cleaner.go`: the
//! `permission-sync-job-cleaner` job.
//!
//! One routine, `auth.permission_sync_job_cleaner`, whose interval and
//! history size both come from site configuration and are re-read on every
//! iteration (the baseline registers `conf.Watch` once and stores the interval
//! in a package variable):
//!
//! * `permissions.syncJobCleanupInterval` (seconds, `<= 0` → 60s default);
//! * `permissions.syncJobsHistorySize` (default 5 when unset).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

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
}
