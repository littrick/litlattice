#![forbid(unsafe_code)]

//! `cmd/worker/internal/eventlogs`: the `event-logs-janitor` job.
//!
//! Two hourly routines, both using the database clock:
//!
//! * `delete_old_event_logs` (immediate first run) deletes `event_logs` older
//!   than 93 days;
//! * `delete_old_security_event_logs` (first run after one hour) deletes
//!   `security_event_logs` older than 30 days, but only when site
//!   configuration stores them in the database (`log.securityEventLog.location`
//!   is `database` or `all`).
//!
//! Both handlers are plain `HandlerFunc`s in the baseline: no `ErrorHandler`,
//! so errors are only recorded by the periodic routine's observation, which
//! is not ported.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};

pub const EVENT_LOGS_RETENTION: Duration = Duration::from_secs(93 * 24 * 3600);
pub const SECURITY_EVENT_LOGS_RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);
pub const INTERVAL: Duration = Duration::from_secs(3600);

const DELETE_OLD_EVENT_LOGS: &str =
    r#"DELETE FROM event_logs WHERE "timestamp" < now() - interval '93' day"#;
const DELETE_OLD_SECURITY_EVENT_LOGS: &str =
    r#"DELETE FROM security_event_logs WHERE "timestamp" < now() - interval '30' day"#;

/// `eventlogs.eventLogsJob`.
pub struct EventLogsJanitor;

#[async_trait]
impl Job for EventLogsJanitor {
    fn description(&self) -> String {
        "deletes old event logs from postgres".to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<PeriodicRoutine>, String> {
        let pool = deps.frontend.clone();
        let live_config = Arc::clone(&deps.live_config);

        Ok(vec![
            PeriodicRoutine::new(
                Arc::new(DeleteOldEventLogs { pool: pool.clone() }) as Arc<dyn Handler>
            )
            .with_name("delete_old_event_logs")
            .with_description("deleting expired rows from event_logs table")
            .with_interval(INTERVAL),
            PeriodicRoutine::new(
                Arc::new(DeleteOldSecurityEventLogs { pool, live_config }) as Arc<dyn Handler>
            )
            .with_name("delete_old_security_event_logs")
            .with_description("deleting expired rows from security_event_logs table")
            .with_interval(INTERVAL)
            .with_initial_delay(INTERVAL),
        ])
    }
}

struct DeleteOldEventLogs {
    pool: PgPool,
}

#[async_trait]
impl Handler for DeleteOldEventLogs {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        delete_old_event_logs(&self.pool)
            .await
            .map_err(HandlerError::new)
    }
}

struct DeleteOldSecurityEventLogs {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

#[async_trait]
impl Handler for DeleteOldSecurityEventLogs {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let site = self.live_config.current().site.clone();
        delete_old_security_event_logs(&self.pool, &site)
            .await
            .map_err(HandlerError::new)
    }
}

/// `deleteOldEventLogsInPostgres`.
pub async fn delete_old_event_logs(pool: &PgPool) -> Result<(), String> {
    sqlx::query(DELETE_OLD_EVENT_LOGS)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// `deleteOldSecurityEventLogsInPostgres`: the `conf.Get()` gate first, then
/// the delete.
pub async fn delete_old_security_event_logs(
    pool: &PgPool,
    site: &SiteConfiguration,
) -> Result<(), String> {
    if !security_event_logs_in_database(site) {
        return Ok(());
    }

    sqlx::query(DELETE_OLD_SECURITY_EVENT_LOGS)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// `c.Log != nil && c.Log.SecurityEventLog != nil &&
/// (location == "database" || location == "all")`.
pub fn security_event_logs_in_database(site: &SiteConfiguration) -> bool {
    let Some(log) = site.raw().get("log") else {
        return false;
    };
    let Some(security_event_log) = log.get("securityEventLog") else {
        return false;
    };
    matches!(
        security_event_log
            .get("location")
            .and_then(serde_json::Value::as_str),
        Some("database" | "all")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn security_event_log_gate_matches_the_baseline() {
        assert!(!security_event_logs_in_database(&site("{}")));
        assert!(!security_event_logs_in_database(&site(
            r#"{"log": {"securityEventLog": null}}"#
        )));
        assert!(!security_event_logs_in_database(&site(
            r#"{"log": {"securityEventLog": {}}}"#
        )));
        assert!(!security_event_logs_in_database(&site(
            r#"{"log": {"securityEventLog": {"location": "none"}}}"#
        )));
        assert!(!security_event_logs_in_database(&site(
            r#"{"log": {"securityEventLog": {"location": "auditlog"}}}"#
        )));
        assert!(security_event_logs_in_database(&site(
            r#"{"log": {"securityEventLog": {"location": "database"}}}"#
        )));
        assert!(security_event_logs_in_database(&site(
            r#"{"log": {"securityEventLog": {"location": "all"}}}"#
        )));
    }

    #[tokio::test]
    async fn job_constructs_the_baseline_routines() {
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };
        let routines = EventLogsJanitor
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 2);
        assert_eq!(routines[0].name(), "delete_old_event_logs");
        assert_eq!(
            routines[0].description(),
            "deleting expired rows from event_logs table"
        );
        assert_eq!(routines[0].interval(), INTERVAL);
        assert_eq!(routines[0].initial_delay(), Duration::ZERO);
        assert_eq!(routines[1].name(), "delete_old_security_event_logs");
        assert_eq!(routines[1].interval(), INTERVAL);
        assert_eq!(
            routines[1].initial_delay(),
            Duration::from_secs(3600),
            "the security janitor waits one hour before its first run"
        );
    }
}
