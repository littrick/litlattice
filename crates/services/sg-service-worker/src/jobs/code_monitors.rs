#![forbid(unsafe_code)]

//! `codemonitors-job` (`cmd/worker/internal/codemonitors` +
//! `internal/codemonitors/background`).
//!
//! M9.8bo lands the two routines that need no search or action execution:
//!
//! * `code_monitors.trigger_query_enqueuer` — `PeriodicGoroutine` every
//!   minute calling `codeMonitorStore.EnqueueQueryTriggerJobs`;
//! * `code_monitors.trigger_jobs_log_deleter` — `PeriodicGoroutine` every
//!   hour calling `DeleteOldTriggerJobs(eventRetentionInDays=30)`.
//!
//! M9.8cl adds [`super::code_monitor_actions`]: the action payload layer
//! (webhook JSON, Slack blocks, e-mail template data) and the webhook/Slack
//! HTTP delivery, ported 1:1 and differentially verified.
//!
//! Still **not** ported: the trigger/action workers and their resetters, the
//! trigger search execution and the e-mail transport/templates. The job is
//! therefore intentionally absent from the registry ([`crate::jobs::all_jobs`]
//! keeps returning the `UnportedJob` error for `codemonitors-job`).
//! Registering it while `Routines` returns a subset would silently drop
//! trigger execution.
//!
//! Baseline facts reproduced here:
//!
//! * `codemonitors.IsEnabled()` is `!DISABLE_CODE_MONITORS` parsed with Go's
//!   `strconv.ParseBool`; an unparsable value counts as `false`, i.e. enabled;
//! * when disabled, `Routines` returns no routines and no error;
//! * the enqueuer ignores the returned trigger jobs;
//! * the deleter uses `eventRetentionInDays = 30`; `cm_action_jobs` rows
//!   cascade through the `cm_action_jobs_trigger_event_fk` foreign key.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::env::parse_go_bool;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::jobs::{Job, JobDeps};

/// `background.eventRetentionInDays`.
pub const EVENT_RETENTION_IN_DAYS: i32 = 30;

pub const ENQUEUER_NAME: &str = "code_monitors.trigger_query_enqueuer";
pub const ENQUEUER_DESCRIPTION: &str = "enqueues code monitor trigger query jobs";
pub const ENQUEUER_INTERVAL: Duration = Duration::from_secs(60);

pub const LOG_DELETER_NAME: &str = "code_monitors.trigger_jobs_log_deleter";
pub const LOG_DELETER_DESCRIPTION: &str = "deletes code job logs from code monitor triggers";
pub const LOG_DELETER_INTERVAL: Duration = Duration::from_secs(3600);

/// `codemonitors.IsEnabled`.
pub fn is_enabled() -> bool {
    !std::env::var("DISABLE_CODE_MONITORS")
        .ok()
        .and_then(|raw| parse_go_bool(&raw).ok())
        .unwrap_or(false)
}

/// `newTriggerQueryEnqueuer`'s handler.
pub struct TriggerQueryEnqueuerHandler {
    frontend: sqlx::PgPool,
}

impl TriggerQueryEnqueuerHandler {
    pub fn new(frontend: sqlx::PgPool) -> Self {
        Self { frontend }
    }
}

#[async_trait]
impl Handler for TriggerQueryEnqueuerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        // The baseline discards the enqueued jobs.
        sg_db::code_monitors::enqueue_query_trigger_jobs(&self.frontend)
            .await
            .map(|_| ())
            .map_err(|err| HandlerError::new(err.to_string()))
    }
}

/// `newTriggerJobsLogDeleter`'s handler.
pub struct TriggerJobsLogDeleterHandler {
    frontend: sqlx::PgPool,
}

impl TriggerJobsLogDeleterHandler {
    pub fn new(frontend: sqlx::PgPool) -> Self {
        Self { frontend }
    }
}

#[async_trait]
impl Handler for TriggerJobsLogDeleterHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        sg_db::code_monitors::delete_old_trigger_jobs(&self.frontend, EVENT_RETENTION_IN_DAYS)
            .await
            .map(|_| ())
            .map_err(|err| HandlerError::new(err.to_string()))
    }
}

/// `cmd/worker/internal/codemonitors.codeMonitorJob`.
///
/// Not registered until the trigger/action workers and action senders are
/// ported; see the module docs.
pub struct CodeMonitorJob;

impl CodeMonitorJob {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CodeMonitorJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for CodeMonitorJob {
    fn description(&self) -> String {
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !is_enabled() {
            return Ok(Vec::new());
        }

        let enqueuer = PeriodicRoutine::new(Arc::new(TriggerQueryEnqueuerHandler::new(
            deps.frontend.clone(),
        )))
        .with_name(ENQUEUER_NAME)
        .with_description(ENQUEUER_DESCRIPTION)
        .with_interval(ENQUEUER_INTERVAL);

        let deleter = PeriodicRoutine::new(Arc::new(TriggerJobsLogDeleterHandler::new(
            deps.frontend.clone(),
        )))
        .with_name(LOG_DELETER_NAME)
        .with_description(LOG_DELETER_DESCRIPTION)
        .with_interval(LOG_DELETER_INTERVAL);

        Ok(vec![enqueuer.into(), deleter.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_matches_go_parse_bool() {
        // Serialize env access with the crate-wide lock used by env tests is
        // not available here; keep this test limited to the pure helper.
        assert!(parse_go_bool("1").unwrap());
        assert!(!parse_go_bool("false").unwrap());
        assert!(parse_go_bool("yes").is_err());
    }

    #[test]
    fn retention_and_intervals_match_baseline() {
        assert_eq!(EVENT_RETENTION_IN_DAYS, 30);
        assert_eq!(ENQUEUER_INTERVAL, Duration::from_secs(60));
        assert_eq!(LOG_DELETER_INTERVAL, Duration::from_secs(3600));
        assert_eq!(ENQUEUER_NAME, "code_monitors.trigger_query_enqueuer");
        assert_eq!(LOG_DELETER_NAME, "code_monitors.trigger_jobs_log_deleter");
    }
}
