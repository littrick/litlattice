#![forbid(unsafe_code)]

//! `cmd/worker/internal/batches/scheduler_job.go` +
//! `internal/batches/scheduler`: the `batches-scheduler` job, ported.
//!
//! Baseline shape:
//!
//! * `schedulerJob.Description` is empty, it has no `env.Config`s, and its
//!   `Routines` returns nothing when `batches.IsEnabled()` is false
//!   (`DISABLE_BATCH_CHANGES` parsed with `strconv.ParseBool`).
//! * Otherwise it returns `scheduler.NewScheduler`, which loops:
//!   build the schedule from the active rollout windows, and every time the
//!   schedule's rate limiter permits, move the oldest `scheduled` changeset to
//!   `queued` (`store.EnqueueNextScheduledChangeset`). An empty queue applies
//!   a bounded exponential backoff (5s ×2, capped at 1m); a successful
//!   enqueue resets it. The schedule is rebuilt when it expires, and when the
//!   `batchChanges.rolloutWindows` site configuration changes.
//!
//! Porting notes (docs/modules/06_worker.md §50):
//!
//! * The scheduler is a [`PeriodicRoutine`] whose handler owns the whole loop,
//!   instead of a `CustomRoutine` that spawns a `ticker` goroutine. The loop
//!   still re-checks the schedule validity and configuration on every
//!   iteration and sleeps between enqueues.
//! * `uber-go/ratelimit`'s limiter is replaced by a plain `unit/n` spacing;
//!   the limiter's one-unit burst allowance is not reproduced.
//! * `Estimate`, `window.Configuration` and the parsing errors live in
//!   [`window`].
//! * The routine records `PeriodicWithMetrics` rather than
//!   `CustomRoutine`; the recorder hooks (`LogStart`/`LogRun`/`LogStop`) are
//!   provided by the periodic framework.

pub mod window;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use self::window::{Configuration, Schedule};
use super::batches::batches_enabled;
use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `schedulerJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `(*scheduler.Scheduler).Name`.
pub const ROUTINE_NAME: &str = "batches-scheduler";
/// `(*scheduler.Scheduler).Description`.
pub const ROUTINE_DESCRIPTION: &str = "Scheduler for batch changes";
/// `(*scheduler.Scheduler).Interval` ("Actually between 5 sec and 1 min,
/// changes dynamically").
pub const SCHEDULER_INTERVAL: Duration = Duration::from_secs(60);

/// `newBackoff(5*time.Second, 2, 1*time.Minute)`.
pub const INITIAL_BACKOFF: Duration = Duration::from_secs(5);
pub const BACKOFF_MULTIPLIER: u32 = 2;
pub const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// `scheduler.backoff`: the next delay is the current value, then the value is
/// multiplied and clamped to the limit.
#[derive(Debug)]
pub struct Backoff {
    init: Duration,
    multiplier: u32,
    limit: Duration,
    current: Duration,
}

impl Backoff {
    pub fn new(init: Duration, multiplier: u32, limit: Duration) -> Self {
        Self {
            init,
            multiplier,
            limit,
            current: init,
        }
    }

    /// `(*backoff).next`.
    pub fn next_delay(&mut self) -> Duration {
        let current = self.current;
        self.current = (self.current * self.multiplier).min(self.limit);
        current
    }

    /// `(*backoff).reset`.
    pub fn reset(&mut self) {
        self.current = self.init;
    }
}

/// The scheduler's mutable state, guarded by a std mutex (never held across an
/// await).
struct State {
    /// The raw `batchChanges.rolloutWindows` value last observed; mirrors the
    /// baseline watcher's `c.raw` comparison so an unrelated configuration
    /// change does not discard the active schedule.
    raw: Option<serde_json::Value>,
    /// Whether a configuration has ever been applied (the baseline's
    /// `c.active != nil`).
    applied: bool,
    config: Configuration,
    schedule: Option<Schedule>,
    /// Wall-clock time before which the next enqueue must not run.
    next_allowed: Option<OffsetDateTime>,
    backoff: Backoff,
}

impl Default for State {
    fn default() -> Self {
        Self {
            raw: None,
            applied: false,
            config: Configuration::default(),
            schedule: None,
            next_allowed: None,
            backoff: Backoff::new(INITIAL_BACKOFF, BACKOFF_MULTIPLIER, MAX_BACKOFF),
        }
    }
}

/// `scheduler.Scheduler.Start`'s loop.
pub struct SchedulerHandler {
    store: sg_db::batches::BatchesStore,
    live_config: Arc<LiveConfig>,
    generation: watch::Receiver<u64>,
    state: Mutex<State>,
}

impl SchedulerHandler {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        let generation = live_config.subscribe();
        Self {
            store: sg_db::batches::BatchesStore::new(pool),
            live_config,
            generation,
            state: Mutex::new(State::default()),
        }
    }

    /// The `config` package's `conf.Watch` callback: parse
    /// `batchChanges.rolloutWindows` when it changes, keeping the previous
    /// configuration (and schedule) when the new one is invalid.
    fn refresh_config(&self, state: &mut State) {
        let raw = self
            .live_config
            .current()
            .site
            .batch_changes_rollout_windows()
            .cloned();
        if state.raw == raw {
            return;
        }

        match Configuration::parse(raw.as_ref()) {
            Ok(config) => {
                state.raw = raw;
                state.applied = true;
                state.config = config;
                state.schedule = None;
            }
            Err(err) => {
                let message = if state.applied {
                    "invalid batch changes rollout configuration detected, using the previous configuration"
                } else {
                    "invalid batch changes rollout configuration detected, using the default"
                };
                tracing::warn!(error = %err, "{message}");
                // Remember the raw value so the warning is emitted once per
                // change; the active schedule is left untouched.
                state.raw = raw;
            }
        }
    }
}

#[async_trait]
impl Handler for SchedulerHandler {
    async fn handle(&self, ctx: CancellationToken) -> Result<(), HandlerError> {
        let mut generation = self.generation.clone();

        loop {
            if ctx.is_cancelled() {
                return Ok(());
            }

            let wait_until = {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                self.refresh_config(&mut state);

                let now = OffsetDateTime::now_utc();
                if state
                    .schedule
                    .is_none_or(|schedule| now >= schedule.valid_until())
                {
                    let schedule = state.config.schedule(now);
                    tracing::debug!(
                        total = schedule.total(),
                        until = %schedule.valid_until(),
                        "applying batch change schedule"
                    );
                    state.schedule = Some(schedule);
                    state.next_allowed = None;
                }

                let schedule = state
                    .schedule
                    .expect("a schedule was built immediately above");
                if schedule.total() == 0 {
                    // A zero schedule never yields; wait for the window to
                    // change (validity, or the configuration update below).
                    Some(schedule.valid_until())
                } else {
                    match state.next_allowed {
                        Some(next) if next > now => Some(next.min(schedule.valid_until())),
                        _ => None,
                    }
                }
            };

            if let Some(deadline) = wait_until {
                let sleep = tokio::time::sleep(sleep_duration(deadline));
                tokio::select! {
                    _ = sleep => {}
                    _ = ctx.cancelled() => return Ok(()),
                    _ = generation.changed() => {}
                }
                continue;
            }

            let result = self.store.enqueue_next_scheduled_changeset().await;
            let now = OffsetDateTime::now_utc();

            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let spacing = state
                .schedule
                .map_or(Duration::ZERO, |schedule| schedule.rate().spacing());
            match result {
                Ok(Some(_)) => {
                    state.backoff.reset();
                    state.next_allowed = if spacing.is_zero() {
                        None
                    } else {
                        Some(now + time_duration(spacing))
                    };
                }
                // `store.ErrNoResults`: nothing to enqueue. The baseline
                // applies the backoff but does not log an error.
                Ok(None) => {
                    state.next_allowed = Some(now + time_duration(state.backoff.next_delay()));
                }
                Err(err) => {
                    tracing::warn!(error = %err, "error enqueueing the next scheduled changeset");
                    state.next_allowed = Some(now + time_duration(state.backoff.next_delay()));
                }
            }
        }
    }
}

/// `time.Until(deadline)` as a non-negative `tokio` sleep duration.
fn sleep_duration(deadline: OffsetDateTime) -> Duration {
    let nanos = deadline.unix_timestamp_nanos() - OffsetDateTime::now_utc().unix_timestamp_nanos();
    Duration::from_nanos(nanos.max(0) as u64)
}

/// `time.Duration` arithmetic on an `OffsetDateTime`.
fn time_duration(duration: Duration) -> time::Duration {
    time::Duration::nanoseconds(duration.as_nanos().min(i64::MAX as u128) as i64)
}

/// `schedulerJob`.
#[derive(Default)]
pub struct BatchesScheduler;

impl BatchesScheduler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for BatchesScheduler {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !batches_enabled() {
            return Ok(Vec::new());
        }

        let handler = Arc::new(SchedulerHandler::new(
            deps.frontend.clone(),
            Arc::clone(&deps.live_config),
        ));
        Ok(vec![PeriodicRoutine::new(handler)
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(SCHEDULER_INTERVAL)
            .into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_clamps() {
        let mut backoff = Backoff::new(INITIAL_BACKOFF, BACKOFF_MULTIPLIER, MAX_BACKOFF);
        assert_eq!(backoff.next_delay(), Duration::from_secs(5));
        assert_eq!(backoff.next_delay(), Duration::from_secs(10));
        assert_eq!(backoff.next_delay(), Duration::from_secs(20));
        assert_eq!(backoff.next_delay(), Duration::from_secs(40));
        assert_eq!(backoff.next_delay(), Duration::from_secs(60));
        assert_eq!(backoff.next_delay(), Duration::from_secs(60));
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(5));
    }

    #[test]
    fn the_job_has_no_configs_and_the_baseline_description() {
        let job = BatchesScheduler::new();
        assert_eq!(job.description(), "");
        assert!(job.configs().is_empty());
    }
}
