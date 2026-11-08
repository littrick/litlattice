#![forbid(unsafe_code)]

//! `cmd/worker/internal/repostatistics`: the `repo-statistics-compactor` and
//! `repo-statistics-resetter` jobs.
//!
//! * Compactor (every 30 minutes): folds the append-only
//!   `repo_statistics` / `gitserver_repos_statistics` rows into one row per
//!   key. Both statements run even when the first fails; the errors are
//!   appended the way `lib/errors.Append` formats them.
//! * Resetter (every 30 minutes): only does work on Sunday between 02:00 and
//!   02:30 UTC — it locks `repo`/`gitserver_repos`, drops both statistic
//!   tables and rebuilds them. Retries up to 10 times, 5 seconds apart, on a
//!   deadlock (Postgres `40P01`) or when the 5-minute attempt times out.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::repo_statistics::{
    compact_gitserver_repos_statistics, compact_repo_statistics, delete_and_recreate_statistics,
};
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

pub const INTERVAL: Duration = Duration::from_secs(30 * 60);
/// `context.WithTimeout(ctx, 5*time.Minute)` per reset attempt.
pub const RESET_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const RESET_RETRIES: u32 = 10;
pub const RESET_RETRY_DELAY: Duration = Duration::from_secs(5);

/// `repostatistics.compactor`.
pub struct RepoStatisticsCompactor;

#[async_trait]
impl Job for RepoStatisticsCompactor {
    fn description(&self) -> String {
        // `Description() string { return "" }`.
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Ok(vec![PeriodicRoutine::new(Arc::new(CompactorHandler {
            pool: deps.frontend.clone(),
        }) as Arc<dyn Handler>)
        .with_name("repomgmt.statistics-compactor")
        .with_description("compacts repo statistics")
        .with_interval(INTERVAL)
        .into()])
    }
}

/// `repostatistics.resetter`.
pub struct RepoStatisticsResetter;

#[async_trait]
impl Job for RepoStatisticsResetter {
    fn description(&self) -> String {
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Ok(vec![PeriodicRoutine::new(Arc::new(ResetterHandler {
            pool: deps.frontend.clone(),
        }) as Arc<dyn Handler>)
        .with_name("repomgmt.statistics-resetter")
        .with_description("resets repo statistics")
        .with_interval(INTERVAL)
        .into()])
    }
}

struct CompactorHandler {
    pool: PgPool,
}

#[async_trait]
impl Handler for CompactorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let mut errors = Vec::new();
        if let Err(err) = compact_repo_statistics(&self.pool).await {
            errors.push(format!("error compacting repo statistics: {err}"));
        }
        if let Err(err) = compact_gitserver_repos_statistics(&self.pool).await {
            errors.push(format!(
                "error compacting gitserver repos statistics: {err}"
            ));
        }

        match append_errors(&errors) {
            Some(message) => Err(HandlerError::new(message)),
            None => Ok(()),
        }
    }
}

struct ResetterHandler {
    pool: PgPool,
}

#[async_trait]
impl Handler for ResetterHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        if !should_reset(OffsetDateTime::now_utc()) {
            tracing::debug!(
                "Skipping deleting and recreating statistics; not Sunday between 2-2:30 UTC"
            );
            return Ok(());
        }

        let mut retries = RESET_RETRIES;
        loop {
            tracing::info!(retries, "Deleting and recreating repo statistics");

            // The timeout keeps the table lock from taking down the instance.
            match tokio::time::timeout(RESET_TIMEOUT, delete_and_recreate_statistics(&self.pool))
                .await
            {
                Ok(Ok(())) => {
                    tracing::info!("Success");
                    return Ok(());
                }
                Ok(Err(err)) => {
                    let deadlock = is_deadlock(&err);
                    if deadlock && retries > 0 {
                        tracing::warn!(
                            deadlock,
                            deadline_exceeded = false,
                            "ran into error. Retrying in 5 seconds..."
                        );
                        retries -= 1;
                        sleep(RESET_RETRY_DELAY).await;
                        continue;
                    }
                    tracing::info!(error = %err, "Failed");
                    return Err(HandlerError::new(err.to_string()));
                }
                Err(_elapsed) => {
                    if retries > 0 {
                        tracing::warn!(
                            deadlock = false,
                            deadline_exceeded = true,
                            "ran into error. Retrying in 5 seconds..."
                        );
                        retries -= 1;
                        sleep(RESET_RETRY_DELAY).await;
                        continue;
                    }
                    tracing::info!("Failed");
                    return Err(HandlerError::new("context deadline exceeded"));
                }
            }
        }
    }
}

/// `now.Weekday() == time.Sunday && now.Hour() == 2 && now.Minute() < 30`.
pub fn should_reset(now: OffsetDateTime) -> bool {
    now.weekday() == time::Weekday::Sunday && now.hour() == 2 && now.minute() < 30
}

/// `errors.As(err, &pgerr) && pgerr.Code == "40P01"`.
fn is_deadlock(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|err| err.code())
        .is_some_and(|code| code == "40P01")
}

/// `cockroachdb/errors.Append`'s rendering.
fn append_errors(errors: &[String]) -> Option<String> {
    match errors {
        [] => None,
        [single] => Some(single.clone()),
        many => {
            let mut message = format!("{} errors occurred:", many.len());
            for error in many {
                message.push_str("\n\t* ");
                message.push_str(error);
            }
            Some(message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::{Date, Month, PrimitiveDateTime};

    fn utc(year: i32, month: Month, day: u8, hour: u8, minute: u8) -> OffsetDateTime {
        PrimitiveDateTime::new(
            Date::from_calendar_date(year, month, day).expect("date"),
            time::Time::from_hms(hour, minute, 0).expect("time"),
        )
        .assume_utc()
    }

    #[test]
    fn reset_window_is_sunday_0200_0230_utc() {
        // 2024-01-07 is a Sunday.
        assert!(should_reset(utc(2024, Month::January, 7, 2, 0)));
        assert!(should_reset(utc(2024, Month::January, 7, 2, 29)));
        assert!(!should_reset(utc(2024, Month::January, 7, 2, 30)));
        assert!(!should_reset(utc(2024, Month::January, 7, 3, 0)));
        assert!(!should_reset(utc(2024, Month::January, 8, 2, 0)), "Monday");
    }

    #[test]
    fn append_errors_matches_the_baseline_multierror() {
        assert_eq!(append_errors(&[]), None);
        assert_eq!(append_errors(&["a".to_owned()]), Some("a".to_owned()));
        assert_eq!(
            append_errors(&["a".to_owned(), "b".to_owned()]),
            Some("2 errors occurred:\n\t* a\n\t* b".to_owned())
        );
    }

    #[tokio::test]
    async fn jobs_construct_the_baseline_routines() {
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };

        let routines = RepoStatisticsCompactor
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");
        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), "repomgmt.statistics-compactor");
        assert_eq!(routines[0].description(), "compacts repo statistics");
        assert_eq!(routines[0].interval(), INTERVAL);

        let routines = RepoStatisticsResetter
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");
        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), "repomgmt.statistics-resetter");
        assert_eq!(routines[0].description(), "resets repo statistics");
        assert_eq!(routines[0].interval(), INTERVAL);
    }
}
