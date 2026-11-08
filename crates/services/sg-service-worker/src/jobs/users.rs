#![forbid(unsafe_code)]

//! `cmd/worker/internal/users`: the `aggregated-users-statistics` job.
//!
//! One routine, `update_aggregated_user_statistics`, first run after five
//! minutes and then every twelve hours. Each run is gated on the bool feature
//! flag `user_management_cache_disabled`: when the flag exists and evaluates
//! to `true` globally, the update is skipped. Any lookup error (missing flag,
//! invalid column state, database failure) falls through to the update, as in
//! the baseline (`if err == nil { ... }`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::feature_flags::{get_feature_flag, FeatureFlagError};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};

pub const FLAG_NAME: &str = "user_management_cache_disabled";
pub const ROUTINE_NAME: &str = "update_aggregated_user_statistics";
pub const ROUTINE_DESCRIPTION: &str = "update aggregated user statistics in the database";
pub const INTERVAL: Duration = Duration::from_secs(12 * 3600);
pub const INITIAL_DELAY: Duration = Duration::from_secs(5 * 60);

/// `eventlogger.NonActiveUserEvents`, in declaration order. The update counts
/// only events *not* in this list.
pub const NON_ACTIVE_USER_EVENTS: &[&str] = &[
    "ViewSignIn",
    "ViewSignUp",
    "SignOutAttempted",
    "SignOutFailed",
    "SignOutSucceeded",
    "SignInAttempted",
    "SignInFailed",
    "SignInSucceeded",
    "PasswordResetRequested",
    "PasswordRandomized",
    "PasswordChanged",
    "EmailVerified",
    "ExternalAuthSignupFailed",
    "ExternalAuthSignupSucceeded",
    "CodyVSCodeExtension:CodySavedLogin:executed",
    "cody.characters.flush",
    "cody.characters",
];

/// `users.aggregatedUsersStatisticsJob`.
pub struct AggregatedUsersStatistics;

#[async_trait]
impl Job for AggregatedUsersStatistics {
    fn description(&self) -> String {
        "updates the aggregated user statistics table in the database".to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<PeriodicRoutine>, String> {
        Ok(vec![PeriodicRoutine::new(
            Arc::new(AggregatedUsersStatisticsHandler {
                pool: deps.frontend.clone(),
            }) as Arc<dyn Handler>,
        )
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(INTERVAL)
        .with_initial_delay(INITIAL_DELAY)])
    }
}

struct AggregatedUsersStatisticsHandler {
    pool: PgPool,
}

#[async_trait]
impl Handler for AggregatedUsersStatisticsHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        if cache_disabled(&self.pool).await {
            return Ok(());
        }
        update_aggregated_users_statistics(&self.pool)
            .await
            .map_err(HandlerError::new)
    }
}

/// The handler's feature flag gate. `true` means "skip the update".
pub async fn cache_disabled(pool: &PgPool) -> bool {
    match get_feature_flag(pool, FLAG_NAME).await {
        Ok(flag) => flag.evaluate_global() == Some(true),
        Err(FeatureFlagError::NotFound)
        | Err(FeatureFlagError::InvalidColumnState)
        | Err(FeatureFlagError::Database(_)) => false,
    }
}

/// `updateAggregatedUsersStatisticsQuery`, with the non-active event list
/// spliced in exactly as the baseline builds it.
pub fn update_query() -> String {
    format!(
        r#"
	INSERT INTO aggregated_user_statistics (user_id, user_last_active_at, user_events_count)
	SELECT
		user_id,
		last_active_at,
		events_count
	FROM
		(
			SELECT
				user_id,
				MAX(timestamp) AS last_active_at,
				-- count billable only events for each user
				COUNT(*) FILTER (WHERE name NOT IN ('{non_active}')) AS events_count
			FROM
				event_logs
			GROUP BY
				user_id
		) AS events
		INNER JOIN users ON users.id = events.user_id
	ON CONFLICT (user_id) DO UPDATE
		SET
			user_last_active_at = EXCLUDED.user_last_active_at,
			user_events_count = EXCLUDED.user_events_count,
			updated_at = NOW();
	"#,
        non_active = NON_ACTIVE_USER_EVENTS.join("','"),
    )
}

/// `updateAggregatedUsersStatisticsTable`.
pub async fn update_aggregated_users_statistics(pool: &PgPool) -> Result<(), String> {
    sqlx::query(&update_query())
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_metadata_matches_the_baseline() {
        assert_eq!(
            AggregatedUsersStatistics.description(),
            "updates the aggregated user statistics table in the database"
        );
    }

    #[test]
    fn non_active_events_are_spliced_into_the_query() {
        assert_eq!(NON_ACTIVE_USER_EVENTS.len(), 17);
        let query = update_query();
        assert!(query.contains(
            "name NOT IN ('ViewSignIn','ViewSignUp','SignOutAttempted','SignOutFailed',\
'SignOutSucceeded','SignInAttempted','SignInFailed','SignInSucceeded',\
'PasswordResetRequested','PasswordRandomized','PasswordChanged','EmailVerified',\
'ExternalAuthSignupFailed','ExternalAuthSignupSucceeded',\
'CodyVSCodeExtension:CodySavedLogin:executed','cody.characters.flush','cody.characters')"
        ));
        assert!(query.contains("ON CONFLICT (user_id) DO UPDATE"));
    }

    #[tokio::test]
    async fn job_constructs_the_baseline_routine() {
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
        };
        let routines = AggregatedUsersStatistics
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), INTERVAL);
        assert_eq!(routines[0].initial_delay(), INITIAL_DELAY);
    }
}
