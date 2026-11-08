#![forbid(unsafe_code)]

//! M9.8d differential harness (Rust side): the aggregated users statistics
//! job.
//!
//! Expects `test_ws/p9-go/aggregated_users_fixtures.sql` (plus a feature flag
//! state) to have been applied to `SG_QTEST_DSN` immediately before the run.
//! The Go counterpart is `cmd/worker/internal/users/harness_test.go`.

use sg_db::feature_flags::get_feature_flag;
use sg_service_worker::jobs::users::{
    cache_disabled, update_aggregated_users_statistics, FLAG_NAME,
};

#[tokio::test]
async fn aggregated_users_statistics_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };

    let pool = sg_db::connect(&dsn, 2).await.expect("connect database");

    let (disabled, evaluatable) = match get_feature_flag(&pool, FLAG_NAME).await {
        Ok(flag) => {
            let value = flag.evaluate_global();
            (value == Some(true), value.is_some())
        }
        Err(_) => (false, false),
    };
    assert_eq!(
        disabled,
        cache_disabled(&pool).await,
        "the job's gate must agree with the direct flag evaluation"
    );
    println!("gate disabled={disabled} evaluatable={evaluatable}");

    if !disabled {
        update_aggregated_users_statistics(&pool)
            .await
            .expect("update aggregated users statistics");
    }

    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        r#"
		SELECT
			user_id,
			COALESCE(to_char(user_last_active_at AT TIME ZONE 'UTC',
				'YYYY-MM-DD"T"HH24:MI:SS"Z"'), 'null'),
			COALESCE(user_events_count::text, 'null')
		FROM aggregated_user_statistics
		WHERE user_id BETWEEN 9401 AND 9499
		ORDER BY user_id"#,
    )
    .fetch_all(&pool)
    .await
    .expect("select aggregated user statistics");

    for (user_id, last_active, events) in rows {
        println!("aggregated user_id={user_id} last_active={last_active} events={events}");
    }
}
