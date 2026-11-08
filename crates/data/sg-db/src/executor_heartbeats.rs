#![forbid(unsafe_code)]

//! `internal/database/executors.go`: `DeleteInactiveHeartbeats`.
//!
//! The janitor (`executors-janitor`) deletes heartbeat rows for executor
//! instances that have not pinged within the configured age.

use std::time::Duration;

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

/// `executorStoreDeleteInactiveHeartbeatsQuery`, copied verbatim.
const DELETE_INACTIVE_HEARTBEATS: &str = r#"
DELETE FROM executor_heartbeats
WHERE $1 - last_seen_at >= $2 * interval '1 second'
"#;

/// `deleteInactiveHeartbeats`: `now` is the process clock
/// (`timeutil.Now()`), `min_age` the configured heartbeat record age.
pub async fn delete_inactive_heartbeats(
    pool: &PgPool,
    now: OffsetDateTime,
    min_age: Duration,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(DELETE_INACTIVE_HEARTBEATS)
        .bind(now)
        .bind(min_age.as_secs() as i64)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}
