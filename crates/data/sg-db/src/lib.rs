//! Postgres access shared by the Rust service processes.
//!
//! Migrations stay owned by the Go `migrator` (the M6 storage contract): this
//! crate only reads and writes the existing schema. Every store here maps to
//! the SQL in `internal/database`, including its `IS DISTINCT FROM` guards so
//! that unchanged writes do not bump `updated_at`.

use sqlx::postgres::{PgPool, PgPoolOptions};

pub mod batches;
pub mod external_service_repos;
pub mod external_service_sync_jobs;
pub mod external_services;
pub mod gitserver_repos;
pub mod repo_statistics;
pub mod repos;
pub mod repoupdater_state;

/// Connect to the frontend database. The pool is intentionally small: the
/// gitserver only reads/writes repository metadata.
pub async fn connect(dsn: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(dsn)
        .await
}

pub use sqlx::types::time::OffsetDateTime;

/// Convert a database timestamp to a [`std::time::SystemTime`]. Used by
/// services whose scheduling decisions are expressed in terms of "time since
/// last fetch/changed" (the repo-updater scheduler).
pub fn offset_to_system_time(time: OffsetDateTime) -> std::time::SystemTime {
    use std::time::{Duration, UNIX_EPOCH};
    let total_nanos =
        i128::from(time.unix_timestamp()) * 1_000_000_000 + i128::from(time.nanosecond());
    if total_nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(total_nanos as u64)
    } else {
        UNIX_EPOCH - Duration::from_nanos((-total_nanos) as u64)
    }
}

/// Inverse of [`offset_to_system_time`].
pub fn system_time_to_offset(time: std::time::SystemTime) -> OffsetDateTime {
    let total_nanos: i128 = match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(after) => after.as_nanos() as i128,
        Err(before) => -(before.duration().as_nanos() as i128),
    };
    OffsetDateTime::from_unix_timestamp_nanos(total_nanos)
        .expect("SystemTime is within OffsetDateTime's range")
}
