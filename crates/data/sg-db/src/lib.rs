//! Postgres access shared by the Rust service processes.
//!
//! Migrations stay owned by the Go `migrator` (the M6 storage contract): this
//! crate only reads and writes the existing schema. Every store here maps to
//! the SQL in `internal/database`, including its `IS DISTINCT FROM` guards so
//! that unchanged writes do not bump `updated_at`.

use sqlx::postgres::{PgPool, PgPoolOptions};

pub mod batches;
pub mod batches_janitor;
pub mod code_monitors;
pub mod codeintel_autoindex_queue;
pub mod codeintel_autoindexing_config;
pub mod codeintel_autoindexing_dependencies;
pub mod codeintel_autoindexing_enqueuer;
pub mod codeintel_codegraph;
pub mod codeintel_dependencies;
pub mod codeintel_dependency_jobs;
pub mod codeintel_policies;
pub mod codeintel_ranking;
pub mod codeintel_uploads;
pub mod conf;
pub mod event_logs;
pub mod executor_heartbeats;
pub mod exhaustive_search;
pub mod external_service_repos;
pub mod external_service_sync_jobs;
pub mod external_services;
pub mod feature_flags;
pub mod github_apps;
pub mod gitserver_repos;
pub mod global_state;
pub mod insights_scheduler;
pub mod out_of_band_migrations;
pub mod own_signal_configurations;
pub mod permission_sync_jobs;
pub mod perms_store;
pub mod recent_contribution_signals;
pub mod repo_embeddings;
pub mod repo_paths;
pub mod repo_statistics;
pub mod repos;
pub mod repository_lookup;
pub mod reposcheduler;
pub mod repoupdater_state;
pub mod sub_repo_perms;
pub mod syntactic_indexing_jobs;
pub mod telemetry_events;
pub mod user_emails;
pub mod user_external_accounts;
pub mod users;
pub mod worker_queue;
pub mod zoekt_repos;

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
