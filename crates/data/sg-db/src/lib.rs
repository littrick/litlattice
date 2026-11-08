//! Postgres access shared by the Rust service processes.
//!
//! Migrations stay owned by the Go `migrator` (the M6 storage contract): this
//! crate only reads and writes the existing schema. Every store here maps to
//! the SQL in `internal/database`, including its `IS DISTINCT FROM` guards so
//! that unchanged writes do not bump `updated_at`.

use sqlx::postgres::{PgPool, PgPoolOptions};

pub mod gitserver_repos;

/// Connect to the frontend database. The pool is intentionally small: the
/// gitserver only reads/writes repository metadata.
pub async fn connect(dsn: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(dsn)
        .await
}
