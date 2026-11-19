#![forbid(unsafe_code)]

//! Rust port of the Go `migrator` (`cmd/migrator` and
//! `internal/database/migration/{definition,schemas,store,runner}`).
//!
//! The migration SQL corpus is **not** vendored here; callers point
//! [`schemas::resolve_schema`] at a `migrations/` directory (the packaged
//! image copies Sourcegraph's corpus to `/migrations`).

pub mod cli;
pub mod definition;
pub mod describe;
pub mod dsn;
pub mod drift;
pub mod error;
pub mod runner;
pub mod multiversion;
pub mod oobmigration;
pub mod schemas;
pub mod service_version;
pub mod stitched;
pub mod store;
pub mod types;

pub use error::MigratorError;
