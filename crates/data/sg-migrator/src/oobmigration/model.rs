#![forbid(unsafe_code)]

//! `internal/oobmigration/{migrator,store}.go`: the migration record and the
//! migrator interface.
//!
//! Go's `oobmigration.Migrator` interface has `Progress`, `Up` and `Down`; the
//! record metadata (`ID`, introduced/deprecated versions, name, description,
//! non-destructiveness) lives in `Migration`/the YAML. This port follows the
//! shape requested for the Rust runtime, exposing all of those through a
//! single [`Migrator`] trait (with metadata defaults derived from
//! [`super::metadata`]).
//!
//! Unlike Go, the migrators take the `PgPool` per call rather than storing a
//! `basestore.Store`; this keeps registration free of connection plumbing and
//! still lets each batch run inside its own transaction.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::error::MigratorError;

use super::metadata;
use super::version::Version;

/// A migration record mirroring the `out_of_band_migrations` table
/// (`store.go:29`).
#[derive(Debug, Clone, PartialEq)]
pub struct Migration {
    pub id: i32,
    pub team: String,
    pub component: String,
    pub description: String,
    pub introduced: Version,
    pub deprecated: Option<Version>,
    pub progress: f64,
    pub created: OffsetDateTime,
    pub last_updated: Option<OffsetDateTime>,
    pub non_destructive: bool,
    pub is_enterprise: bool,
    pub apply_reverse: bool,
    pub errors: Vec<MigrationError>,
    /// Custom JSON metadata (Go's `json.RawMessage`).
    pub metadata: Option<Value>,
}

impl Default for Migration {
    fn default() -> Self {
        Self {
            id: 0,
            team: String::new(),
            component: String::new(),
            description: String::new(),
            introduced: Version::new(0, 0),
            deprecated: None,
            progress: 0.0,
            created: OffsetDateTime::UNIX_EPOCH,
            last_updated: None,
            non_destructive: false,
            is_enterprise: false,
            apply_reverse: false,
            errors: Vec::new(),
            metadata: None,
        }
    }
}

impl Migration {
    /// `Migration.Complete` (`store.go:49`): completed in whichever direction
    /// `apply_reverse` selects.
    pub fn complete(&self) -> bool {
        (self.progress == 1.0 && !self.apply_reverse)
            || (self.progress == 0.0 && self.apply_reverse)
    }
}

/// The per-family database handles used to run migration bodies.
///
/// Go registers enterprise migrators with more than one `basestore.Store`
/// (frontend, code-intelligence and code-insights); the migration record
/// itself always lives in the frontend database. Migrators whose tables span
/// databases override the `*_with_pools` methods below; every other migrator
/// falls back to [`MigratorPools::frontend`].
#[derive(Clone)]
pub struct MigratorPools {
    pub frontend: PgPool,
    pub codeintel: Option<PgPool>,
    pub insights: Option<PgPool>,
}

impl MigratorPools {
    pub fn frontend_only(pool: PgPool) -> Self {
        Self {
            frontend: pool,
            codeintel: None,
            insights: None,
        }
    }

    pub fn codeintel_or_frontend(&self) -> &PgPool {
        self.codeintel.as_ref().unwrap_or(&self.frontend)
    }

    pub fn insights_or_frontend(&self) -> &PgPool {
        self.insights.as_ref().unwrap_or(&self.frontend)
    }

    /// The baseline's `basestore.Store` selection (`register.go`): code-intel
    /// migrations use the code-intel DB, insights migrations the code-insights
    /// DB (when enabled), everything else the frontend DB.
    pub fn for_id(&self, id: i32) -> &PgPool {
        match metadata::find(id).map(|migration| migration.team) {
            Some("code-intelligence") => self.codeintel_or_frontend(),
            Some("code-insights") => self.insights_or_frontend(),
            _ => &self.frontend,
        }
    }
}

/// An error attached to a migration record (`store.go:62`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationError {
    pub message: String,
    pub created: OffsetDateTime,
}

impl MigrationError {
    pub fn new(message: impl Into<String>, created: OffsetDateTime) -> Self {
        Self {
            message: message.into(),
            created,
        }
    }
}

/// Out-of-band migration implementation.
///
/// Mirrors the requested Go interface surface: ID, introduced version, name,
/// description, non-idempotency, progress and the up/down batch functions.
#[async_trait]
pub trait Migrator: Send + Sync {
    /// `Migration.ID`.
    fn id(&self) -> i32;

    /// Suggested time between invocations (`MigratorOptions.Interval`).
    fn interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    /// Introduced version (`Migration.Introduced`), from the canonical YAML.
    fn version(&self) -> Version {
        metadata::find(self.id())
            .map(|migration| migration.introduced())
            .unwrap_or(Version::new(0, 0))
    }

    /// Human-readable component name (`Migration.Component`).
    fn name(&self) -> &'static str {
        metadata::find(self.id())
            .map(|migration| migration.component)
            .unwrap_or("")
    }

    /// Human-readable description (`Migration.Description`).
    fn description(&self) -> &'static str {
        metadata::find(self.id())
            .map(|migration| migration.description)
            .unwrap_or("")
    }

    /// Inverse of `Migration.NonDestructive`: `true` when running `Down`
    /// materially transforms data and must complete before a downgrade.
    fn non_idempotent(&self) -> bool {
        metadata::find(self.id())
            .map(|migration| !migration.non_destructive)
            .unwrap_or(false)
    }

    /// Percentage (`[0, 1]`) of records migrated in the requested direction.
    ///
    /// The pool is supplied per call (rather than stored on the migrator) so
    /// registration stays connection-agnostic.
    async fn progress(&self, db: &PgPool, apply_reverse: bool) -> Result<f64, MigratorError>;

    /// Run one batch in the forward direction.
    async fn up(&self, db: &PgPool) -> Result<(), MigratorError>;

    /// Run one batch in the reverse direction. Non-destructive migrations may
    /// implement this as a no-op.
    async fn down(&self, db: &PgPool) -> Result<(), MigratorError>;

    // Multi-database variants used by the runner. Migrators that only touch
    // the frontend database (the overwhelming majority) use these defaults;
    // code-intelligence and code-insights migrations override them.

    /// [`Migrator::progress`] with access to every family pool.
    async fn progress_with_pools(
        &self,
        pools: &MigratorPools,
        apply_reverse: bool,
    ) -> Result<f64, MigratorError> {
        self.progress(&pools.frontend, apply_reverse).await
    }

    /// [`Migrator::up`] with access to every family pool.
    async fn up_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.up(&pools.frontend).await
    }

    /// [`Migrator::down`] with access to every family pool.
    async fn down_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.down(&pools.frontend).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_tracks_direction() {
        let mut migration = Migration::default();

        migration.progress = 1.0;
        assert!(migration.complete());
        migration.apply_reverse = true;
        assert!(!migration.complete());

        migration.progress = 0.0;
        assert!(migration.complete());
        migration.apply_reverse = false;
        assert!(!migration.complete());

        migration.progress = 0.5;
        migration.apply_reverse = true;
        assert!(!migration.complete());
    }
}
