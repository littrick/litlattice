#![forbid(unsafe_code)]

//! `internal/oobmigration/migrations/register/register.go`: the migrator
//! registry equivalent of `RegisterOSSMigrators` / `RegisterEnterpriseMigrators`.
//!
//! Every canonical migration ID is registered exactly once. Families that have
//! not been ported still appear in the registry as [`UnportedMigrator`]s so
//! that running them fails with an explicit message instead of being silently
//! skipped.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;

use crate::error::MigratorError;

use super::model::Migrator;
use super::{batches, codeintel, iam, insights, metadata};

/// A registered migration implementation.
pub type SharedMigrator = Arc<dyn Migrator>;

/// Placeholder for a migration whose Go implementation has not been ported.
///
/// Every operation returns a clear error; the migration remains visible in the
/// registry (and its metadata is still synchronized).
#[derive(Debug, Clone)]
pub struct UnportedMigrator {
    pub id: i32,
    pub family: &'static str,
    pub reason: &'static str,
}

impl UnportedMigrator {
    pub fn new(id: i32, family: &'static str, reason: &'static str) -> Self {
        Self { id, family, reason }
    }

    fn error(&self) -> MigratorError {
        MigratorError::new(format!(
            "out-of-band migration {} ({}) is not ported to Rust: {}",
            self.id, self.family, self.reason
        ))
    }
}

#[async_trait]
impl Migrator for UnportedMigrator {
    fn id(&self) -> i32 {
        self.id
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn progress(&self, _db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        Err(self.error())
    }

    async fn up(&self, _db: &PgPool) -> Result<(), MigratorError> {
        Err(self.error())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        Err(self.error())
    }
}

/// Registry of migrator implementations (`oobmigration.Runner`'s
/// `migrators` map).
#[derive(Default)]
pub struct MigratorRegistry {
    migrators: BTreeMap<i32, SharedMigrator>,
}

impl MigratorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Runner.Register`: an error is returned when the ID is already present.
    pub fn register(&mut self, migrator: SharedMigrator) -> Result<(), MigratorError> {
        let id = migrator.id();
        if self.migrators.contains_key(&id) {
            return Err(MigratorError::new(format!(
                "migrator {} already registered",
                id
            )));
        }

        self.migrators.insert(id, migrator);
        Ok(())
    }

    pub fn contains(&self, id: i32) -> bool {
        self.migrators.contains_key(&id)
    }

    pub fn get(&self, id: i32) -> Option<&SharedMigrator> {
        self.migrators.get(&id)
    }

    pub fn ids(&self) -> Vec<i32> {
        self.migrators.keys().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.migrators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.migrators.is_empty()
    }

    /// All registered migrators, ascending by ID.
    pub fn all(&self) -> Vec<SharedMigrator> {
        self.migrators.values().cloned().collect()
    }

    /// IDs backed by an [`UnportedMigrator`].
    pub fn unported_ids(&self) -> Vec<i32> {
        self.migrators
            .iter()
            .filter_map(|(id, migrator)| {
                // The trait object cannot be downcast without `Any`; use the
                // canonical reason tables instead.
                let _ = migrator;
                is_unported_id(*id).then_some(*id)
            })
            .collect()
    }
}

/// Canonical IDs that the Rust port does not implement.
pub fn unported_ids() -> Vec<i32> {
    let mut ids: Vec<i32> = Vec::new();
    for (id, _) in batches::UNPORTED
        .iter()
        .chain(codeintel::UNPORTED.iter())
        .chain(insights::UNPORTED.iter())
    {
        ids.push(*id);
    }
    ids.sort_unstable();
    ids
}

fn is_unported_id(id: i32) -> bool {
    batches::UNPORTED
        .iter()
        .any(|(candidate, _)| *candidate == id)
        || codeintel::UNPORTED
            .iter()
            .any(|(candidate, _)| *candidate == id)
        || insights::UNPORTED
            .iter()
            .any(|(candidate, _)| *candidate == id)
}

/// `registerOSSMigrators` (`register.go:65`).
pub fn register_oss_migrators(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    batches::register_oss(registry)?;
    iam::register_oss(registry)?;
    insights::register_oss(registry)?;
    codeintel::register_oss(registry)
}

/// `registerEnterpriseMigrators` (`register.go:158`).
pub fn register_enterprise_migrators(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    batches::register_enterprise(registry)?;
    iam::register_enterprise(registry)?;
    insights::register_enterprise(registry)?;
    codeintel::register_enterprise(registry)
}

pub(crate) fn register_unported_family(
    registry: &mut MigratorRegistry,
    family: &'static str,
    unported: &'static [(i32, &'static str)],
    enterprise: bool,
) -> Result<(), MigratorError> {
    for (id, reason) in unported {
        let is_enterprise = metadata::find(*id)
            .map(|migration| migration.is_enterprise)
            .unwrap_or(false);
        if is_enterprise != enterprise {
            continue;
        }

        registry.register(Arc::new(UnportedMigrator::new(*id, family, reason)))?;
    }

    Ok(())
}

/// Both registration sets, as used by the standalone CLI runtime.
pub fn full_registry() -> Result<MigratorRegistry, MigratorError> {
    let mut registry = MigratorRegistry::new();
    register_oss_migrators(&mut registry)?;
    register_enterprise_migrators(&mut registry)?;
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_registry_covers_every_canonical_id() {
        let registry = full_registry().unwrap();
        assert_eq!(registry.ids(), metadata::ids());
        assert_eq!(registry.len(), 16);
    }

    #[test]
    fn registry_partitions_ported_and_unported() {
        let registry = full_registry().unwrap();
        let unported = registry.unported_ids();

        assert!(
            unported.is_empty(),
            "expected every migration ported: {unported:?}"
        );
        assert_eq!(registry.len(), metadata::ids().len());
        for id in metadata::ids() {
            assert!(registry.contains(id), "expected ported migration {id}");
        }
    }

    #[test]
    fn rejects_duplicate_registration() {
        let mut registry = MigratorRegistry::new();
        register_oss_migrators(&mut registry).unwrap();
        let error = register_oss_migrators(&mut registry).unwrap_err();
        assert!(error.to_string().contains("already registered"));
    }

    #[tokio::test]
    async fn unported_migrator_returns_explicit_error() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/oobmigration")
            .expect("lazy pool");

        let migrator = UnportedMigrator::new(20, "codeintel", "SCIP conversion");
        let progress_error = migrator.progress(&pool, false).await.unwrap_err();
        assert!(progress_error.to_string().contains("is not ported to Rust"));
        assert!(progress_error.to_string().contains("codeintel"));

        let up_error = migrator.up(&pool).await.unwrap_err();
        assert!(up_error.to_string().contains("migration 20"));
    }
}
