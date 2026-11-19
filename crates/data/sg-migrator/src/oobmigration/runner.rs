#![forbid(unsafe_code)]

//! `internal/oobmigration/runner.go`: correlate migration records with
//! registered migrators and drive them to completion.
//!
//! Go runs one goroutine per migration ticked independently; this port drives
//! the same state machine from a single async loop. Each iteration reads the
//! migration records, refreshes progress from the migrator (detecting
//! desynchronisation with the stored value), runs one batch in the selected
//! direction for incomplete migrations, and sleeps for the refresh interval.
//! Errors raised while computing progress are surfaced to the caller; errors
//! raised by a batch are attached to the migration record, mirroring
//! `runMigrationFunction`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::error::MigratorError;

pub use super::model::{Migration, Migrator, MigratorPools};
use super::store::Store;
use super::version::{compare_versions, Version, VersionOrder};

/// Run away from migrations for this long between refresh/run passes.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

struct RegisteredMigrator {
    migrator: Arc<dyn Migrator>,
    /// Retained for parity with `MigratorOptions.Interval`; the CLI runner
    /// performs one batch per refresh pass.
    _interval: Duration,
}

/// `oobmigration.Runner`.
pub struct Runner {
    store: Store,
    /// The database each migrator family runs against: frontend by default,
    /// code-intelligence and code-insights stores for their families
    /// (`register.RegisterEnterpriseMigrators`).
    migrator_pools: MigratorPools,
    migrators: BTreeMap<i32, RegisteredMigrator>,
    refresh_interval: Duration,
    stopped: AtomicBool,
}

impl Runner {
    pub fn new(pool: PgPool, refresh_interval: Duration) -> Self {
        Self::new_with_pools(MigratorPools::frontend_only(pool), refresh_interval)
    }

    pub fn new_with_pools(migrator_pools: MigratorPools, refresh_interval: Duration) -> Self {
        Self {
            store: Store::new(migrator_pools.frontend.clone()),
            migrator_pools,
            migrators: BTreeMap::new(),
            refresh_interval,
            stopped: AtomicBool::new(false),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// `Runner.SynchronizeMetadata`.
    pub async fn synchronize_metadata(&self) -> Result<(), MigratorError> {
        self.store.synchronize_metadata().await
    }

    /// `Runner.Register` (`runner.go:81`).
    pub fn register(
        &mut self,
        migrator: Arc<dyn Migrator>,
        interval: Option<Duration>,
    ) -> Result<(), MigratorError> {
        let id = migrator.id();
        if self.migrators.contains_key(&id) {
            return Err(MigratorError::new(format!(
                "migrator {} already registered",
                id
            )));
        }

        let interval = interval.unwrap_or_else(|| migrator.interval());
        self.migrators.insert(
            id,
            RegisteredMigrator {
                migrator,
                _interval: interval,
            },
        );

        Ok(())
    }

    /// Registered migration IDs, ascending.
    pub fn registered_ids(&self) -> Vec<i32> {
        self.migrators.keys().copied().collect()
    }

    /// `Runner.Validate` (`runner.go:128`). Checks the persisted migration
    /// records against the running version and the instance's first version.
    pub async fn validate(
        &self,
        current_version: Version,
        first_version: Version,
    ) -> Result<(), MigratorError> {
        let migrations = self.store.list().await?;

        let mut errors = Vec::new();
        for migration in migrations {
            let introduced_order = compare_versions(current_version, migration.introduced);
            if introduced_order == VersionOrder::Before && migration.progress != 0.0 {
                // Unfinished rollback: current version before introduction and
                // progress > 0.
                errors.push(migration_status_error(
                    migration.id,
                    0.0,
                    migration.progress,
                ));
            }

            let Some(deprecated) = migration.deprecated else {
                continue;
            };

            if compare_versions(first_version, deprecated) != VersionOrder::Before {
                // Edge case: instance booted on or after deprecation version.
                continue;
            }

            let deprecated_order = compare_versions(current_version, deprecated);
            if deprecated_order != VersionOrder::Before && migration.progress != 1.0 {
                // Unfinished migration: current version on or after deprecation
                // and progress < 1.
                errors.push(migration_status_error(
                    migration.id,
                    1.0,
                    migration.progress,
                ));
            }
        }

        wrap_migration_errors(&errors)
    }

    /// `Runner.UpdateDirection` (`runner.go:180`): apply the direction to every
    /// ID atomically.
    pub async fn update_direction(
        &self,
        ids: &[i32],
        apply_reverse: bool,
    ) -> Result<(), MigratorError> {
        self.store.update_directions(ids, apply_reverse).await
    }

    /// Ask the run loop to stop after the current pass.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// `Runner.Start` (`runner.go:202`): run migrations active for the given
    /// version bounds.
    pub async fn run_standard(
        &self,
        current_version: Version,
        first_version: Version,
    ) -> Result<(), MigratorError> {
        self.run_filtered(|migration| {
            is_migration_in_version_bounds(migration, current_version, first_version)
        })
        .await
    }

    /// `Runner.StartPartial` (`runner.go:223`): run only the given IDs. An
    /// empty set means every registered migration.
    pub async fn run_partial(&self, ids: &[i32]) -> Result<(), MigratorError> {
        let selected: BTreeSet<i32> = ids.iter().copied().collect();
        self.run_filtered(|migration| migration_is_selected(migration.id, &selected))
            .await
    }

    async fn run_filtered<F>(&self, should_run: F) -> Result<(), MigratorError>
    where
        F: Fn(&Migration) -> bool,
    {
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return Ok(());
            }

            let migrations = self.store.list().await?;
            let mut all_complete = true;
            let mut considered = false;

            for record in migrations {
                if !should_run(&record) {
                    continue;
                }

                let Some(registered) = self.migrators.get(&record.id) else {
                    continue;
                };
                considered = true;

                // Refresh from the migrator in case the stored progress has
                // drifted from the actual data.
                let progress = registered
                    .migrator
                    .progress_with_pools(&self.migrator_pools, record.apply_reverse)
                    .await?;
                self.store.update_progress(record.id, progress).await?;

                let mut migration = record;
                migration.progress = progress;

                if migration.complete() {
                    continue;
                }

                // A non-destructive migration has nothing to undo.
                if migration.apply_reverse && migration.non_destructive {
                    continue;
                }

                all_complete = false;
                self.run_migration_function(&migration, registered.migrator.as_ref())
                    .await?;
            }

            if !considered || all_complete {
                return Ok(());
            }

            tokio::time::sleep(self.refresh_interval).await;
        }
    }

    /// `runMigrationFunction` (`runner.go:409`): run one batch and record any
    /// error against the migration, then refresh progress.
    async fn run_migration_function(
        &self,
        migration: &Migration,
        migrator: &dyn Migrator,
    ) -> Result<(), MigratorError> {
        let result = if migration.apply_reverse {
            migrator.down_with_pools(&self.migrator_pools).await
        } else {
            migrator.up_with_pools(&self.migrator_pools).await
        };

        if let Err(error) = result {
            // The batch may have made partial progress before failing, so the
            // error is attached to the record and progress is refreshed below.
            self.store
                .add_error(migration.id, &error.to_string())
                .await?;
        }

        let progress = migrator
            .progress_with_pools(&self.migrator_pools, migration.apply_reverse)
            .await?;
        self.store.update_progress(migration.id, progress).await?;

        Ok(())
    }
}

/// `Runner.Start`'s filter (`runner.go:203`).
pub fn is_migration_in_version_bounds(
    migration: &Migration,
    current_version: Version,
    first_version: Version,
) -> bool {
    if compare_versions(current_version, migration.introduced) == VersionOrder::Before {
        return false;
    }

    if let Some(deprecated) = migration.deprecated {
        if compare_versions(first_version, deprecated) == VersionOrder::After {
            return false;
        }
    }

    migration.deprecated.is_none()
        || migration
            .deprecated
            .map(|deprecated| compare_versions(current_version, deprecated) == VersionOrder::Before)
            .unwrap_or(true)
}

/// `StartPartial`'s filter: an empty selection matches everything.
pub fn migration_is_selected(id: i32, selected: &BTreeSet<i32>) -> bool {
    selected.is_empty() || selected.contains(&id)
}

/// `migrationStatusError` message (`runner.go:113`).
pub fn migration_status_error(
    id: i32,
    expected_progress: f64,
    actual_progress: f64,
) -> MigratorError {
    MigratorError::new(format!(
        "migration {} expected to be at {:.2}% (at {:.2}%)",
        id,
        expected_progress * 100.0,
        actual_progress * 100.0
    ))
}

/// `wrapMigrationErrors` (`runner.go:162`).
pub fn wrap_migration_errors(errors: &[MigratorError]) -> Result<(), MigratorError> {
    if errors.is_empty() {
        return Ok(());
    }

    let mut descriptions: Vec<String> = errors
        .iter()
        .map(|error| format!("  - {error}\n"))
        .collect();
    descriptions.sort();

    Err(MigratorError::new(format!(
        "Unfinished migrations. Please revert Sourcegraph to the previous version and wait for the following migrations to complete.\n\n{}\n",
        descriptions.join("\n")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oobmigration::version::Version;

    fn migration_at(progress: f64, apply_reverse: bool) -> Migration {
        Migration {
            progress,
            apply_reverse,
            ..Migration::default()
        }
    }

    #[test]
    fn empty_selection_matches_everything() {
        let selected = BTreeSet::new();
        assert!(migration_is_selected(7, &selected));
        assert!(migration_is_selected(99, &selected));
    }

    #[test]
    fn non_empty_selection_filters_ids() {
        let selected: BTreeSet<i32> = [4, 7].into_iter().collect();
        assert!(migration_is_selected(4, &selected));
        assert!(migration_is_selected(7, &selected));
        assert!(!migration_is_selected(5, &selected));
    }

    #[test]
    fn version_bounds_filter() {
        let mut open = migration_at(0.5, false);
        open.introduced = Version::new(3, 10);

        // Introduced after the current version: not active.
        assert!(!is_migration_in_version_bounds(
            &open,
            Version::new(3, 9),
            Version::new(0, 0)
        ));
        // Active, not deprecated.
        assert!(is_migration_in_version_bounds(
            &open,
            Version::new(3, 10),
            Version::new(0, 0)
        ));

        let mut deprecated = open.clone();
        deprecated.deprecated = Some(Version::new(3, 12));

        // Deprecated at or before the current version: no longer active.
        assert!(!is_migration_in_version_bounds(
            &deprecated,
            Version::new(3, 12),
            Version::new(0, 0)
        ));
        // Instance was initialized after deprecation.
        assert!(!is_migration_in_version_bounds(
            &deprecated,
            Version::new(3, 11),
            Version::new(3, 13)
        ));
        assert!(is_migration_in_version_bounds(
            &deprecated,
            Version::new(3, 11),
            Version::new(3, 0)
        ));
    }

    #[test]
    fn formats_validation_errors_like_go() {
        let error = migration_status_error(1, 1.0, 0.65);
        assert_eq!(
            error.to_string(),
            "migration 1 expected to be at 100.00% (at 65.00%)"
        );

        let wrapped = wrap_migration_errors(&[error]).unwrap_err().to_string();
        assert_eq!(
            wrapped,
            "Unfinished migrations. Please revert Sourcegraph to the previous version and wait for the following migrations to complete.\n\n  - migration 1 expected to be at 100.00% (at 65.00%)\n\n"
        );
    }
}
