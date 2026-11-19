#![forbid(unsafe_code)]

//! `internal/oobmigration`: the out-of-band migration runtime and its
//! registration surface (`run-out-of-band-migrations`).
//!
//! This is a port of:
//!
//! * `internal/oobmigration/{iface,migrator,runner,store,version,interrupts,upgrade,downgrade}.go`
//! * `internal/oobmigration/migrations/register/register.go`
//! * `internal/database/migration/multiversion/run.go` (`RunOutOfBandMigrations`)
//!
//! The runtime drives registered migrators over the `out_of_band_migrations`
//! table. The CLI semantics are preserved: optional ID filtering, forward or
//! reverse (`apply_reverse`) execution, dry-run short-circuiting, metadata
//! synchronization and progress polling.
//!
//! # Deviations from Go
//!
//! * Migrators receive a single [`sqlx::PgPool`] instead of Go's per-service
//!   `basestore.Store`. Code-intelligence and code-insights migrations in Go
//!   use separate databases; this port assumes the pool handed to
//!   [`run_out_of_band_migrations`] can reach the relevant tables.
//! * The runner drives all migrations from one async loop rather than one
//!   goroutine/ticker per migration. Observable state transitions (direction,
//!   progress, error records) match the Go runner.
//! * Unported migration families are registered as explicit
//!   [`registry::UnportedMigrator`]s that return a descriptive error.
//! * Go's `insights.IsEnabled()` also disables code insights for
//!   single-container deployments; the port honours `DISABLE_CODE_INSIGHTS`.

use sqlx::PgPool;

use crate::error::MigratorError;

pub mod batches;
pub mod codeintel;
pub mod iam;
pub mod insights;
pub mod interrupts;
pub mod metadata;
pub mod model;
pub mod registry;
pub mod runner;
pub mod store;
pub mod version;

pub use interrupts::{schedule_migration_interrupts, MigrationInterrupt};
pub use metadata::YamlMigration;
pub use model::{Migration, MigrationError, Migrator};
pub use registry::{
    full_registry, register_enterprise_migrators, register_oss_migrators, MigratorRegistry,
    UnportedMigrator,
};
pub use runner::{wrap_migration_errors, Runner, DEFAULT_REFRESH_INTERVAL};
pub use store::Store;
pub use version::{compare_versions, upgrade_range, Version, VersionOrder};

/// Options for `run-out-of-band-migrations`.
#[derive(Debug, Clone, Default)]
pub struct RunOutOfBandOptions {
    pub ids: Vec<i32>,
    pub apply_reverse: bool,
    pub dry_run: bool,
    pub disable_animation: bool,
}

/// How [`run_out_of_band_migrations`] should proceed. Factored out so the
/// dry-run/ID-selection decisions are unit-testable without a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPlan {
    /// A dry run with explicit IDs: report and return before opening the DB.
    DryRunWithoutDb,
    /// A dry run over all migrations: metadata sync + listing still happen,
    /// then the run is skipped.
    DryRunAfterListing,
    /// Perform the migration.
    Execute,
}

/// Decide the run plan from the options (`multiversion/run.go:176-206`).
pub fn plan_out_of_band_run(options: &RunOutOfBandOptions) -> RunPlan {
    if options.dry_run {
        if options.ids.is_empty() {
            RunPlan::DryRunAfterListing
        } else {
            RunPlan::DryRunWithoutDb
        }
    } else {
        RunPlan::Execute
    }
}

/// Select the IDs to operate on: explicit IDs win, otherwise every migration
/// currently in the store. Sorted ascending, matching Go's `sort.Ints`.
pub fn select_migration_ids(requested: &[i32], stored: &[i32]) -> Vec<i32> {
    let mut ids = if requested.is_empty() {
        stored.to_vec()
    } else {
        requested.to_vec()
    };
    ids.sort_unstable();
    ids
}

/// Apply-reverse direction decision. In Go the caller passes `up = !applyReverse`,
/// then `UpdateDirection(ids, !up)`; the net effect is the value below.
pub fn apply_reverse_for(options: &RunOutOfBandOptions) -> bool {
    options.apply_reverse
}

/// `multiversion.RunOutOfBandMigrations` (`run.go:166`).
pub async fn run_out_of_band_migrations(
    pool: &PgPool,
    options: &RunOutOfBandOptions,
) -> Result<(), MigratorError> {
    run_out_of_band_migrations_with_pools(pool, None, None, options).await
}

/// Like [`run_out_of_band_migrations`], with the code-intelligence and
/// code-insights database handles the enterprise migrators run against
/// (`register.RegisterEnterpriseMigrators`). `None` falls back to the frontend
/// pool for that family.
pub async fn run_out_of_band_migrations_with_pools(
    pool: &PgPool,
    codeintel_pool: Option<&PgPool>,
    insights_pool: Option<&PgPool>,
    options: &RunOutOfBandOptions,
) -> Result<(), MigratorError> {
    match plan_out_of_band_run(options) {
        RunPlan::DryRunWithoutDb => {
            tracing::info!(
                "Running out of band migrations {}",
                display_ids(&options.ids)
            );
            return Ok(());
        }
        RunPlan::DryRunAfterListing | RunPlan::Execute => {}
    }

    if !options.ids.is_empty() {
        tracing::info!(
            "Running out of band migrations {}",
            display_ids(&options.ids)
        );
    }

    let store = Store::new(pool.clone());
    let mut runner = Runner::new_with_pools(
        runner::MigratorPools {
            frontend: pool.clone(),
            codeintel: codeintel_pool.cloned(),
            insights: insights_pool.cloned(),
        },
        DEFAULT_REFRESH_INTERVAL,
    );

    runner.synchronize_metadata().await?;

    let registry = full_registry()?;
    for migrator in registry.all() {
        let interval = migrator.interval();
        runner.register(migrator, Some(interval))?;
    }

    let stored_ids = if options.ids.is_empty() {
        store
            .list()
            .await?
            .into_iter()
            .map(|migration| migration.id)
            .collect()
    } else {
        Vec::new()
    };
    let ids = select_migration_ids(&options.ids, &stored_ids);

    if options.dry_run {
        return Ok(());
    }

    // Go passes `up` and then negates it; the persisted flag is apply_reverse.
    runner
        .update_direction(&ids, apply_reverse_for(options))
        .await?;

    runner.run_partial(&ids).await?;

    tracing::info!("Out of band migrations complete");
    Ok(())
}

fn display_ids(ids: &[i32]) -> String {
    let joined = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{joined}]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dry_run_with_ids_skips_the_database() {
        let options = RunOutOfBandOptions {
            ids: vec![3, 1],
            dry_run: true,
            ..Default::default()
        };
        assert_eq!(plan_out_of_band_run(&options), RunPlan::DryRunWithoutDb);
    }

    #[test]
    fn dry_run_without_ids_lists_then_skips() {
        let options = RunOutOfBandOptions {
            ids: Vec::new(),
            dry_run: true,
            ..Default::default()
        };
        assert_eq!(plan_out_of_band_run(&options), RunPlan::DryRunAfterListing);
    }

    #[test]
    fn non_dry_run_executes() {
        let options = RunOutOfBandOptions {
            ids: vec![1],
            dry_run: false,
            ..Default::default()
        };
        assert_eq!(plan_out_of_band_run(&options), RunPlan::Execute);
    }

    #[test]
    fn selects_explicit_ids_over_stored() {
        assert_eq!(select_migration_ids(&[9, 2], &[1, 2, 3]), vec![2, 9]);
    }

    #[test]
    fn falls_back_to_stored_ids_sorted() {
        assert_eq!(select_migration_ids(&[], &[3, 1, 2]), vec![1, 2, 3]);
    }

    #[test]
    fn apply_reverse_is_passed_through() {
        let options = RunOutOfBandOptions {
            apply_reverse: true,
            ..Default::default()
        };
        assert!(apply_reverse_for(&options));
    }

    #[test]
    fn formats_ids_like_go() {
        assert_eq!(display_ids(&[1, 2, 3]), "[1 2 3]");
    }
}
