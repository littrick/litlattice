#![forbid(unsafe_code)]

//! Rust equivalent of the embedded `internal/oobmigration/oobmigrations.yaml`.
//!
//! The Go store reads this file to populate/refresh the
//! `out_of_band_migrations` metadata table. The Rust port keeps the same
//! definition as a static table so `SynchronizeMetadata`, `List`,
//! `ScheduleMigrationInterrupts` and the [`crate::oobmigration::registry`]
//! all agree on the canonical migration set.

use super::version::Version;

/// One entry of `oobmigrations.yaml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YamlMigration {
    pub id: i32,
    pub team: &'static str,
    pub component: &'static str,
    pub description: &'static str,
    pub non_destructive: bool,
    pub is_enterprise: bool,
    pub introduced_version_major: i32,
    pub introduced_version_minor: i32,
    pub deprecated_version_major: Option<i32>,
    pub deprecated_version_minor: Option<i32>,
}

impl YamlMigration {
    pub const fn introduced(&self) -> Version {
        Version::new(self.introduced_version_major, self.introduced_version_minor)
    }

    pub const fn deprecated(&self) -> Option<Version> {
        match self.deprecated_version_major {
            Some(major) => Some(Version::new(
                major,
                // The YAML always specifies both fields together.
                match self.deprecated_version_minor {
                    Some(minor) => minor,
                    None => 0,
                },
            )),
            None => None,
        }
    }
}

macro_rules! yaml_migration {
    ($id:literal, $team:literal, $component:literal, $description:literal, $non_destructive:literal, $is_enterprise:literal, $imajor:literal, $iminor:literal, $dmajor:expr, $dminor:expr) => {
        YamlMigration {
            id: $id,
            team: $team,
            component: $component,
            description: $description,
            non_destructive: $non_destructive,
            is_enterprise: $is_enterprise,
            introduced_version_major: $imajor,
            introduced_version_minor: $iminor,
            deprecated_version_major: $dmajor,
            deprecated_version_minor: $dminor,
        }
    };
}

/// The canonical out-of-band migration definitions, sorted by ID.
pub const MIGRATIONS: &[YamlMigration] = &[
    yaml_migration!(1, "code-intelligence", "codeintel-db.lsif_data_documents", "Populate num_diagnostics from gob-encoded payload", true, true, 3, 25, Some(4), Some(0)),
    yaml_migration!(2, "campaigns", "frontend-db.authenticators", "Prepare for SSH pushes to code hosts", true, true, 3, 26, Some(4), Some(0)),
    yaml_migration!(4, "code-intelligence", "codeintel-db.lsif_data_definitions", "Populate num_locations from gob-encoded payload", true, true, 3, 26, Some(4), Some(0)),
    yaml_migration!(5, "code-intelligence", "codeintel-db.lsif_data_references", "Populate num_locations from gob-encoded payload", true, true, 3, 26, Some(4), Some(0)),
    yaml_migration!(7, "code-intelligence", "codeintel-db.lsif_data_documents", "Split payload into multiple columns", false, true, 3, 27, Some(4), Some(0)),
    yaml_migration!(13, "batch-changes", "frontend-db.external_services", "Calculate the webhook state of each external service", true, false, 3, 34, Some(4), Some(0)),
    yaml_migration!(14, "code-insights", "db.insights_settings_migration_jobs", "Migrating insight definitions from settings files to database tables as a last stage to use the GraphQL API.", true, true, 3, 35, Some(4), Some(0)),
    yaml_migration!(15, "iam", "frontend-db.product_subscriptions", "Attempt to extract account number from the associated user of subscriptions.", true, true, 3, 43, Some(4), Some(0)),
    yaml_migration!(16, "iam", "frontend-db.product_licenses", "Extract license fields from the license key text and store them to dedicated columns.", true, true, 3, 43, Some(4), Some(0)),
    yaml_migration!(17, "code-insights", "codeinsights-db.insight_series", "Save historical recording times for all existing insights", true, true, 4, 3, Some(4), Some(4)),
    yaml_migration!(18, "code-insights", "db.insights_backfillerv2_migration", "Migrates insight series from the v1 backfiller to v2.", true, true, 4, 3, Some(4), Some(4)),
    yaml_migration!(19, "batch-changes", "frontend-db.user_roles", "Assigns roles to existing users", true, false, 4, 5, Some(4), Some(6)),
    yaml_migration!(20, "code-intelligence", "codeintel-db.lsif_data_*", "Convert LSIF data to SCIP.", false, true, 4, 5, Some(4), Some(6)),
    yaml_migration!(21, "batch-changes", "frontend-db.changesets", "Populate external_fork_name column for existing changesets published on Bitbucket Server and Cloud.", true, true, 4, 5, None, None),
    yaml_migration!(22, "iam", "frontend-db.user_repo_permissions", "Migrate data from user_permissions table to unified user_repo_permissions.", true, true, 5, 0, None, None),
    yaml_migration!(23, "batch-changes", "frontend-db.batch_specs", "Re-ID empty batch specs belonging to draft batch changes.", true, true, 5, 0, None, None),
];

/// Look up a definition by migration ID.
pub fn find(id: i32) -> Option<&'static YamlMigration> {
    MIGRATIONS.iter().find(|migration| migration.id == id)
}

/// All canonical migration IDs, ascending.
pub fn ids() -> Vec<i32> {
    MIGRATIONS.iter().map(|migration| migration.id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definitions_are_sorted_and_unique() {
        let mut previous = 0;
        for migration in MIGRATIONS {
            assert!(migration.id > previous, "IDs must ascend");
            previous = migration.id;
        }
        assert_eq!(MIGRATIONS.len(), 16);
        assert_eq!(find(21).unwrap().deprecated(), None);
        assert_eq!(find(20).unwrap().deprecated(), Some(Version::new(4, 6)));
    }
}
