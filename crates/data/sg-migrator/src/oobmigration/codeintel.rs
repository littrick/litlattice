#![forbid(unsafe_code)]

//! `internal/oobmigration/migrations/codeintel/lsif`: code-intelligence
//! migrations.
//!
//! These migrations operate on LSIF records whose `data` columns are
//! `gzip(encoding/gob(value))`, and the SCIP migration additionally converts
//! LSIF documents to SCIP protobufs. The baseline registers every one of them
//! from `registerEnterpriseMigrators` (none are OSS).
//!
//! IDs:
//! * `1`  diagnostics count   (`lsif_data_documents.num_diagnostics`)
//! * `4`  definitions count   (`lsif_data_definitions.num_locations`)
//! * `5`  references count    (`lsif_data_references.num_locations`)
//! * `7`  document column split (`lsif_data_documents` payload columns)
//! * `20` LSIF -> SCIP conversion
//!
//! The gob/gzip codecs live in [`gob`] and [`gzip`] because the workspace has
//! no gob or flate2 dependency available to this crate.

mod gob;
mod gzip;
mod lsif;
mod migrator;
mod scip;
mod serializer;
mod types;

use crate::error::MigratorError;

use super::registry::{register_unported_family, MigratorRegistry};

/// Migrations in this family that still have no Rust implementation.
///
/// Empty: all five code-intelligence migrations are ported and registered by
/// [`register_enterprise`].
pub const UNPORTED: &[(i32, &str)] = &[];

/// `registerOSSMigrators` for the codeintel family. The baseline registers no
/// code-intelligence migrations from the OSS path.
pub fn register_oss(_registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    Ok(())
}

/// `registerEnterpriseMigrators` for the codeintel family.
pub fn register_enterprise(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    for migrator in lsif::migrators() {
        registry.register(migrator)?;
    }
    for migrator in scip::migrators() {
        registry.register(migrator)?;
    }

    // All code-intelligence IDs are ported, so no placeholder migrators are
    // registered.
    register_unported_family(registry, "codeintel", UNPORTED, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn registers_all_five_canonical_ids() {
        let mut registry = MigratorRegistry::new();
        register_oss(&mut registry).unwrap();
        register_enterprise(&mut registry).unwrap();

        assert_eq!(registry.len(), 5);
        for id in [1, 4, 5, 7, 20] {
            assert!(registry.contains(id), "expected migration {id}");
            assert_eq!(registry.get(id).unwrap().interval(), Duration::from_secs(1));
        }
    }
}
