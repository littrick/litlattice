#![forbid(unsafe_code)]

//! `internal/database/migration/schemas`: schema names, migration table names,
//! and definition loading from a migration corpus directory.

use std::path::Path;

use crate::error::MigratorError;
use crate::types::Schema;

/// `schemas.Schemas` order.
pub const SCHEMA_NAMES: [&str; 3] = ["frontend", "codeintel", "codeinsights"];

/// `schemas.SchemaNames` (`names.go`): the schema names in `Schemas` order.
pub fn schema_names() -> &'static [&'static str] {
    &SCHEMA_NAMES
}

/// Whether `name` is one of the known schema names (`frontend`, `codeintel`,
/// `codeinsights`).
pub fn is_known_schema(name: &str) -> bool {
    SCHEMA_NAMES.contains(&name)
}

/// Validate a schema name, mirroring the unknown-name branch of the Go
/// resolver.
pub fn validate_schema_name(name: &str) -> Result<(), MigratorError> {
    if is_known_schema(name) {
        Ok(())
    } else {
        Err(MigratorError::UnknownSchema(name.to_string()))
    }
}

/// `MigrationsTableName`: `frontend` keeps the legacy `schema_migrations`
/// name, other schemas use `<name>_schema_migrations`.
///
/// The Go implementation formats `<name>_schema_migrations` and strips one
/// leading `frontend_` prefix, which this reproduces exactly.
pub fn migrations_table_name(name: &str) -> String {
    let formatted = format!("{name}_schema_migrations");
    formatted
        .strip_prefix("frontend_")
        .unwrap_or(&formatted)
        .to_string()
}

/// `ResolveSchema(fs, name)`: read `migrations/<name>/` under `root`.
pub fn resolve_schema(root: &Path, name: &str) -> Result<Schema, MigratorError> {
    validate_schema_name(name)?;

    let definitions = crate::definition::read_definitions(root, &format!("migrations/{name}"))
        .map_err(|error| MigratorError::MalformedDefinitions {
            schema: name.to_string(),
            detail: error.to_string(),
        })?;

    Ok(Schema {
        name: name.to_string(),
        migrations_table_name: migrations_table_name(name),
        definitions,
    })
}

/// `schemas.Schemas`: resolve every schema in [`SCHEMA_NAMES`] order.
/// Resolve `<root>/<name>/` where `root` is the corpus directory itself
/// (`/migrations` in the image, `migrations/` in a Sourcegraph checkout).
pub fn resolve_schema_in(root: &Path, name: &str) -> Result<Schema, MigratorError> {
    validate_schema_name(name)?;
    let definitions = crate::definition::read_definitions(root, name).map_err(|error| {
        MigratorError::MalformedDefinitions {
            schema: name.to_owned(),
            detail: error.to_string(),
        }
    })?;
    Ok(Schema {
        name: name.to_owned(),
        migrations_table_name: migrations_table_name(name),
        definitions,
    })
}

/// Resolve every known schema from a corpus directory.
pub fn resolve_all_in(root: &Path) -> Result<Vec<Schema>, MigratorError> {
    SCHEMA_NAMES
        .iter()
        .map(|name| resolve_schema_in(root, name))
        .collect()
}

pub fn resolve_all(root: &Path) -> Result<Vec<Schema>, MigratorError> {
    SCHEMA_NAMES
        .iter()
        .map(|name| resolve_schema(root, name))
        .collect()
}

/// `FilterSchemasByName`: keep the schemas whose name is in `names`,
/// preserving the order of `schemas` (the order of `names` is irrelevant).
pub fn filter_by_names(schemas: Vec<Schema>, names: &[String]) -> Vec<Schema> {
    schemas
        .into_iter()
        .filter(|schema| names.iter().any(|name| name == &schema.name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Definitions;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Removes its directory when dropped so test fixtures do not accumulate
    /// under the system temp directory.
    struct TempDir(PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_temp_dir() -> TempDir {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        TempDir(std::env::temp_dir().join(format!(
            "sg-migrator-schemas-{}-{nanos}",
            std::process::id()
        )))
    }

    fn schema(name: &str) -> Schema {
        Schema::new(name, migrations_table_name(name), Definitions::default())
    }

    fn names(schemas: &[Schema]) -> Vec<&str> {
        schemas.iter().map(|schema| schema.name.as_str()).collect()
    }

    #[test]
    fn migrations_table_names_match_golang_migrate() {
        assert_eq!(migrations_table_name("frontend"), "schema_migrations");
        assert_eq!(
            migrations_table_name("codeintel"),
            "codeintel_schema_migrations"
        );
        assert_eq!(
            migrations_table_name("codeinsights"),
            "codeinsights_schema_migrations"
        );
        // Go strips a single leading `frontend_` from the formatted name.
        assert_eq!(
            migrations_table_name("frontend_extra"),
            "extra_schema_migrations"
        );
    }

    #[test]
    fn schema_name_validation_rejects_unknown_names() {
        for name in SCHEMA_NAMES {
            assert!(is_known_schema(name));
            assert_eq!(validate_schema_name(name), Ok(()));
        }

        assert!(!is_known_schema("bogus"));
        assert_eq!(
            validate_schema_name("bogus"),
            Err(MigratorError::UnknownSchema("bogus".to_string()))
        );
    }

    #[test]
    fn resolve_schema_rejects_unknown_schema_before_reading_definitions() {
        // The path does not exist; validation must fail first regardless.
        let error = resolve_schema(Path::new("/nonexistent/sg-migrator"), "bogus")
            .expect_err("unknown schema must be rejected");
        assert_eq!(error, MigratorError::UnknownSchema("bogus".to_string()));
    }

    #[test]
    fn filter_by_names_keeps_schema_order_and_ignores_target_order() {
        let all = vec![
            schema("frontend"),
            schema("codeintel"),
            schema("codeinsights"),
        ];

        let filtered = filter_by_names(
            all.clone(),
            &["codeinsights".to_string(), "frontend".to_string()],
        );
        assert_eq!(names(&filtered), vec!["frontend", "codeinsights"]);

        // Duplicate target names do not duplicate schemas.
        let filtered = filter_by_names(
            all.clone(),
            &["codeintel".to_string(), "codeintel".to_string()],
        );
        assert_eq!(names(&filtered), vec!["codeintel"]);

        // No targets, or only unknown targets, yield an empty slice.
        assert!(filter_by_names(all.clone(), &[]).is_empty());
        assert!(filter_by_names(all, &["unknown".to_string()]).is_empty());
    }

    /// Requires S1 (`definition::read_definitions`) to be implemented; until
    /// then this test panics on `todo!("S1: definition parsing")`.
    #[test]
    fn resolve_schema_reads_fixture_definitions() {
        let root = unique_temp_dir();
        let migration_dir = root
            .0
            .join("migrations/frontend/1648051770_squashed_migrations_privileged");
        fs::create_dir_all(&migration_dir).expect("create fixture directory");
        fs::write(migration_dir.join("up.sql"), "SELECT 1;\n").expect("write up.sql");
        fs::write(migration_dir.join("down.sql"), "SELECT 1;\n").expect("write down.sql");
        fs::write(
            migration_dir.join("metadata.yaml"),
            "name: squashed migrations (privileged)\n\
             parents: []\n\
             privileged: true\n\
             nonIdempotent: true\n",
        )
        .expect("write metadata.yaml");

        let resolved = resolve_schema(&root.0, "frontend").expect("resolve fixture schema");
        assert_eq!(resolved.name, "frontend");
        assert_eq!(resolved.migrations_table_name, "schema_migrations");
        let ids: Vec<i32> = resolved
            .definitions
            .all()
            .iter()
            .map(|definition| definition.id)
            .collect();
        assert_eq!(ids, vec![1648051770]);
    }
}
