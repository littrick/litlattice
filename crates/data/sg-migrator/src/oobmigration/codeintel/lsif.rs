#![forbid(unsafe_code)]

//! The four payload-based LSIF migrations:
//!
//! * `1` diagnostics count (`diagnostics_count.go`)
//! * `4` definitions locations count (`locations_count.go`)
//! * `5` references locations count (`locations_count.go`)
//! * `7` document column split (`document_column_split.go`)
//!
//! Each is a [`GenericMigrator`] with the field layout and row transform from
//! the baseline. The batch sizes match `register.go`'s enterprise registration.

use std::sync::Arc;

use crate::error::MigratorError;

use super::super::model::Migrator;
use super::migrator::{
    nullable_bytes, text, FieldSpec, GenericMigrator, MigrationDriver, SqlValue,
};
use super::serializer;

const DOCUMENT_FIELDS: &[FieldSpec] = &[
    FieldSpec::primary_key("path", "text not null"),
    FieldSpec::read_only("data", "bytea"),
    FieldSpec::update_only("num_diagnostics", "integer not null"),
];

const LOCATION_FIELDS: &[FieldSpec] = &[
    FieldSpec::primary_key("scheme", "text not null"),
    FieldSpec::primary_key("identifier", "text not null"),
    FieldSpec::read_only("data", "bytea"),
    FieldSpec::update_only("num_locations", "integer not null"),
];

const DOCUMENT_SPLIT_FIELDS: &[FieldSpec] = &[
    FieldSpec::primary_key("path", "text not null"),
    FieldSpec::read_write("data", "bytea"),
    FieldSpec::read_write("ranges", "bytea"),
    FieldSpec::read_write("hovers", "bytea"),
    FieldSpec::read_write("monikers", "bytea"),
    FieldSpec::read_write("packages", "bytea"),
    FieldSpec::read_write("diagnostics", "bytea"),
];

/// `diagnosticsCountMigrator`, ID 1.
pub struct DiagnosticsCountDriver;

impl MigrationDriver for DiagnosticsCountDriver {
    fn id(&self) -> i32 {
        1
    }

    fn table_name(&self) -> &'static str {
        "lsif_data_documents"
    }

    fn target_version(&self) -> i32 {
        2
    }

    fn batch_size(&self) -> i64 {
        1000
    }

    fn fields(&self) -> &'static [FieldSpec] {
        DOCUMENT_FIELDS
    }

    fn migrate_row_up(&self, row: &sqlx::postgres::PgRow) -> Result<Vec<SqlValue>, MigratorError> {
        let path = text(row, "path")?;
        let data = nullable_bytes(row, "data")?;
        let document = serializer::unmarshal_legacy_document_data(data.as_deref().unwrap_or(&[]))
            .map_err(|error| {
            MigratorError::new(format!("codeintel diagnostics count decode: {error}"))
        })?;
        Ok(vec![
            SqlValue::Text(path),
            SqlValue::Int(document.diagnostics.len() as i32),
        ])
    }

    fn migrate_row_down(
        &self,
        row: &sqlx::postgres::PgRow,
    ) -> Result<Vec<SqlValue>, MigratorError> {
        Ok(vec![SqlValue::Text(text(row, "path")?), SqlValue::Int(0)])
    }
}

/// `locationsCountMigrator`, IDs 4 and 5.
pub struct LocationsCountDriver {
    pub id: i32,
    pub table: &'static str,
}

impl MigrationDriver for LocationsCountDriver {
    fn id(&self) -> i32 {
        self.id
    }

    fn table_name(&self) -> &'static str {
        self.table
    }

    fn target_version(&self) -> i32 {
        2
    }

    fn batch_size(&self) -> i64 {
        1000
    }

    fn fields(&self) -> &'static [FieldSpec] {
        LOCATION_FIELDS
    }

    fn migrate_row_up(&self, row: &sqlx::postgres::PgRow) -> Result<Vec<SqlValue>, MigratorError> {
        let scheme = text(row, "scheme")?;
        let identifier = text(row, "identifier")?;
        let data = nullable_bytes(row, "data")?;
        let locations =
            serializer::unmarshal_locations(data.as_deref().unwrap_or(&[])).map_err(|error| {
                MigratorError::new(format!("codeintel locations count decode: {error}"))
            })?;
        Ok(vec![
            SqlValue::Text(scheme),
            SqlValue::Text(identifier),
            SqlValue::Int(locations.len() as i32),
        ])
    }

    fn migrate_row_down(
        &self,
        row: &sqlx::postgres::PgRow,
    ) -> Result<Vec<SqlValue>, MigratorError> {
        Ok(vec![
            SqlValue::Text(text(row, "scheme")?),
            SqlValue::Text(text(row, "identifier")?),
            SqlValue::Int(0),
        ])
    }
}

/// `documentColumnSplitMigrator`, ID 7.
pub struct DocumentColumnSplitDriver;

impl MigrationDriver for DocumentColumnSplitDriver {
    fn id(&self) -> i32 {
        7
    }

    fn table_name(&self) -> &'static str {
        "lsif_data_documents"
    }

    fn target_version(&self) -> i32 {
        3
    }

    fn batch_size(&self) -> i64 {
        100
    }

    fn fields(&self) -> &'static [FieldSpec] {
        DOCUMENT_SPLIT_FIELDS
    }

    fn migrate_row_up(&self, row: &sqlx::postgres::PgRow) -> Result<Vec<SqlValue>, MigratorError> {
        let path = text(row, "path")?;
        let data = nullable_bytes(row, "data")?;
        let document = serializer::unmarshal_legacy_document_data(data.as_deref().unwrap_or(&[]))
            .map_err(|error| {
            MigratorError::new(format!("codeintel document split decode: {error}"))
        })?;
        let encoded = serializer::marshal_document_data(&document).map_err(|error| {
            MigratorError::new(format!("codeintel document split encode: {error}"))
        })?;

        Ok(vec![
            SqlValue::Text(path),
            SqlValue::Null,
            SqlValue::Bytes(encoded.ranges),
            SqlValue::Bytes(encoded.hover_results),
            SqlValue::Bytes(encoded.monikers),
            SqlValue::Bytes(encoded.package_information),
            SqlValue::Bytes(encoded.diagnostics),
        ])
    }

    fn migrate_row_down(
        &self,
        row: &sqlx::postgres::PgRow,
    ) -> Result<Vec<SqlValue>, MigratorError> {
        let path = text(row, "path")?;
        let encoded = super::types::MarshalledDocumentData {
            ranges: nullable_bytes(row, "ranges")?.unwrap_or_default(),
            hover_results: nullable_bytes(row, "hovers")?.unwrap_or_default(),
            monikers: nullable_bytes(row, "monikers")?.unwrap_or_default(),
            package_information: nullable_bytes(row, "packages")?.unwrap_or_default(),
            diagnostics: nullable_bytes(row, "diagnostics")?.unwrap_or_default(),
        };
        let document = serializer::unmarshal_document_data(&encoded).map_err(|error| {
            MigratorError::new(format!("codeintel document split unsplit decode: {error}"))
        })?;
        let legacy = serializer::marshal_legacy_document_data(&document).map_err(|error| {
            MigratorError::new(format!("codeintel document split unsplit encode: {error}"))
        })?;

        Ok(vec![
            SqlValue::Text(path),
            SqlValue::Bytes(legacy),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ])
    }
}

/// The registered implementations for IDs 1, 4, 5 and 7.
pub fn migrators() -> Vec<Arc<dyn Migrator>> {
    vec![
        Arc::new(GenericMigrator::new(DiagnosticsCountDriver)),
        Arc::new(GenericMigrator::new(LocationsCountDriver {
            id: 4,
            table: "lsif_data_definitions",
        })),
        Arc::new(GenericMigrator::new(LocationsCountDriver {
            id: 5,
            table: "lsif_data_references",
        })),
        Arc::new(GenericMigrator::new(DocumentColumnSplitDriver)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_expected_ids_and_intervals() {
        let migrators = migrators();
        let ids: Vec<i32> = migrators.iter().map(|migrator| migrator.id()).collect();
        assert_eq!(ids, vec![1, 4, 5, 7]);
        for migrator in migrators {
            assert_eq!(migrator.interval(), std::time::Duration::from_secs(1));
        }
    }

    /// End-to-end diagnostics-count migration. Requires `DATABASE_URL`.
    #[tokio::test]
    #[ignore = "requires a PostgreSQL database and the LSIF tables"]
    async fn migrates_diagnostics_count_against_database() {
        use super::super::super::model::Migrator as _;
        use super::super::types::{DiagnosticData, DocumentData};

        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let pool = sqlx::PgPool::connect(&url).await.expect("connect");

        sqlx::query("DROP TABLE IF EXISTS lsif_data_documents_schema_versions")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS lsif_data_documents")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE lsif_data_documents (\
                dump_id integer NOT NULL, path text NOT NULL, data bytea, \
                schema_version integer NOT NULL, num_diagnostics integer NOT NULL, \
                ranges bytea, hovers bytea, monikers bytea, packages bytea, diagnostics bytea, \
                PRIMARY KEY (dump_id, path))",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE lsif_data_documents_schema_versions (\
                dump_id integer PRIMARY KEY NOT NULL, \
                min_schema_version integer, max_schema_version integer)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let document = DocumentData {
            diagnostics: vec![
                DiagnosticData {
                    code: "a".to_string(),
                    ..DiagnosticData::default()
                },
                DiagnosticData {
                    code: "b".to_string(),
                    ..DiagnosticData::default()
                },
            ],
            ..DocumentData::default()
        };
        let data = serializer::marshal_legacy_document_data(&document).unwrap();

        sqlx::query(
            "INSERT INTO lsif_data_documents (dump_id, path, data, schema_version, num_diagnostics) \
             VALUES (42, 'p', $1, 1, 0)",
        )
        .bind(data)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO lsif_data_documents_schema_versions (dump_id, min_schema_version, max_schema_version) \
             VALUES (42, 1, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let migrator = GenericMigrator::new(DiagnosticsCountDriver);
        assert_eq!(migrator.progress(&pool, false).await.unwrap(), 0.0);
        migrator.up(&pool).await.unwrap();
        assert_eq!(migrator.progress(&pool, false).await.unwrap(), 1.0);

        let count: i32 =
            sqlx::query_scalar("SELECT num_diagnostics FROM lsif_data_documents WHERE path = 'p'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 2);

        migrator.down(&pool).await.unwrap();
        assert_eq!(migrator.progress(&pool, true).await.unwrap(), 0.0);

        sqlx::query("DROP TABLE lsif_data_documents_schema_versions")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE lsif_data_documents")
            .execute(&pool)
            .await
            .unwrap();
    }
}
