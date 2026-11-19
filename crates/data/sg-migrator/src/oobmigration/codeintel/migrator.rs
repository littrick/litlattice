#![forbid(unsafe_code)]

//! Port of `internal/oobmigration/migrations/codeintel/lsif/migrator.go`.
//!
//! The code-intelligence tables are large, so progress is tracked in a
//! `<table>_schema_versions` aggregate keyed by `dump_id` rather than by
//! counting rows in the data table. Each `up`/`down` batch locks one dump,
//! transforms its rows through the driver into a temporary table, then applies
//! them in a single `UPDATE ... FROM`.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::error::MigratorError;

use super::super::model::{Migrator, MigratorPools};

/// One column of the table being migrated (`fieldSpec` in Go).
#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    pub name: &'static str,
    pub postgres_type: &'static str,
    pub primary_key: bool,
    pub read_only: bool,
    pub update_only: bool,
}

impl FieldSpec {
    pub const fn primary_key(name: &'static str, postgres_type: &'static str) -> Self {
        Self {
            name,
            postgres_type,
            primary_key: true,
            read_only: false,
            update_only: false,
        }
    }

    pub const fn read_only(name: &'static str, postgres_type: &'static str) -> Self {
        Self {
            name,
            postgres_type,
            primary_key: false,
            read_only: true,
            update_only: false,
        }
    }

    pub const fn update_only(name: &'static str, postgres_type: &'static str) -> Self {
        Self {
            name,
            postgres_type,
            primary_key: false,
            read_only: false,
            update_only: true,
        }
    }

    pub const fn read_write(name: &'static str, postgres_type: &'static str) -> Self {
        Self {
            name,
            postgres_type,
            primary_key: false,
            read_only: false,
            update_only: false,
        }
    }
}

/// A value written to the temporary table. The variants mirror the column
/// types used by these migrations (`text`, `integer`, `bytea`).
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Text(String),
    Int(i32),
    Bytes(Vec<u8>),
    Null,
}

/// The per-row transformation the generic loop delegates to.
pub trait MigrationDriver: Send + Sync {
    fn id(&self) -> i32;
    fn table_name(&self) -> &'static str;
    fn target_version(&self) -> i32;
    fn batch_size(&self) -> i64;
    fn fields(&self) -> &'static [FieldSpec];

    /// `MigrateRowUp`: values in temporary-table column order.
    fn migrate_row_up(&self, row: &sqlx::postgres::PgRow) -> Result<Vec<SqlValue>, MigratorError>;

    /// `MigrateRowDown`: values in temporary-table column order.
    fn migrate_row_down(&self, row: &sqlx::postgres::PgRow)
        -> Result<Vec<SqlValue>, MigratorError>;
}

const TEMPORARY_TABLE_NAME: &str = "t_migration_payload";

/// `migrator` in `migrator.go`.
pub struct GenericMigrator<D: MigrationDriver> {
    driver: D,
    selection_expressions: Vec<&'static str>,
    temporary_table_field_names: Vec<&'static str>,
    temporary_table_field_specs: Vec<String>,
    update_conditions: Vec<String>,
    update_assignments: Vec<String>,
}

impl<D: MigrationDriver> GenericMigrator<D> {
    pub fn new(driver: D) -> Self {
        let mut selection_expressions = Vec::new();
        let mut temporary_table_field_names = Vec::new();
        let mut temporary_table_field_specs = Vec::new();
        let mut update_conditions = Vec::new();
        let mut update_assignments = Vec::new();

        for field in driver.fields() {
            if field.primary_key {
                update_conditions.push(format!("dest.{} = src.{}", field.name, field.name));
            }
            if !field.update_only {
                selection_expressions.push(field.name);
            }
            if !field.read_only {
                temporary_table_field_names.push(field.name);
                temporary_table_field_specs.push(format!("{} {}", field.name, field.postgres_type));
                if !field.primary_key {
                    update_assignments.push(format!("{} = src.{}", field.name, field.name));
                }
            }
        }

        Self {
            driver,
            selection_expressions,
            temporary_table_field_names,
            temporary_table_field_specs,
            update_conditions,
            update_assignments,
        }
    }

    fn progress_query(&self, apply_reverse: bool) -> String {
        let table = self.driver.table_name();
        let column = if apply_reverse {
            "max_schema_version"
        } else {
            "min_schema_version"
        };
        format!(
            "SELECT CASE c2.count WHEN 0 THEN 1 ELSE \
             cast(c1.count as float) / cast(c2.count as float) END FROM \
             (SELECT COUNT(*) as count FROM {table}_schema_versions WHERE {column} >= $1) c1, \
             (SELECT COUNT(*) as count FROM {table}_schema_versions) c2"
        )
    }

    fn select_and_lock_dump_query(&self) -> String {
        format!(
            "SELECT dump_id FROM {}_schema_versions \
             WHERE min_schema_version <= $1 AND max_schema_version >= $2 \
             ORDER BY dump_id LIMIT 1 FOR UPDATE SKIP LOCKED",
            self.driver.table_name()
        )
    }

    fn process_rows_query(&self) -> String {
        format!(
            "SELECT {} FROM {} WHERE dump_id = $1 AND schema_version = $2 LIMIT $3",
            self.selection_expressions.join(", "),
            self.driver.table_name()
        )
    }

    fn create_temporary_table_query(&self) -> String {
        format!(
            "CREATE TEMPORARY TABLE {TEMPORARY_TABLE_NAME} ({}) ON COMMIT DROP",
            self.temporary_table_field_specs.join(", ")
        )
    }

    fn insert_temporary_row_query(&self) -> String {
        let placeholders: Vec<String> = (1..=self.temporary_table_field_names.len())
            .map(|index| format!("${index}"))
            .collect();
        format!(
            "INSERT INTO {TEMPORARY_TABLE_NAME} ({}) VALUES ({})",
            self.temporary_table_field_names.join(", "),
            placeholders.join(", ")
        )
    }

    fn update_batch_query(&self, target_version: i32) -> String {
        format!(
            "UPDATE {} dest SET {}, schema_version = {} FROM {TEMPORARY_TABLE_NAME} src \
             WHERE dump_id = $1 AND {}",
            self.driver.table_name(),
            self.update_assignments.join(", "),
            target_version,
            self.update_conditions.join(" AND ")
        )
    }

    fn update_bounds_query(&self) -> String {
        let table = self.driver.table_name();
        format!(
            "WITH \
             current_bounds AS ( \
                 SELECT $1::integer AS dump_id, \
                 MIN(schema_version) as min_schema_version, \
                 MAX(schema_version) as max_schema_version \
                 FROM {table} WHERE dump_id = $1 \
             ), \
             ups AS ( \
                 INSERT INTO {table}_schema_versions \
                 SELECT dump_id, min_schema_version, max_schema_version \
                 FROM current_bounds \
                 WHERE min_schema_version IS NOT NULL AND min_schema_version IS NOT NULL \
                 ON CONFLICT (dump_id) DO UPDATE SET \
                     min_schema_version = EXCLUDED.min_schema_version, \
                     max_schema_version = EXCLUDED.max_schema_version \
                 RETURNING 1 \
             ), \
             del AS ( \
                 DELETE FROM {table}_schema_versions \
                 WHERE dump_id = $1 AND EXISTS ( \
                     SELECT 1 FROM current_bounds \
                     WHERE min_schema_version IS NULL AND max_schema_version IS NULL \
                 ) \
                 RETURNING 1 \
             ) \
             SELECT (SELECT COUNT(*) FROM ups) AS num_ups, \
                    (SELECT COUNT(*) FROM del) AS num_del"
        )
    }

    async fn run(
        &self,
        db: &PgPool,
        source_version: i32,
        target_version: i32,
        up: bool,
    ) -> Result<(), MigratorError> {
        let mut tx = db.begin().await.map_err(|error| {
            MigratorError::new(format!(
                "codeintel migration {} begin: {error}",
                self.driver.id()
            ))
        })?;

        let dump: Option<i32> = sqlx::query_scalar(&self.select_and_lock_dump_query())
            .bind(source_version)
            .bind(source_version)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!(
                    "codeintel migration {} select dump: {error}",
                    self.driver.id()
                ))
            })?;

        let Some(dump) = dump else {
            tx.commit().await.map_err(|error| {
                MigratorError::new(format!(
                    "codeintel migration {} commit: {error}",
                    self.driver.id()
                ))
            })?;
            return Ok(());
        };

        let rows = sqlx::query(&self.process_rows_query())
            .bind(dump)
            .bind(source_version)
            .bind(self.driver.batch_size())
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!(
                    "codeintel migration {} select rows: {error}",
                    self.driver.id()
                ))
            })?;

        let mut values = Vec::with_capacity(rows.len());
        for row in &rows {
            values.push(if up {
                self.driver.migrate_row_up(row)?
            } else {
                self.driver.migrate_row_down(row)?
            });
        }

        sqlx::query(&self.create_temporary_table_query())
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!(
                    "codeintel migration {} create temporary table: {error}",
                    self.driver.id()
                ))
            })?;

        let insert_query = self.insert_temporary_row_query();
        for row_values in &values {
            insert_temporary_row(&mut tx, &insert_query, row_values)
                .await
                .map_err(|error| {
                    MigratorError::new(format!(
                        "codeintel migration {} insert temporary row: {error}",
                        self.driver.id()
                    ))
                })?;
        }

        sqlx::query(&self.update_batch_query(target_version))
            .bind(dump)
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!(
                    "codeintel migration {} update batch: {error}",
                    self.driver.id()
                ))
            })?;

        // Refresh the schema-version bounds for this dump. This happens even
        // when no rows were migrated (to clear dumps that have no data rows).
        sqlx::query(&self.update_bounds_query())
            .bind(dump)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!(
                    "codeintel migration {} update bounds: {error}",
                    self.driver.id()
                ))
            })?;

        tx.commit().await.map_err(|error| {
            MigratorError::new(format!(
                "codeintel migration {} commit: {error}",
                self.driver.id()
            ))
        })?;

        Ok(())
    }
}

async fn insert_temporary_row(
    tx: &mut Transaction<'_, Postgres>,
    query: &str,
    values: &[SqlValue],
) -> Result<(), sqlx::Error> {
    let mut statement = sqlx::query(query);
    for value in values {
        statement = match value {
            SqlValue::Text(value) => statement.bind(value.clone()),
            SqlValue::Int(value) => statement.bind(*value),
            SqlValue::Bytes(value) => statement.bind(value.clone()),
            SqlValue::Null => statement.bind(Option::<Vec<u8>>::None),
        };
    }
    statement.execute(&mut **tx).await?;
    Ok(())
}

#[async_trait]
impl<D: MigrationDriver + 'static> Migrator for GenericMigrator<D> {
    fn id(&self) -> i32 {
        self.driver.id()
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn progress(&self, db: &PgPool, apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(&self.progress_query(apply_reverse))
            .bind(self.driver.target_version())
            .fetch_one(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!(
                    "codeintel migration {} progress: {error}",
                    self.driver.id()
                ))
            })
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        self.run(
            db,
            self.driver.target_version() - 1,
            self.driver.target_version(),
            true,
        )
        .await
    }

    async fn down(&self, db: &PgPool) -> Result<(), MigratorError> {
        self.run(
            db,
            self.driver.target_version(),
            self.driver.target_version() - 1,
            false,
        )
        .await
    }

    async fn progress_with_pools(
        &self,
        pools: &MigratorPools,
        apply_reverse: bool,
    ) -> Result<f64, MigratorError> {
        self.progress(pools.codeintel_or_frontend(), apply_reverse)
            .await
    }

    async fn up_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.up(pools.codeintel_or_frontend()).await
    }

    async fn down_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.down(pools.codeintel_or_frontend()).await
    }
}

/// Read a nullable `bytea` column as a byte vector.
pub fn nullable_bytes(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<Option<Vec<u8>>, MigratorError> {
    row.try_get::<Option<Vec<u8>>, _>(column)
        .map_err(|error| MigratorError::new(format!("codeintel read {column}: {error}")))
}

/// Read a `text` column.
pub fn text(row: &sqlx::postgres::PgRow, column: &str) -> Result<String, MigratorError> {
    row.try_get::<String, _>(column)
        .map_err(|error| MigratorError::new(format!("codeintel read {column}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDriver;

    const FIELDS: &[FieldSpec] = &[
        FieldSpec::primary_key("path", "text not null"),
        FieldSpec::read_only("data", "bytea"),
        FieldSpec::update_only("num_diagnostics", "integer not null"),
    ];

    impl MigrationDriver for TestDriver {
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
            FIELDS
        }
        fn migrate_row_up(
            &self,
            _row: &sqlx::postgres::PgRow,
        ) -> Result<Vec<SqlValue>, MigratorError> {
            Ok(vec![])
        }
        fn migrate_row_down(
            &self,
            _row: &sqlx::postgres::PgRow,
        ) -> Result<Vec<SqlValue>, MigratorError> {
            Ok(vec![])
        }
    }

    #[test]
    fn builds_queries_like_go() {
        let migrator = GenericMigrator::new(TestDriver);
        assert_eq!(
            migrator.progress_query(false),
            "SELECT CASE c2.count WHEN 0 THEN 1 ELSE cast(c1.count as float) / cast(c2.count as float) END FROM (SELECT COUNT(*) as count FROM lsif_data_documents_schema_versions WHERE min_schema_version >= $1) c1, (SELECT COUNT(*) as count FROM lsif_data_documents_schema_versions) c2"
        );
        assert!(migrator
            .progress_query(true)
            .contains("max_schema_version >= $1"));
        assert_eq!(
            migrator.process_rows_query(),
            "SELECT path, data FROM lsif_data_documents WHERE dump_id = $1 AND schema_version = $2 LIMIT $3"
        );
        assert_eq!(
            migrator.create_temporary_table_query(),
            "CREATE TEMPORARY TABLE t_migration_payload (path text not null, num_diagnostics integer not null) ON COMMIT DROP"
        );
        assert_eq!(
            migrator.insert_temporary_row_query(),
            "INSERT INTO t_migration_payload (path, num_diagnostics) VALUES ($1, $2)"
        );
        assert_eq!(
            migrator.update_batch_query(2),
            "UPDATE lsif_data_documents dest SET num_diagnostics = src.num_diagnostics, schema_version = 2 FROM t_migration_payload src WHERE dump_id = $1 AND dest.path = src.path"
        );
    }

    #[test]
    fn interval_is_one_second() {
        let migrator = GenericMigrator::new(TestDriver);
        assert_eq!(migrator.interval(), Duration::from_secs(1));
        assert_eq!(migrator.id(), 1);
    }
}
