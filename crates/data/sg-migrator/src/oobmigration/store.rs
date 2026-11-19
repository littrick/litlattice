#![forbid(unsafe_code)]

//! `internal/oobmigration/store.go`: the `out_of_band_migrations` DB layer.
//!
//! Query text mirrors the Go baseline; the only intentional difference is that
//! the error columns are aliased (`error_message`, `error_created`) so they can
//! be addressed by name instead of position with sqlx.

use serde_json::Value;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::error::MigratorError;

use super::metadata;
use super::model::{Migration, MigrationError};
use super::version::Version;

/// `MaxMigrationErrors` (`store.go:554`).
pub const MAX_MIGRATION_ERRORS: i32 = 100;

const FALLBACK_COLUMNS: &[&str] = &[
    "is_enterprise",
    "introduced_version_major",
    "introduced_version_minor",
    "deprecated_version_major",
    "deprecated_version_minor",
];

/// The out-of-band migration store.
#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `Store.SynchronizeMetadata` (`store.go:209`).
    pub async fn synchronize_metadata(&self) -> Result<(), MigratorError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin metadata transaction", error))?;

        for migration in metadata::MIGRATIONS {
            let result = sqlx::query(SYNCHRONIZE_METADATA_UPSERT_QUERY)
                .bind(migration.id)
                .bind(migration.team)
                .bind(migration.component)
                .bind(migration.description)
                .bind(migration.non_destructive)
                .bind(migration.is_enterprise)
                .bind(migration.introduced_version_major)
                .bind(migration.introduced_version_minor)
                .bind(migration.deprecated_version_major)
                .bind(migration.deprecated_version_minor)
                .bind(migration.team)
                .bind(migration.component)
                .bind(migration.description)
                .bind(migration.non_destructive)
                .bind(migration.is_enterprise)
                .bind(migration.introduced_version_major)
                .bind(migration.introduced_version_minor)
                .bind(migration.deprecated_version_major)
                .bind(migration.deprecated_version_minor)
                .execute(&mut *tx)
                .await;

            if let Err(error) = result {
                if should_fallback(&error) {
                    // Roll back the partially applied optimistic transaction and
                    // fall back to the pre-3.29 column layout.
                    drop(tx);
                    return self.synchronize_metadata_fallback().await;
                }

                return Err(db_error("synchronize metadata", error));
            }
        }

        tx.commit()
            .await
            .map_err(|error| db_error("commit metadata transaction", error))
    }

    /// `Store.synchronizeMetadataFallback` (`store.go:286`).
    async fn synchronize_metadata_fallback(&self) -> Result<(), MigratorError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin metadata fallback transaction", error))?;

        for migration in metadata::MIGRATIONS {
            let introduced = version_string(
                migration.introduced_version_major,
                migration.introduced_version_minor,
            );
            let deprecated = match migration.deprecated_version_major {
                Some(major) => Some(version_string(
                    major,
                    migration.deprecated_version_minor.unwrap_or(0),
                )),
                None => None,
            };

            sqlx::query(SYNCHRONIZE_METADATA_FALLBACK_UPSERT_QUERY)
                .bind(migration.id)
                .bind(migration.team)
                .bind(migration.component)
                .bind(migration.description)
                .bind(migration.non_destructive)
                .bind(introduced.clone())
                .bind(deprecated.clone())
                .bind(migration.team)
                .bind(migration.component)
                .bind(migration.description)
                .bind(migration.non_destructive)
                .bind(introduced)
                .bind(deprecated)
                .execute(&mut *tx)
                .await
                .map_err(|error| db_error("synchronize metadata fallback", error))?;
        }

        tx.commit()
            .await
            .map_err(|error| db_error("commit metadata fallback transaction", error))
    }

    /// `Store.GetByID` (`store.go:348`). Returns `(migration, found)`.
    pub async fn get_by_id(&self, id: i32) -> Result<(Migration, bool), MigratorError> {
        let rows = sqlx::query(GET_BY_ID_QUERY)
            .bind(id)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("get out-of-band migration", error))?;

        let migrations = scan_migrations(rows)?;
        Ok(match migrations.into_iter().next() {
            Some(migration) => (migration, true),
            None => (Migration::default(), false),
        })
    }

    /// `Store.GetByIDs` (`store.go:386`).
    pub async fn get_by_ids(&self, ids: &[i32]) -> Result<Vec<Migration>, MigratorError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(GET_BY_IDS_QUERY)
            .bind(ids.to_vec())
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("get out-of-band migrations", error))?;

        let migrations = scan_migrations(rows)?;

        let mut missing: Vec<i32> = ids
            .iter()
            .copied()
            .filter(|wanted| !migrations.iter().any(|migration| migration.id == *wanted))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            missing.dedup();
            return Err(MigratorError::new(format!(
                "unknown migration id(s) {}",
                format_int_slice(&missing)
            )));
        }

        Ok(migrations)
    }

    /// `Store.List` (`store.go:436`). Only canonical IDs are returned.
    pub async fn list(&self) -> Result<Vec<Migration>, MigratorError> {
        let ids = metadata::ids();

        let result = sqlx::query(LIST_QUERY)
            .bind(ids.clone())
            .fetch_all(&self.pool)
            .await;

        let rows = match result {
            Ok(rows) => rows,
            Err(error) if should_fallback(&error) => sqlx::query(LIST_FALLBACK_QUERY)
                .bind(ids)
                .fetch_all(&self.pool)
                .await
                .map_err(|error| db_error("list out-of-band migrations fallback", error))?,
            Err(error) => return Err(db_error("list out-of-band migrations", error)),
        };

        scan_migrations(rows)
    }

    /// `Store.UpdateDirection` (`store.go:518`).
    pub async fn update_direction(
        &self,
        id: i32,
        apply_reverse: bool,
    ) -> Result<(), MigratorError> {
        sqlx::query(UPDATE_DIRECTION_QUERY)
            .bind(apply_reverse)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("update migration direction", error))?;

        Ok(())
    }

    /// `Runner.UpdateDirection` (`runner.go:180`): set the direction for every
    /// given migration atomically.
    pub async fn update_directions(
        &self,
        ids: &[i32],
        apply_reverse: bool,
    ) -> Result<(), MigratorError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin direction transaction", error))?;

        for id in ids {
            sqlx::query(UPDATE_DIRECTION_QUERY)
                .bind(apply_reverse)
                .bind(*id)
                .execute(&mut *tx)
                .await
                .map_err(|error| db_error("update migration direction", error))?;
        }

        tx.commit()
            .await
            .map_err(|error| db_error("commit direction transaction", error))
    }

    /// `Store.UpdateProgress` (`store.go:527`).
    pub async fn update_progress(&self, id: i32, progress: f64) -> Result<(), MigratorError> {
        self.update_progress_at(id, progress, OffsetDateTime::now_utc())
            .await
    }

    async fn update_progress_at(
        &self,
        id: i32,
        progress: f64,
        now: OffsetDateTime,
    ) -> Result<(), MigratorError> {
        sqlx::query(UPDATE_PROGRESS_QUERY)
            .bind(progress)
            .bind(now)
            .bind(id)
            .bind(progress)
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("update migration progress", error))?;

        Ok(())
    }

    /// `Store.UpdateMetadata` (`store.go:540`).
    pub async fn update_metadata(
        &self,
        id: i32,
        metadata: Option<Value>,
    ) -> Result<(), MigratorError> {
        self.update_metadata_at(id, metadata, OffsetDateTime::now_utc())
            .await
    }

    async fn update_metadata_at(
        &self,
        id: i32,
        metadata: Option<Value>,
        now: OffsetDateTime,
    ) -> Result<(), MigratorError> {
        sqlx::query(UPDATE_METADATA_QUERY)
            .bind(metadata.clone())
            .bind(now)
            .bind(id)
            .bind(metadata)
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("update migration metadata", error))?;

        Ok(())
    }

    /// `Store.AddError` (`store.go:559`): record the error, bump
    /// `last_updated`, and prune entries beyond [`MAX_MIGRATION_ERRORS`].
    pub async fn add_error(&self, id: i32, message: &str) -> Result<(), MigratorError> {
        self.add_error_at(id, message, OffsetDateTime::now_utc())
            .await
    }

    async fn add_error_at(
        &self,
        id: i32,
        message: &str,
        now: OffsetDateTime,
    ) -> Result<(), MigratorError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin error transaction", error))?;

        sqlx::query(ADD_ERROR_QUERY)
            .bind(id)
            .bind(message)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(|error| db_error("add migration error", error))?;

        sqlx::query(ADD_ERROR_UPDATE_TIME_QUERY)
            .bind(now)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|error| db_error("update migration error timestamp", error))?;

        sqlx::query(ADD_ERROR_PRUNE_QUERY)
            .bind(id)
            .bind(MAX_MIGRATION_ERRORS)
            .execute(&mut *tx)
            .await
            .map_err(|error| db_error("prune migration errors", error))?;

        tx.commit()
            .await
            .map_err(|error| db_error("commit error transaction", error))
    }
}

fn scan_migrations(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Migration>, MigratorError> {
    let mut migrations: Vec<Migration> = Vec::new();

    for row in rows {
        let id: i32 = row
            .try_get("id")
            .map_err(|error| db_error("scan migration id", error))?;
        let team: String = row
            .try_get("team")
            .map_err(|error| db_error("scan migration team", error))?;
        let component: String = row
            .try_get("component")
            .map_err(|error| db_error("scan migration component", error))?;
        let description: String = row
            .try_get("description")
            .map_err(|error| db_error("scan migration description", error))?;
        let introduced_major: i32 = row
            .try_get("introduced_version_major")
            .map_err(|error| db_error("scan migration introduced major", error))?;
        let introduced_minor: i32 = row
            .try_get("introduced_version_minor")
            .map_err(|error| db_error("scan migration introduced minor", error))?;
        let deprecated_major: Option<i32> = row
            .try_get("deprecated_version_major")
            .map_err(|error| db_error("scan migration deprecated major", error))?;
        let deprecated_minor: Option<i32> = row
            .try_get("deprecated_version_minor")
            .map_err(|error| db_error("scan migration deprecated minor", error))?;
        let progress: f64 = row
            .try_get("progress")
            .map_err(|error| db_error("scan migration progress", error))?;
        let created: OffsetDateTime = row
            .try_get("created")
            .map_err(|error| db_error("scan migration created", error))?;
        let last_updated: Option<OffsetDateTime> = row
            .try_get("last_updated")
            .map_err(|error| db_error("scan migration last_updated", error))?;
        let non_destructive: bool = row
            .try_get("non_destructive")
            .map_err(|error| db_error("scan migration non_destructive", error))?;
        let is_enterprise: bool = row
            .try_get("is_enterprise")
            .map_err(|error| db_error("scan migration is_enterprise", error))?;
        let apply_reverse: bool = row
            .try_get("apply_reverse")
            .map_err(|error| db_error("scan migration apply_reverse", error))?;
        let metadata: Option<Value> = row
            .try_get("metadata")
            .map_err(|error| db_error("scan migration metadata", error))?;
        let error_message: Option<String> = row
            .try_get("error_message")
            .map_err(|error| db_error("scan migration error message", error))?;
        let error_created: Option<OffsetDateTime> = row
            .try_get("error_created")
            .map_err(|error| db_error("scan migration error created", error))?;

        let deprecated = match (deprecated_major, deprecated_minor) {
            (Some(major), Some(minor)) => Some(Version::new(major, minor)),
            _ => None,
        };

        let mut value = Migration {
            id,
            team,
            component,
            description,
            introduced: Version::new(introduced_major, introduced_minor),
            deprecated,
            progress,
            created,
            last_updated,
            non_destructive,
            is_enterprise,
            apply_reverse,
            errors: Vec::new(),
            metadata,
        };

        if let (Some(message), Some(created)) = (error_message, error_created) {
            if !message.is_empty() {
                value.errors.push(MigrationError::new(message, created));
            }
        }

        // Rows are ordered such that errors for the same migration are adjacent.
        if let Some(previous) = migrations.last_mut() {
            if previous.id == value.id {
                previous.errors.extend(value.errors);
                continue;
            }
        }

        migrations.push(value);
    }

    Ok(migrations)
}

fn db_error(context: &str, error: sqlx::Error) -> MigratorError {
    MigratorError::new(format!("{context}: {error}"))
}

fn should_fallback(error: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(database_error) = error {
        if database_error.code().as_deref() == Some("42703") {
            let message = database_error.message();
            return FALLBACK_COLUMNS
                .iter()
                .any(|column| message.contains(column));
        }
    }

    false
}

/// `versionString` (`store.go:620`).
fn version_string(major: i32, minor: i32) -> String {
    format!("{major}.{minor}.0")
}

/// Render an integer slice the way Go's `%v` does (`[1 2 3]`).
fn format_int_slice(values: &[i32]) -> String {
    let joined = values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{joined}]")
}

const SYNCHRONIZE_METADATA_UPSERT_QUERY: &str = r#"
INSERT INTO out_of_band_migrations
(
	id,
	team,
	component,
	description,
	created,
	non_destructive,
	is_enterprise,
	introduced_version_major,
	introduced_version_minor,
	deprecated_version_major,
	deprecated_version_minor
)
VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO UPDATE SET
	team = $11,
	component = $12,
	description = $13,
	non_destructive = $14,
	is_enterprise = $15,
	introduced_version_major = $16,
	introduced_version_minor = $17,
	deprecated_version_major = $18,
	deprecated_version_minor = $19
"#;

const SYNCHRONIZE_METADATA_FALLBACK_UPSERT_QUERY: &str = r#"
INSERT INTO out_of_band_migrations
(
	id,
	team,
	component,
	description,
	created,
	non_destructive,
	introduced,
	deprecated
)
VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7)
ON CONFLICT (id) DO UPDATE SET
	team = $8,
	component = $9,
	description = $10,
	non_destructive = $11,
	introduced = $12,
	deprecated = $13
"#;

const GET_BY_ID_QUERY: &str = r#"
SELECT
	m.id,
	m.team,
	m.component,
	m.description,
	m.introduced_version_major,
	m.introduced_version_minor,
	m.deprecated_version_major,
	m.deprecated_version_minor,
	m.progress,
	m.created,
	m.last_updated,
	m.non_destructive,
	m.is_enterprise,
	m.apply_reverse,
	m.metadata,
	e.message AS error_message,
	e.created AS error_created
FROM out_of_band_migrations m
LEFT JOIN out_of_band_migrations_errors e ON e.migration_id = m.id
WHERE m.id = $1
ORDER BY e.created desc
"#;

const GET_BY_IDS_QUERY: &str = r#"
SELECT
	m.id,
	m.team,
	m.component,
	m.description,
	m.introduced_version_major,
	m.introduced_version_minor,
	m.deprecated_version_major,
	m.deprecated_version_minor,
	m.progress,
	m.created,
	m.last_updated,
	m.non_destructive,
	m.is_enterprise,
	m.apply_reverse,
	m.metadata,
	e.message AS error_message,
	e.created AS error_created
FROM out_of_band_migrations m
LEFT JOIN out_of_band_migrations_errors e ON e.migration_id = m.id
WHERE m.id = ANY($1)
ORDER BY m.id ASC, e.created DESC
"#;

const LIST_QUERY: &str = r#"
SELECT
	m.id,
	m.team,
	m.component,
	m.description,
	m.introduced_version_major,
	m.introduced_version_minor,
	m.deprecated_version_major,
	m.deprecated_version_minor,
	m.progress,
	m.created,
	m.last_updated,
	m.non_destructive,
	m.is_enterprise,
	m.apply_reverse,
	m.metadata,
	e.message AS error_message,
	e.created AS error_created
FROM out_of_band_migrations m
LEFT JOIN out_of_band_migrations_errors e ON e.migration_id = m.id
WHERE m.id = ANY($1)
ORDER BY m.id desc, e.created DESC
"#;

const LIST_FALLBACK_QUERY: &str = r#"
WITH split_migrations AS (
	SELECT
		m.*,
		regexp_matches(m.introduced, E'^(\\d+)\\.(\\d+)') AS introduced_parts,
		regexp_matches(m.deprecated, E'^(\\d+)\\.(\\d+)') AS deprecated_parts
	FROM out_of_band_migrations m
)
SELECT
	m.id,
	m.team,
	m.component,
	m.description,
	(introduced_parts[1])::integer AS introduced_version_major,
	(introduced_parts[2])::integer AS introduced_version_minor,
	CASE WHEN m.deprecated = '' THEN NULL ELSE (deprecated_parts[1])::integer END AS deprecated_version_major,
	CASE WHEN m.deprecated = '' THEN NULL ELSE (deprecated_parts[2])::integer END AS deprecated_version_minor,
	m.progress,
	m.created,
	m.last_updated,
	m.non_destructive,
	-- New migrator against an old instance: multi-version upgrades are an
	-- enterprise feature, so assume enterprise.
	true AS is_enterprise,
	m.apply_reverse,
	m.metadata,
	e.message AS error_message,
	e.created AS error_created
FROM split_migrations m
LEFT JOIN out_of_band_migrations_errors e ON e.migration_id = m.id
WHERE m.id = ANY($1)
ORDER BY m.id desc, e.created DESC
"#;

const UPDATE_DIRECTION_QUERY: &str = r#"
UPDATE out_of_band_migrations SET apply_reverse = $1 WHERE id = $2
"#;

const UPDATE_PROGRESS_QUERY: &str = r#"
UPDATE out_of_band_migrations SET progress = $1, last_updated = $2 WHERE id = $3 AND progress != $4
"#;

const UPDATE_METADATA_QUERY: &str = r#"
UPDATE out_of_band_migrations SET metadata = $1, last_updated = $2 WHERE id = $3 AND metadata != $4
"#;

const ADD_ERROR_QUERY: &str = r#"
INSERT INTO out_of_band_migrations_errors (migration_id, message, created) VALUES ($1, $2, $3)
"#;

const ADD_ERROR_UPDATE_TIME_QUERY: &str = r#"
UPDATE out_of_band_migrations SET last_updated = $1 where id = $2
"#;

const ADD_ERROR_PRUNE_QUERY: &str = r#"
DELETE FROM out_of_band_migrations_errors WHERE id IN (
	SELECT id FROM out_of_band_migrations_errors WHERE migration_id = $1 ORDER BY created DESC OFFSET $2
)
"#;
