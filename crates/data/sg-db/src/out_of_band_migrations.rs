//! `internal/oobmigration/store.go`: the `out_of_band_migrations` table.
//!
//! Rows are keyed by the identifiers listed in the baseline's embedded
//! `oobmigrations.yaml`; [`synchronize_metadata`] upserts that static metadata
//! and [`list`] hides rows the current binary has no definition for. The
//! pre-3.29 fallback queries (`introduced`/`deprecated` text columns) are
//! reproduced verbatim but only trigger on a schema without
//! `introduced_version_*`/`is_enterprise` (Postgres `42703` naming one of those
//! columns), which the current schema never is.
//!
//! `metadata` is exposed as [`serde_json::Value`] (the column is `jsonb`), and
//! the error list on a [`Migration`] is the left-joined
//! `out_of_band_migrations_errors` rows, newest first, exactly as
//! `scanMigrations` aggregates them.

use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};

/// `MaxMigrationErrors`: errors tracked per migration before pruning.
pub const MAX_MIGRATION_ERRORS: i64 = 100;

/// `oobmigration.MigrationError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationError {
    pub message: String,
    pub created: OffsetDateTime,
}

/// `oobmigration.Migration`.
#[derive(Debug, Clone, PartialEq)]
pub struct Migration {
    pub id: i32,
    pub team: String,
    pub component: String,
    pub description: String,
    pub introduced_major: i32,
    pub introduced_minor: i32,
    pub deprecated_major: Option<i32>,
    pub deprecated_minor: Option<i32>,
    pub progress: f64,
    pub created: OffsetDateTime,
    pub last_updated: Option<OffsetDateTime>,
    pub non_destructive: bool,
    pub is_enterprise: bool,
    pub apply_reverse: bool,
    pub metadata: Value,
    pub errors: Vec<MigrationError>,
}

impl Migration {
    /// `Migration.Complete`: 100% forward, or 0% in reverse.
    pub fn complete(&self) -> bool {
        (self.progress == 1.0 && !self.apply_reverse)
            || (self.progress == 0.0 && self.apply_reverse)
    }
}

/// `oobmigration.yamlMigration`: one entry of the embedded `oobmigrations.yaml`.
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

/// `oobmigration.yalmMigrations`: the embedded YAML, transcribed as constants
/// (sorted by id, as `yamlMigrations` sorts after parsing).
pub const YAML_MIGRATIONS: &[YamlMigration] = &[
    YamlMigration {
        id: 1,
        team: "code-intelligence",
        component: "codeintel-db.lsif_data_documents",
        description: "Populate num_diagnostics from gob-encoded payload",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 25,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 2,
        team: "campaigns",
        component: "frontend-db.authenticators",
        description: "Prepare for SSH pushes to code hosts",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 26,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 4,
        team: "code-intelligence",
        component: "codeintel-db.lsif_data_definitions",
        description: "Populate num_locations from gob-encoded payload",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 26,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 5,
        team: "code-intelligence",
        component: "codeintel-db.lsif_data_references",
        description: "Populate num_locations from gob-encoded payload",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 26,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 7,
        team: "code-intelligence",
        component: "codeintel-db.lsif_data_documents",
        description: "Split payload into multiple columns",
        non_destructive: false,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 27,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 13,
        team: "batch-changes",
        component: "frontend-db.external_services",
        description: "Calculate the webhook state of each external service",
        non_destructive: true,
        is_enterprise: false,
        introduced_version_major: 3,
        introduced_version_minor: 34,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 14,
        team: "code-insights",
        component: "db.insights_settings_migration_jobs",
        description: "Migrating insight definitions from settings files to database tables as a last stage to use the GraphQL API.",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 35,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 15,
        team: "iam",
        component: "frontend-db.product_subscriptions",
        description: "Attempt to extract account number from the associated user of subscriptions.",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 43,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 16,
        team: "iam",
        component: "frontend-db.product_licenses",
        description: "Extract license fields from the license key text and store them to dedicated columns.",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 3,
        introduced_version_minor: 43,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(0),
    },
    YamlMigration {
        id: 17,
        team: "code-insights",
        component: "codeinsights-db.insight_series",
        description: "Save historical recording times for all existing insights",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 4,
        introduced_version_minor: 3,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(4),
    },
    YamlMigration {
        id: 18,
        team: "code-insights",
        component: "db.insights_backfillerv2_migration",
        description: "Migrates insight series from the v1 backfiller to v2.",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 4,
        introduced_version_minor: 3,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(4),
    },
    YamlMigration {
        id: 19,
        team: "batch-changes",
        component: "frontend-db.user_roles",
        description: "Assigns roles to existing users",
        non_destructive: true,
        is_enterprise: false,
        introduced_version_major: 4,
        introduced_version_minor: 5,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(6),
    },
    YamlMigration {
        id: 20,
        team: "code-intelligence",
        component: "codeintel-db.lsif_data_*",
        description: "Convert LSIF data to SCIP.",
        non_destructive: false,
        is_enterprise: true,
        introduced_version_major: 4,
        introduced_version_minor: 5,
        deprecated_version_major: Some(4),
        deprecated_version_minor: Some(6),
    },
    YamlMigration {
        id: 21,
        team: "batch-changes",
        component: "frontend-db.changesets",
        description: "Populate external_fork_name column for existing changesets published on Bitbucket Server and Cloud.",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 4,
        introduced_version_minor: 5,
        deprecated_version_major: None,
        deprecated_version_minor: None,
    },
    YamlMigration {
        id: 22,
        team: "iam",
        component: "frontend-db.user_repo_permissions",
        description: "Migrate data from user_permissions table to unified user_repo_permissions.",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 5,
        introduced_version_minor: 0,
        deprecated_version_major: None,
        deprecated_version_minor: None,
    },
    YamlMigration {
        id: 23,
        team: "batch-changes",
        component: "frontend-db.batch_specs",
        description: "Re-ID empty batch specs belonging to draft batch changes.",
        non_destructive: true,
        is_enterprise: true,
        introduced_version_major: 5,
        introduced_version_minor: 0,
        deprecated_version_major: None,
        deprecated_version_minor: None,
    },
];

/// `yamlMigrationIDs`, in table-definition order.
pub fn yaml_migration_ids() -> Vec<i32> {
    YAML_MIGRATIONS.iter().map(|m| m.id).collect()
}

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
	e.message,
	e.created
FROM out_of_band_migrations m
LEFT JOIN out_of_band_migrations_errors e ON e.migration_id = m.id
WHERE m.id = ANY($1)
ORDER BY m.id desc, e.created DESC
"#;

const LIST_FALLBACK_QUERY: &str = r#"
WITH split_migrations AS (
	SELECT
		m.*,
		regexp_matches(m.introduced, E'^(\\d+)\.(\\d+)') AS introduced_parts,
		regexp_matches(m.deprecated, E'^(\\d+)\.(\\d+)') AS deprecated_parts
	FROM out_of_band_migrations m
)
SELECT
	m.id,
	m.team,
	m.component,
	m.description,
	introduced_parts[1] AS introduced_version_major,
	introduced_parts[2] AS introduced_version_minor,
	CASE WHEN m.deprecated = '' THEN NULL ELSE deprecated_parts[1] END AS deprecated_version_major,
	CASE WHEN m.deprecated = '' THEN NULL ELSE deprecated_parts[2] END AS deprecated_version_minor,
	m.progress,
	m.created,
	m.last_updated,
	m.non_destructive,
	-- Note that we use true here as a default as we only expect to require this fallback
	-- query when using a newer migrator version against an old instance, and multi-version
	-- upgrades are an enterprise feature.
	true AS is_enterprise,
	m.apply_reverse,
	m.metadata,
	e.message,
	e.created
FROM split_migrations m
LEFT JOIN out_of_band_migrations_errors e ON e.migration_id = m.id
WHERE m.id = ANY($1)
ORDER BY m.id desc, e.created DESC
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
	e.message,
	e.created
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
	e.message,
	e.created
FROM out_of_band_migrations m
LEFT JOIN out_of_band_migrations_errors e ON e.migration_id = m.id
WHERE m.id = ANY($1)
ORDER BY m.id ASC, e.created DESC
"#;

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
	team = $2,
	component = $3,
	description = $4,
	non_destructive = $5,
	is_enterprise = $6,
	introduced_version_major = $7,
	introduced_version_minor = $8,
	deprecated_version_major = $9,
	deprecated_version_minor = $10
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
	team = $2,
	component = $3,
	description = $4,
	non_destructive = $5,
	introduced = $6,
	deprecated = $7
"#;

/// The columns whose absence (Postgres `42703`) selects the pre-3.29
/// fallback queries.
const FALLBACK_COLUMNS: [&str; 5] = [
    "is_enterprise",
    "introduced_version_major",
    "introduced_version_minor",
    "deprecated_version_major",
    "deprecated_version_minor",
];

/// `shouldFallback`.
fn should_fallback(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = err {
        if db.code().as_deref() == Some("42703") {
            let message = db.message();
            return FALLBACK_COLUMNS
                .iter()
                .any(|column| message.contains(column));
        }
    }
    false
}

fn scan_migrations(rows: Vec<PgRow>) -> Result<Vec<Migration>, sqlx::Error> {
    let mut values: Vec<Migration> = Vec::new();

    for row in rows {
        let deprecated_major: Option<i32> = row.try_get(6)?;
        let deprecated_minor: Option<i32> = row.try_get(7)?;
        let message: Option<String> = row.try_get(15)?;
        let error_created: Option<OffsetDateTime> = row.try_get(16)?;

        let mut value = Migration {
            id: row.try_get(0)?,
            team: row.try_get(1)?,
            component: row.try_get(2)?,
            description: row.try_get(3)?,
            introduced_major: row.try_get(4)?,
            introduced_minor: row.try_get(5)?,
            deprecated_major: None,
            deprecated_minor: None,
            progress: row.try_get(8)?,
            created: row.try_get(9)?,
            last_updated: row.try_get(10)?,
            non_destructive: row.try_get(11)?,
            is_enterprise: row.try_get(12)?,
            apply_reverse: row.try_get(13)?,
            metadata: row.try_get(14)?,
            errors: Vec::new(),
        };

        if let (Some(major), Some(minor)) = (deprecated_major, deprecated_minor) {
            value.deprecated_major = Some(major);
            value.deprecated_minor = Some(minor);
        }

        let has_error = message.as_deref().is_some_and(|m| !m.is_empty());
        if has_error {
            value.errors.push(MigrationError {
                message: message.unwrap_or_default(),
                created: error_created.expect("error row always has created"),
            });
        }

        if let Some(previous) = values.last_mut() {
            if previous.id == value.id {
                previous.errors.append(&mut value.errors);
                continue;
            }
        }

        values.push(value);
    }

    Ok(values)
}

/// `Store.SynchronizeMetadata`: upsert the embedded YAML metadata.
pub async fn synchronize_metadata(pool: &PgPool) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;

    for migration in YAML_MIGRATIONS {
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
            .execute(&mut *tx)
            .await;

        if let Err(err) = result {
            if should_fallback(&err) {
                // Roll back the partially applied upserts before trying the
                // old table shape, like `tx.Done(err)` in the baseline.
                let _ = tx.rollback().await;
                return synchronize_metadata_fallback(pool).await;
            }
            return Err(err.to_string());
        }
    }

    tx.commit().await.map_err(|e| e.to_string())
}

async fn synchronize_metadata_fallback(pool: &PgPool) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;

    for migration in YAML_MIGRATIONS {
        let introduced = version_string(
            migration.introduced_version_major,
            migration.introduced_version_minor,
        );
        let deprecated = match (
            migration.deprecated_version_major,
            migration.deprecated_version_minor,
        ) {
            (Some(major), Some(minor)) => Some(version_string(major, minor)),
            _ => None,
        };

        sqlx::query(SYNCHRONIZE_METADATA_FALLBACK_UPSERT_QUERY)
            .bind(migration.id)
            .bind(migration.team)
            .bind(migration.component)
            .bind(migration.description)
            .bind(migration.non_destructive)
            .bind(introduced)
            .bind(deprecated)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
    }

    tx.commit().await.map_err(|e| e.to_string())
}

/// `Store.List`: all migrations defined by the embedded YAML, newest id first.
pub async fn list(pool: &PgPool) -> Result<Vec<Migration>, String> {
    let ids = yaml_migration_ids();

    match sqlx::query(LIST_QUERY).bind(&ids).fetch_all(pool).await {
        Ok(rows) => scan_migrations(rows).map_err(|e| e.to_string()),
        Err(err) => {
            if !should_fallback(&err) {
                return Err(err.to_string());
            }
            let rows = sqlx::query(LIST_FALLBACK_QUERY)
                .bind(&ids)
                .fetch_all(pool)
                .await
                .map_err(|e| e.to_string())?;
            scan_migrations(rows).map_err(|e| e.to_string())
        }
    }
}

/// `Store.GetByID`: a false-like `None` when the id is unknown.
pub async fn get_by_id(pool: &PgPool, id: i32) -> Result<Option<Migration>, String> {
    let rows = sqlx::query(GET_BY_ID_QUERY)
        .bind(id)
        .fetch_all(pool)
        .await
        .map_err(|e| e.to_string())?;

    let migrations = scan_migrations(rows).map_err(|e| e.to_string())?;
    Ok(migrations.into_iter().next())
}

/// `Store.GetByIDs`; errors when any requested id is absent.
pub async fn get_by_ids(pool: &PgPool, ids: &[i32]) -> Result<Vec<Migration>, String> {
    let rows = sqlx::query(GET_BY_IDS_QUERY)
        .bind(ids)
        .fetch_all(pool)
        .await
        .map_err(|e| e.to_string())?;
    let migrations = scan_migrations(rows).map_err(|e| e.to_string())?;

    let received: std::collections::HashSet<i32> = migrations.iter().map(|m| m.id).collect();
    let mut difference: Vec<i32> = ids
        .iter()
        .copied()
        .filter(|id| !received.contains(id))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    difference.sort_unstable();

    if !difference.is_empty() {
        let list = difference
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        return Err(format!("unknown migration id(s) [{list}]"));
    }

    Ok(migrations)
}

/// `Store.UpdateDirection`.
pub async fn update_direction(pool: &PgPool, id: i32, apply_reverse: bool) -> Result<(), String> {
    sqlx::query("UPDATE out_of_band_migrations SET apply_reverse = $1 WHERE id = $2")
        .bind(apply_reverse)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// `Store.UpdateProgress` (`last_updated` is client-side `time.Now()`).
pub async fn update_progress(pool: &PgPool, id: i32, progress: f64) -> Result<(), String> {
    update_progress_at(pool, id, progress, OffsetDateTime::now_utc()).await
}

pub async fn update_progress_at(
    pool: &PgPool,
    id: i32,
    progress: f64,
    now: OffsetDateTime,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE out_of_band_migrations SET progress = $1, last_updated = $2 WHERE id = $3 AND progress != $1",
    )
    .bind(progress)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// `Store.UpdateMetadata`.
pub async fn update_metadata(pool: &PgPool, id: i32, metadata: &Value) -> Result<(), String> {
    update_metadata_at(pool, id, metadata, OffsetDateTime::now_utc()).await
}

pub async fn update_metadata_at(
    pool: &PgPool,
    id: i32,
    metadata: &Value,
    now: OffsetDateTime,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE out_of_band_migrations SET metadata = $1, last_updated = $2 WHERE id = $3 AND metadata != $1",
    )
    .bind(metadata)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// `Store.AddError` + pruning to [`MAX_MIGRATION_ERRORS`].
pub async fn add_error(pool: &PgPool, id: i32, message: &str) -> Result<(), String> {
    add_error_at(pool, id, message, OffsetDateTime::now_utc()).await
}

pub async fn add_error_at(
    pool: &PgPool,
    id: i32,
    message: &str,
    now: OffsetDateTime,
) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;

    sqlx::query("INSERT INTO out_of_band_migrations_errors (migration_id, message, created) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(message)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;

    sqlx::query("UPDATE out_of_band_migrations SET last_updated = $1 where id = $2")
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;

    sqlx::query(
        "DELETE FROM out_of_band_migrations_errors WHERE id IN (
	SELECT id FROM out_of_band_migrations_errors WHERE migration_id = $1 ORDER BY created DESC OFFSET $2
)",
    )
    .bind(id)
    .bind(MAX_MIGRATION_ERRORS)
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;

    tx.commit().await.map_err(|e| e.to_string())
}

/// `versionString`: `"%d.%d.0"`.
pub fn version_string(major: i32, minor: i32) -> String {
    format!("{major}.{minor}.0")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_migrations_are_sorted_and_unique() {
        let ids = yaml_migration_ids();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted);
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len());
    }

    #[test]
    fn complete_matches_direction() {
        let mut migration = Migration {
            id: 1,
            team: String::new(),
            component: String::new(),
            description: String::new(),
            introduced_major: 0,
            introduced_minor: 0,
            deprecated_major: None,
            deprecated_minor: None,
            progress: 1.0,
            created: OffsetDateTime::now_utc(),
            last_updated: None,
            non_destructive: true,
            is_enterprise: false,
            apply_reverse: false,
            metadata: Value::Null,
            errors: Vec::new(),
        };
        assert!(migration.complete());
        migration.apply_reverse = true;
        assert!(!migration.complete());
        migration.progress = 0.0;
        assert!(migration.complete());
        migration.apply_reverse = false;
        assert!(!migration.complete());
    }

    #[test]
    fn version_string_formats_patch_zero() {
        assert_eq!(version_string(3, 25), "3.25.0");
    }
}
