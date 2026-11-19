//! `internal/oobmigration/migrations/batches/external_fork_name_migrator.go`,
//! migration `21`.
//!
//! For changesets published on Bitbucket Server or Bitbucket Cloud, decode the
//! typed changeset metadata (`ScanChangeset`'s switch) and copy the fork
//! repository name into `changesets.external_fork_name`.
//!
//! * Bitbucket Server: `PullRequest.FromRef.Repository.Slug`.
//! * Bitbucket Cloud: `AnnotatedPullRequest.Source.Repo.Name`, i.e.
//!   `source.repository.name` in the stored JSON.
//!
//! Only the columns the migration reads are selected (`id`,
//! `external_service_type`, `metadata`); the predicate, ordering, batching and
//! progress query mirror the Go query.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::error::MigratorError;

use super::super::model::Migrator;

/// `externalForkNameMigrator`.
#[derive(Debug, Clone)]
pub struct ExternalForkNameMigrator {
    pub batch_size: i32,
}

impl Default for ExternalForkNameMigrator {
    fn default() -> Self {
        Self { batch_size: 500 }
    }
}

#[async_trait]
impl Migrator for ExternalForkNameMigrator {
    fn id(&self) -> i32 {
        21
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(FORK_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("external fork name progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        let rows = sqlx::query(FORK_SELECT_QUERY)
            .bind(self.batch_size)
            .fetch_all(db)
            .await
            .map_err(|error| MigratorError::new(format!("external fork name select: {error}")))?;

        for row in rows {
            let id: i32 = row
                .try_get("id")
                .map_err(|error| MigratorError::new(format!("external fork name id: {error}")))?;
            let service_type: String = row.try_get("external_service_type").map_err(|error| {
                MigratorError::new(format!("external fork name service type: {error}"))
            })?;
            let metadata: Option<Value> = row.try_get("metadata").map_err(|error| {
                MigratorError::new(format!("external fork name metadata: {error}"))
            })?;

            // Go scans `metadata` into `json.RawMessage`; a SQL NULL becomes an
            // empty slice and `json.Unmarshal` fails with "unexpected end of
            // JSON input".
            let metadata = metadata.ok_or_else(|| {
                MigratorError::new(format!(
                    "scanChangeset: failed to unmarshal {service_type:?} metadata: unexpected end of JSON input"
                ))
            })?;

            let fork_name = fork_name_from_metadata(&service_type, &metadata).map_err(|error| {
                MigratorError::new(format!(
                    "scanChangeset: failed to unmarshal {service_type:?} metadata: {error}"
                ))
            })?;

            if fork_name.is_empty() {
                continue;
            }

            sqlx::query(FORK_UPDATE_QUERY)
                .bind(fork_name)
                .bind(id)
                .execute(db)
                .await
                .map_err(|error| {
                    MigratorError::new(format!("external fork name update {id}: {error}"))
                })?;
        }

        Ok(())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Non-destructive.
        Ok(())
    }
}

const FORK_PROGRESS_QUERY: &str = r#"
SELECT
	CASE total.count WHEN 0 THEN 1 ELSE
		CAST(migrated.count AS float) / CAST(total.count AS float)
	END
FROM
(SELECT COUNT(1) AS count FROM changesets
	WHERE external_fork_name IS NOT NULL
	AND external_fork_namespace IS NOT NULL
	AND external_deleted_at IS NULL
	AND external_service_type IN ('bitbucketServer', 'bitbucketCloud')) migrated,
(SELECT COUNT(1) AS count FROM changesets
	WHERE external_fork_namespace IS NOT NULL
	AND external_deleted_at IS NULL
	AND external_service_type IN ('bitbucketServer', 'bitbucketCloud')) total
"#;

const FORK_SELECT_QUERY: &str = r#"
SELECT id, external_service_type, metadata FROM changesets
	WHERE external_fork_namespace IS NOT NULL
	AND external_fork_name IS NULL
	AND external_deleted_at IS NULL
	AND external_service_type IN ('bitbucketServer', 'bitbucketCloud')
	ORDER BY id LIMIT $1 FOR UPDATE
"#;

const FORK_UPDATE_QUERY: &str = r#"
UPDATE changesets SET external_fork_name = $1 WHERE id = $2
"#;

/// `getforkName` / `ScanChangeset` metadata decode for the two Bitbucket types.
fn fork_name_from_metadata(service_type: &str, metadata: &Value) -> Result<String, String> {
    // `json.Unmarshal` into a struct rejects any non-object root (except
    // `null`, which leaves the zero value).
    match metadata {
        Value::Object(_) | Value::Null => {}
        _ => return Err("metadata is not a JSON object".to_owned()),
    }

    match service_type {
        "bitbucketServer" => {
            // `fromRef` -> `repository` -> `slug`
            let from_ref = struct_field(metadata, "fromRef")?;
            let repository = struct_field(&from_ref, "repository")?;
            string_field(&repository, "slug")
        }
        "bitbucketCloud" => {
            // `source` -> `repository` -> `name`
            let source = struct_field(metadata, "source")?;
            let repository = struct_field(&source, "repository")?;
            string_field(&repository, "name")
        }
        other => Err(format!("unknown external service type: {other}")),
    }
}

/// `encoding/json` semantics for a struct-typed field: absent or `null`
/// yield an empty object, an object yields itself, anything else is an error.
fn struct_field(value: &Value, name: &str) -> Result<Value, String> {
    match ci_get(value, name) {
        None | Some(Value::Null) => Ok(Value::Null),
        Some(value @ Value::Object(_)) => Ok(value.clone()),
        Some(_) => Err(format!("field {name} is not an object")),
    }
}

/// `encoding/json` semantics for a `string` field: absent or `null` yield the
/// empty string, a string yields itself, anything else is an error.
fn string_field(value: &Value, name: &str) -> Result<String, String> {
    match ci_get(value, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("field {name} is not a string")),
    }
}

fn ci_get<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    let object = value.as_object()?;
    if let Some(value) = object.get(name) {
        return Some(value);
    }
    object
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bitbucket_server_uses_from_ref_repository_slug() {
        let metadata = json!({
            "id": 1,
            "fromRef": {"id": "refs/heads/branch", "repository": {"slug": "fork-repo"}},
            "toRef": {"repository": {"slug": "base-repo"}}
        });
        assert_eq!(
            fork_name_from_metadata("bitbucketServer", &metadata).unwrap(),
            "fork-repo"
        );
    }

    #[test]
    fn bitbucket_cloud_uses_source_repository_name() {
        let metadata = json!({
            "source": {"repository": {"name": "fork-name"}, "branch": {"name": "branch"}},
            "destination": {"repository": {"name": "base-name"}}
        });
        assert_eq!(
            fork_name_from_metadata("bitbucketCloud", &metadata).unwrap(),
            "fork-name"
        );
    }

    #[test]
    fn missing_nested_values_yield_empty_names() {
        assert_eq!(
            fork_name_from_metadata("bitbucketServer", &json!({})).unwrap(),
            ""
        );
        assert_eq!(
            fork_name_from_metadata("bitbucketCloud", &json!({"source": null})).unwrap(),
            ""
        );
        assert_eq!(
            fork_name_from_metadata("bitbucketServer", &json!({"fromRef": {"repository": {}}}))
                .unwrap(),
            ""
        );
    }

    #[test]
    fn wrong_types_are_errors() {
        assert!(fork_name_from_metadata(
            "bitbucketServer",
            &json!({"fromRef": {"repository": {"slug": 5}}})
        )
        .is_err());
        assert!(fork_name_from_metadata("bitbucketCloud", &json!({"source": 5})).is_err());
        assert!(fork_name_from_metadata("bitbucketCloud", &json!([])).is_err());
    }

    #[test]
    fn field_matching_is_case_insensitive() {
        let metadata = json!({"FromRef": {"Repository": {"Slug": "fork"}}});
        assert_eq!(
            fork_name_from_metadata("bitbucketServer", &metadata).unwrap(),
            "fork"
        );
    }

    #[tokio::test]
    #[ignore = "requires Postgres (set DATABASE_URL)"]
    async fn populates_external_fork_name_against_postgres() {
        let dsn = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/postgres".to_owned());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect to Postgres");

        sqlx::query(
            r#"CREATE TEMP TABLE changesets (
                id integer PRIMARY KEY,
                external_service_type text NOT NULL,
                external_fork_namespace text,
                external_fork_name text,
                external_deleted_at timestamptz,
                metadata jsonb
            )"#,
        )
        .execute(&pool)
        .await
        .expect("create temp table");

        sqlx::query(
            r#"INSERT INTO changesets
                (id, external_service_type, external_fork_namespace, metadata)
               VALUES
                (1, 'bitbucketServer', 'ns',
                 '{"fromRef": {"repository": {"slug": "server-fork"}}}'),
                (2, 'bitbucketCloud', 'ns',
                 '{"source": {"repository": {"name": "cloud-fork"}}}')"#,
        )
        .execute(&pool)
        .await
        .expect("insert changesets");

        ExternalForkNameMigrator::default()
            .up(&pool)
            .await
            .expect("up");

        let rows = sqlx::query("SELECT id, external_fork_name FROM changesets ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("select");
        let values: Vec<(i32, Option<String>)> = rows
            .iter()
            .map(|row| (row.get("id"), row.get("external_fork_name")))
            .collect();
        assert_eq!(
            values,
            vec![
                (1, Some("server-fork".to_owned())),
                (2, Some("cloud-fork".to_owned())),
            ]
        );
    }
}
