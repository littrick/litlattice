//! `internal/oobmigration/migrations/batches/extsvc_webhook_migrator.go`,
//! migration `13`.
//!
//! Decrypts each external service config, parses it as JSONC (comments and
//! trailing commas are tolerated) and records whether the config contains
//! webhooks in `external_services.has_webhooks`.
//!
//! The Go migration only *reads* the config; it never rewrites it, so this
//! port does not perform any JSONC edits either. `sg_jsonx::parse` is used for
//! the tolerant parse so the accepted grammar matches `internal/jsonc.Parse`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::{PgPool, Row};

use sg_encryption::Key;
use sg_jsonx::{parse, JsonValue, ParseOptions};

use crate::error::MigratorError;

use super::super::model::Migrator;

const PARSE_OPTIONS: ParseOptions = ParseOptions {
    comments: true,
    trailing_commas: true,
};

/// `externalServiceWebhookMigrator`.
#[derive(Clone)]
pub struct ExternalServiceWebhookMigrator {
    pub batch_size: i32,
    key: Option<Arc<dyn Key>>,
}

impl std::fmt::Debug for ExternalServiceWebhookMigrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalServiceWebhookMigrator")
            .field("batch_size", &self.batch_size)
            .field("has_key", &self.key.is_some())
            .finish()
    }
}

impl Default for ExternalServiceWebhookMigrator {
    fn default() -> Self {
        Self::new(50, None)
    }
}

impl ExternalServiceWebhookMigrator {
    pub fn new(batch_size: i32, key: Option<Arc<dyn Key>>) -> Self {
        Self { batch_size, key }
    }

    /// Build from the process keyring (`keyring.Default().ExternalServiceKey`).
    pub fn from_keyring(keyring: &sg_keyring::Keyring, batch_size: i32) -> Self {
        Self::new(batch_size, keyring.external_service_key())
    }
}

#[async_trait]
impl Migrator for ExternalServiceWebhookMigrator {
    fn id(&self) -> i32 {
        13
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(3)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(WEBHOOK_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("external service webhook progress: {error}"))
            })
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        let mut tx = db.begin().await.map_err(|error| {
            MigratorError::new(format!("external service webhook begin: {error}"))
        })?;

        let rows = sqlx::query(WEBHOOK_SELECT_QUERY)
            .bind(self.batch_size)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!("external service webhook select: {error}"))
            })?;

        let mut services = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i32 = row.try_get("id").map_err(|error| {
                MigratorError::new(format!("external service webhook id: {error}"))
            })?;
            let kind: String = row.try_get("kind").map_err(|error| {
                MigratorError::new(format!("external service webhook kind: {error}"))
            })?;
            let config: String = row.try_get("config").map_err(|error| {
                MigratorError::new(format!("external service webhook config: {error}"))
            })?;
            let key_id: String = row.try_get("encryption_key_id").map_err(|error| {
                MigratorError::new(format!("external service webhook key id: {error}"))
            })?;

            let plaintext = sg_encryption::maybe_decrypt(self.key.as_deref(), &config, &key_id)
                .await
                .map_err(|error| {
                    MigratorError::new(format!(
                        "external service webhook decrypt service {id}: {error}"
                    ))
                })?;

            services.push((id, kind, plaintext));
        }

        // Parse failures are non-fatal: the batch still commits the rows that
        // parsed, and the errors are returned afterwards (Go's `parseErrs`).
        let mut parse_errors = Vec::new();
        for (id, kind, config) in services {
            match has_webhooks(&kind, &config) {
                Ok(has_webhooks) => {
                    sqlx::query(WEBHOOK_UPDATE_QUERY)
                        .bind(has_webhooks)
                        .bind(id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|error| {
                            MigratorError::new(format!(
                                "external service webhook update service {id}: {error}"
                            ))
                        })?;
                }
                Err(error) => parse_errors.push(format!("service {id}: {error}")),
            }
        }

        tx.commit().await.map_err(|error| {
            MigratorError::new(format!("external service webhook commit: {error}"))
        })?;

        if parse_errors.is_empty() {
            Ok(())
        } else {
            Err(MigratorError::new(format!(
                "external service webhook: {} config(s) failed to parse: {}",
                parse_errors.len(),
                parse_errors.join("; ")
            )))
        }
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Non-destructive.
        Ok(())
    }
}

const WEBHOOK_PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		CAST(c1.count AS float) / CAST(c2.count AS float)
	END
FROM
	(SELECT COUNT(*) AS count FROM external_services WHERE deleted_at IS NULL AND has_webhooks IS NOT NULL) c1,
	(SELECT COUNT(*) AS count FROM external_services WHERE deleted_at IS NULL) c2
"#;

const WEBHOOK_SELECT_QUERY: &str = r#"
SELECT id, kind, config, encryption_key_id
FROM external_services
WHERE deleted_at IS NULL AND has_webhooks IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE
"#;

const WEBHOOK_UPDATE_QUERY: &str = r#"
UPDATE external_services SET has_webhooks = $1 WHERE id = $2
"#;

/// `hasWebhooks`: the Go switch over `strings.ToUpper(kind)`.
fn has_webhooks(kind: &str, raw_config: &str) -> Result<bool, String> {
    match kind.to_ascii_uppercase().as_str() {
        "GITHUB" | "GITLAB" => {
            let root = parse_jsonc(raw_config)?;
            // `jsonGitHubGitLabConfig{ Webhooks []*jsonWebhook }`
            let Some(object) = root.as_object() else {
                if root.is_null() {
                    return Ok(false);
                }
                return Err("config is not a JSON object".to_owned());
            };
            let some = ci_get(object, "webhooks");
            match some {
                None | Some(Value::Null) => Ok(false),
                Some(Value::Array(items)) => {
                    for item in items {
                        if item.is_null() {
                            continue;
                        }
                        let Some(webhook) = item.as_object() else {
                            return Err("webhook is not an object".to_owned());
                        };
                        secret(webhook)?;
                    }
                    Ok(!items.is_empty())
                }
                Some(_) => Err("webhooks is not an array".to_owned()),
            }
        }
        "BITBUCKETSERVER" => {
            let root = parse_jsonc(raw_config)?;
            let Some(object) = root.as_object() else {
                if root.is_null() {
                    return Ok(false);
                }
                return Err("config is not a JSON object".to_owned());
            };

            let has_webhooks = match ci_get(object, "webhooks") {
                None | Some(Value::Null) => false,
                Some(Value::Object(webhook)) => !secret(webhook)?.is_empty(),
                Some(_) => return Err("webhooks is not an object".to_owned()),
            };

            let has_plugin_webhooks = match ci_get(object, "plugin") {
                None | Some(Value::Null) => false,
                Some(Value::Object(plugin)) => match ci_get(plugin, "webhooks") {
                    None | Some(Value::Null) => false,
                    Some(Value::Object(webhook)) => !secret(webhook)?.is_empty(),
                    Some(_) => return Err("plugin.webhooks is not an object".to_owned()),
                },
                Some(_) => return Err("plugin is not an object".to_owned()),
            };

            Ok(has_webhooks || has_plugin_webhooks)
        }
        _ => Ok(false),
    }
}

fn secret(object: &serde_json::Map<String, Value>) -> Result<String, String> {
    match ci_get(object, "secret") {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(value)) => Ok(value.clone()),
        Some(_) => Err("webhook secret is not a string".to_owned()),
    }
}

fn ci_get<'a>(object: &'a serde_json::Map<String, Value>, name: &str) -> Option<&'a Value> {
    if let Some(value) = object.get(name) {
        return Some(value);
    }
    object
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

/// `internal/jsonc.Parse` + `json.Unmarshal`: tolerant parse, then plain JSON.
/// A document with no root value is the JSON `null` document.
fn parse_jsonc(raw: &str) -> Result<Value, String> {
    let (value, errors) = parse(raw, PARSE_OPTIONS);
    if !errors.is_empty() {
        let messages: Vec<&str> = errors.iter().map(|error| error.code_name()).collect();
        return Err(format!("failed to parse JSON: {}", messages.join(", ")));
    }
    match value {
        None => Ok(Value::Null),
        Some(value) => json_value(value),
    }
}

fn json_value(value: JsonValue) -> Result<Value, String> {
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Bool(value),
        JsonValue::String(value) => Value::String(value),
        JsonValue::Number(lexeme) => parse_number(&lexeme)?,
        JsonValue::Array(items) => Value::Array(
            items
                .into_iter()
                .map(json_value)
                .collect::<Result<_, _>>()?,
        ),
        JsonValue::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| Ok((key, json_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, String>>()
                .map(|map| map.into_iter().collect())?,
        ),
    })
}

fn parse_number(lexeme: &str) -> Result<Value, String> {
    if let Ok(value) = lexeme.parse::<i64>() {
        return Ok(Value::Number(value.into()));
    }
    if let Ok(value) = lexeme.parse::<u64>() {
        return Ok(Value::Number(value.into()));
    }
    let value: f64 = lexeme
        .parse()
        .map_err(|_| format!("invalid number {lexeme}"))?;
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| format!("invalid number {lexeme}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_webhooks_with_comments_and_trailing_commas() {
        let config = r#"
        {
            // inline comment
            "webhooks": [
                {"secret": "s"},
            ],
        }
        "#;
        assert!(has_webhooks("GITHUB", config).unwrap());
        assert!(has_webhooks("github", "{}").unwrap() == false);
        assert!(has_webhooks("GitLab", r#"{"webhooks": []}"#).unwrap() == false);
    }

    #[test]
    fn bitbucket_server_webhooks_and_plugin() {
        let direct = r#"{"webhooks": {"secret": "s"}}"#;
        assert!(has_webhooks("BITBUCKETSERVER", direct).unwrap());

        let plugin = r#"{"plugin": {"webhooks": {"secret": "s"}}}"#;
        assert!(has_webhooks("bitbucketserver", plugin).unwrap());

        let empty = r#"{"webhooks": {"secret": ""}, "plugin": {"webhooks": {}}}"#;
        assert!(!has_webhooks("BITBUCKETSERVER", empty).unwrap());
    }

    #[test]
    fn unknown_kinds_have_no_webhooks() {
        assert!(!has_webhooks("GERRIT", r#"{"webhooks": [{"secret": "s"}]}"#).unwrap());
    }

    #[test]
    fn malformed_configs_error() {
        assert!(has_webhooks("GITHUB", r#"{"webhooks": "nope"}"#).is_err());
        assert!(has_webhooks("GITHUB", r#"{"webhooks": 5}"#).is_err());
        assert!(has_webhooks("GITHUB", r#"{"webhooks": [5]}"#).is_err());
        assert!(has_webhooks("BITBUCKETSERVER", r#"{"plugin": 5}"#).is_err());
        assert!(has_webhooks("BITBUCKETSERVER", r#"{"webhooks": {"secret": 5}}"#).is_err());
        assert!(has_webhooks("GITHUB", "{ not json").is_err());
    }

    #[test]
    fn null_documents_are_empty() {
        assert!(!has_webhooks("GITHUB", "").unwrap());
        assert!(!has_webhooks("BITBUCKETSERVER", "null").unwrap());
    }

    #[tokio::test]
    #[ignore = "requires Postgres (set DATABASE_URL)"]
    async fn populates_has_webhooks_against_postgres() {
        let dsn = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/postgres".to_owned());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect to Postgres");

        sqlx::query(
            r#"CREATE TEMP TABLE external_services (
                id integer PRIMARY KEY,
                kind text NOT NULL,
                config text NOT NULL,
                encryption_key_id text NOT NULL DEFAULT '',
                deleted_at timestamptz,
                has_webhooks boolean
            )"#,
        )
        .execute(&pool)
        .await
        .expect("create temp table");

        sqlx::query(
            r#"INSERT INTO external_services (id, kind, config) VALUES
               (1, 'GITHUB', '{"webhooks": [{"secret": "s"},], // c
               }'),
               (2, 'GITLAB', '{}'),
               (3, 'BITBUCKETSERVER', '{"plugin": {"webhooks": {"secret": "s"}}}')"#,
        )
        .execute(&pool)
        .await
        .expect("insert services");

        ExternalServiceWebhookMigrator::default()
            .up(&pool)
            .await
            .expect("up");

        let rows = sqlx::query("SELECT id, has_webhooks FROM external_services ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("select");
        let values: Vec<(i32, Option<bool>)> = rows
            .iter()
            .map(|row| (row.get("id"), row.get("has_webhooks")))
            .collect();
        assert_eq!(
            values,
            vec![(1, Some(true)), (2, Some(false)), (3, Some(true))]
        );
    }
}
