#![forbid(unsafe_code)]

//! `internal/oobmigration/migrations/iam`: IAM/product migrations.
//!
//! Ported: `15` subscription account number, `16` license key fields and `22`
//! unified permissions. All three are self-contained SQL/progress loops.
//! There are no unported IAM migrations.

use std::time::Duration;

use async_trait::async_trait;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use serde::Deserialize;
use sqlx::PgPool;

use crate::error::MigratorError;
use std::sync::Arc;

use super::registry::MigratorRegistry;

use super::model::Migrator;

/// `authz.SourceUserSync`.
pub const SOURCE_USER_SYNC: &str = "user_sync";
/// `authz.SourceAPI`.
pub const SOURCE_API: &str = "api";

/// `subscriptionAccountNumberMigrator` (`subscription_account_number_migrator.go`), ID 15.
#[derive(Debug, Clone)]
pub struct SubscriptionAccountNumberMigrator {
    pub batch_size: i32,
}

impl Default for SubscriptionAccountNumberMigrator {
    fn default() -> Self {
        Self { batch_size: 500 }
    }
}

#[async_trait]
impl Migrator for SubscriptionAccountNumberMigrator {
    fn id(&self) -> i32 {
        15
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(SUBSCRIPTION_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("subscription migration progress: {error}"))
            })
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        sqlx::query(SUBSCRIPTION_UP_QUERY)
            .bind(self.batch_size)
            .execute(db)
            .await
            .map_err(|error| MigratorError::new(format!("subscription migration: {error}")))?;
        Ok(())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Non-destructive.
        Ok(())
    }
}

const SUBSCRIPTION_PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		cast(c1.count as float) / cast(c2.count as float)
	END
FROM
	(SELECT count(*) as count FROM product_subscriptions WHERE account_number IS NOT NULL) c1,
	(SELECT count(*) as count FROM product_subscriptions) c2
"#;

const SUBSCRIPTION_UP_QUERY: &str = r#"
WITH candidates AS (
	SELECT
		product_subscriptions.id::uuid AS subscription_id,
		COALESCE(split_part(users.username, '-', 2), '') AS account_number
	FROM product_subscriptions
	JOIN users ON product_subscriptions.user_id = users.id
	WHERE product_subscriptions.account_number IS NULL
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
UPDATE product_subscriptions
SET account_number = candidates.account_number
FROM candidates
WHERE product_subscriptions.id = candidates.subscription_id
"#;

/// `licenseKeyFieldsMigrator` (`license_key_field_migrator.go`), ID 16.
#[derive(Debug, Clone)]
pub struct LicenseKeyFieldsMigrator {
    pub batch_size: i32,
}

impl Default for LicenseKeyFieldsMigrator {
    fn default() -> Self {
        Self { batch_size: 500 }
    }
}

#[derive(Deserialize)]
struct DecodedLicenseKey {
    /// Base64-encoded JSON payload (Go's `[]byte` JSON decoding).
    info: String,
}

#[derive(Deserialize, Default)]
struct LicenseInfo {
    #[serde(default, rename = "t")]
    tags: Option<Vec<String>>,
    #[serde(default, rename = "u")]
    user_count: i32,
    #[serde(default, rename = "e")]
    expires_at: Option<String>,
}

#[async_trait]
impl Migrator for LicenseKeyFieldsMigrator {
    fn id(&self) -> i32 {
        16
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(LICENSE_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("license migration progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        let mut tx = db
            .begin()
            .await
            .map_err(|error| MigratorError::new(format!("license migration begin: {error}")))?;

        let rows = sqlx::query(LICENSE_SELECT_QUERY)
            .bind(self.batch_size)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| MigratorError::new(format!("license migration select: {error}")))?;

        for row in rows {
            use sqlx::Row as _;

            let id: String = row
                .try_get("id")
                .map_err(|error| MigratorError::new(format!("license id scan: {error}")))?;
            let license_key: String = row
                .try_get("license_key")
                .map_err(|error| MigratorError::new(format!("license key scan: {error}")))?;

            let info = decode_license_key(&license_key)?;

            sqlx::query(LICENSE_UPDATE_QUERY)
                .bind(1_i32)
                .bind(info.tags)
                .bind(info.user_count)
                .bind(info.expires_at)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    MigratorError::new(format!("license migration update: {error}"))
                })?;
        }

        tx.commit()
            .await
            .map_err(|error| MigratorError::new(format!("license migration commit: {error}")))?;

        Ok(())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Non-destructive.
        Ok(())
    }
}

fn decode_license_key(license_key: &str) -> Result<LicenseInfo, MigratorError> {
    let decoded_text = URL_SAFE_NO_PAD
        .decode(license_key)
        .map_err(|error| MigratorError::new(format!("decode license key: {error}")))?;

    let decoded_key: DecodedLicenseKey = serde_json::from_slice(&decoded_text)
        .map_err(|error| MigratorError::new(format!("parse license key: {error}")))?;

    let info_bytes = STANDARD
        .decode(decoded_key.info)
        .map_err(|error| MigratorError::new(format!("decode license info: {error}")))?;

    serde_json::from_slice(&info_bytes)
        .map_err(|error| MigratorError::new(format!("parse license info: {error}")))
}

const LICENSE_PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		cast(c1.count as float) / cast(c2.count as float)
	END
FROM
	(SELECT count(*) as count FROM product_licenses WHERE license_tags IS NOT NULL) c1,
	(SELECT count(*) as count FROM product_licenses) c2
"#;

const LICENSE_SELECT_QUERY: &str = r#"
SELECT
	id::text AS id,
	license_key
FROM product_licenses
WHERE license_tags IS NULL
LIMIT $1
FOR UPDATE SKIP LOCKED
"#;

const LICENSE_UPDATE_QUERY: &str = r#"
UPDATE product_licenses
SET
	license_version    = $1,
	license_tags       = $2::text[],
	license_user_count = $3,
	license_expires_at = $4::timestamptz
WHERE id = $5::uuid
"#;

/// `unifiedPermissionsMigrator` (`unified_permissions_migrator.go`), ID 22.
#[derive(Debug, Clone)]
pub struct UnifiedPermissionsMigrator {
    pub batch_size: i32,
    /// `authz.SourceUserSync` unless permissions mappings are enabled.
    pub source: String,
}

impl Default for UnifiedPermissionsMigrator {
    fn default() -> Self {
        Self {
            batch_size: 100,
            source: SOURCE_USER_SYNC.to_string(),
        }
    }
}

#[async_trait]
impl Migrator for UnifiedPermissionsMigrator {
    fn id(&self) -> i32 {
        22
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(60)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(UNIFIED_PERMISSIONS_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("unified permissions progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        sqlx::query(UNIFIED_PERMISSIONS_QUERY)
            .bind(self.batch_size)
            .bind(&self.source)
            .execute(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("unified permissions migration: {error}"))
            })?;
        Ok(())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Non-destructive.
        Ok(())
    }
}

const UNIFIED_PERMISSIONS_PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		cast(c1.count as float) / cast(c2.count as float)
	END
FROM
	(SELECT count(*) as count FROM user_permissions WHERE migrated) c1,
	(SELECT count(*) as count FROM user_permissions) c2
"#;

const UNIFIED_PERMISSIONS_QUERY: &str = r#"
WITH candidates AS (
	SELECT u.user_id, u.permission, u.object_type
	FROM user_permissions as u
	WHERE NOT migrated
	LIMIT $1
	FOR UPDATE SKIP LOCKED
),
s AS (
	SELECT
		u.user_id,
		unnest(u.object_ids_ints) as repo_id,
		u.updated_at as created_at,
		u.updated_at,
		$2 as source,
		u.permission,
		u.object_type
	FROM user_permissions as u
	INNER JOIN candidates ON
		candidates.user_id = u.user_id
		AND candidates.permission = u.permission
		AND candidates.object_type = u.object_type
),
ins AS (
	INSERT INTO user_repo_permissions(user_id, user_external_account_id, repo_id, created_at, updated_at, source)
	SELECT s.user_id, ua.id as user_external_account_id, s.repo_id, s.created_at, s.updated_at, s.source
	FROM s
	INNER JOIN repo AS r ON
		r.deleted_at IS NULL AND r.id = s.repo_id
	INNER JOIN user_external_accounts AS ua ON
		ua.user_id = s.user_id AND ua.deleted_at IS NULL
		AND ua.service_type = r.external_service_type
		AND ua.service_id = r.external_service_id
	ON CONFLICT (user_id, user_external_account_id, repo_id) DO NOTHING
)
UPDATE user_permissions
SET migrated = TRUE
FROM candidates AS c
WHERE user_permissions.user_id = c.user_id
	AND user_permissions.permission = c.permission
	AND user_permissions.object_type = c.object_type
"#;

/// No unported IAM migrations.
pub const UNPORTED: &[(i32, &str)] = &[];

/// `registerOSSMigrators` for the iam family (none).
pub fn register_oss(_registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    Ok(())
}

/// `registerEnterpriseMigrators` for the iam family.
pub fn register_enterprise(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    registry.register(Arc::new(SubscriptionAccountNumberMigrator::default()))?;
    registry.register(Arc::new(LicenseKeyFieldsMigrator::default()))?;
    registry.register(Arc::new(UnifiedPermissionsMigrator::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_license_key() {
        // {"info":"<base64 of {\"t\":[\"plan:enterprise\"],\"u\":42,\"e\":\"2024-01-02T03:04:05Z\"}>"}
        let info = serde_json::json!({
            "t": ["plan:enterprise"],
            "u": 42,
            "e": "2024-01-02T03:04:05Z"
        })
        .to_string();
        let inner = STANDARD.encode(info.as_bytes());
        let outer_json = serde_json::json!({ "info": inner }).to_string();
        let key = URL_SAFE_NO_PAD.encode(outer_json.as_bytes());

        let decoded = decode_license_key(&key).unwrap();
        assert_eq!(decoded.user_count, 42);
        assert_eq!(decoded.tags, Some(vec!["plan:enterprise".to_string()]));
        assert_eq!(decoded.expires_at.as_deref(), Some("2024-01-02T03:04:05Z"));
    }
}
