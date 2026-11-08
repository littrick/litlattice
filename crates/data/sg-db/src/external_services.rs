//! Read-only view of `external_services`, as needed by gitserver's remote URL
//! lookup (`internal/database/external_services.go`: `GetByID` → `List` with
//! `IncludeDeleted=false`).
//!
//! `config` stays encrypted exactly as stored; callers decrypt it with the
//! keyring identified by `encryption_key_id` (`extsvc.NewEncryptedConfig`).

use sqlx::postgres::PgPool;
use sqlx::Row;

/// Subset of `types.ExternalService` used by the gitserver write path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalService {
    pub id: i64,
    pub kind: String,
    pub display_name: String,
    /// The encrypted JSON configuration, as stored in `external_services.config`.
    pub config: String,
    /// `''` when the config is not encrypted.
    pub encryption_key_id: String,
}

/// Lookup failures. The baseline's `GetByID` returns
/// `externalServiceNotFoundError` for a missing or soft-deleted service and
/// the raw database error otherwise.
#[derive(Debug)]
pub enum ExternalServiceError {
    NotFound { id: i64 },
    Database(sqlx::Error),
}

impl std::fmt::Display for ExternalServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { id } => write!(f, "external service not found: id {id}"),
            Self::Database(err) => write!(f, "external service: {err}"),
        }
    }
}

impl std::error::Error for ExternalServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotFound { .. } => None,
            Self::Database(err) => Some(err),
        }
    }
}

impl From<sqlx::Error> for ExternalServiceError {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err)
    }
}

#[derive(Clone)]
pub struct ExternalServiceStore {
    pool: PgPool,
}

impl ExternalServiceStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `ExternalServices().GetByID`: soft-deleted services are not returned.
    pub async fn get_by_id(&self, id: i64) -> Result<ExternalService, ExternalServiceError> {
        let row = sqlx::query(
            r#"
SELECT id, kind, display_name, config, encryption_key_id
FROM external_services
WHERE id = $1 AND deleted_at IS NULL
"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ExternalServiceError::NotFound { id })?;

        Ok(ExternalService {
            id: row.try_get("id")?,
            kind: row.try_get("kind")?,
            display_name: row.try_get("display_name")?,
            config: row.try_get("config")?,
            encryption_key_id: row.try_get("encryption_key_id")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_live_external_services_and_hides_deleted_ones() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = ExternalServiceStore::new(pool.clone());

        let service_id: i64 = sqlx::query_scalar(
            "INSERT INTO external_services(kind, display_name, config, encryption_key_id) \
             VALUES ('GITHUB', 'gh', '{\"token\":\"secret\"}', 'key-1') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .expect("insert service");

        let service = store.get_by_id(service_id).await.expect("get");
        assert_eq!(
            service,
            ExternalService {
                id: service_id,
                kind: "GITHUB".to_owned(),
                display_name: "gh".to_owned(),
                config: "{\"token\":\"secret\"}".to_owned(),
                encryption_key_id: "key-1".to_owned(),
            }
        );

        sqlx::query("UPDATE external_services SET deleted_at = NOW() WHERE id = $1")
            .bind(service_id)
            .execute(&pool)
            .await
            .expect("soft delete");
        let err = store.get_by_id(service_id).await.expect_err("deleted");
        assert!(matches!(
            err,
            ExternalServiceError::NotFound { id } if id == service_id
        ));

        sqlx::query("DELETE FROM external_services WHERE id = $1")
            .bind(service_id)
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    async fn missing_external_service_reports_its_id() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = ExternalServiceStore::new(pool);

        let err = store.get_by_id(-1).await.expect_err("missing");
        assert!(err.to_string().contains("-1"), "{err}");
    }
}
