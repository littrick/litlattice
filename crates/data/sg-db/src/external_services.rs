//! View of `external_services`, as needed by gitserver's remote URL lookup
//! (`internal/database/external_services.go`: `GetByID` → `List` with
//! `IncludeDeleted=false`), the syncer scheduler, the rate-limit job, and the
//! `codeintel-package-filter-applicator` job (which bumps `next_sync_at` and
//! attaches the package code host; see [`ExternalServiceStore::mark_next_sync_at`]).
//!
//! `config` stays encrypted exactly as stored; callers decrypt it with the
//! keyring identified by `encryption_key_id` (`extsvc.NewEncryptedConfig`).

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// Subset of `types.ExternalService` used by the gitserver write path and the
/// repo-updater syncer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalService {
    pub id: i64,
    pub kind: String,
    pub display_name: String,
    /// The encrypted JSON configuration, as stored in `external_services.config`.
    pub config: String,
    /// `''` when the config is not encrypted.
    pub encryption_key_id: String,
    pub last_sync_at: Option<sqlx::types::time::OffsetDateTime>,
    pub next_sync_at: Option<sqlx::types::time::OffsetDateTime>,
    pub unrestricted: bool,
    /// `external_services.has_webhooks`; `NULL` until an upsert computes it.
    pub has_webhooks: Option<bool>,
    /// `external_services.code_host_id`; `None` until an upsert attaches the
    /// service to a code host.
    pub code_host_id: Option<i32>,
}

/// Lookup failures. The baseline's `GetByID` returns
/// `externalServiceNotFoundError` for a missing or soft-deleted service and
/// the raw database error otherwise.
#[derive(Debug)]
pub enum ExternalServiceError {
    NotFound {
        id: i64,
    },
    /// `mark_next_sync_at` only handles the package repository kinds.
    InvalidKind {
        id: i64,
        kind: String,
    },
    Database(sqlx::Error),
}

impl std::fmt::Display for ExternalServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { id } => write!(f, "external service not found: id {id}"),
            Self::InvalidKind { id, kind } => {
                write!(f, "external service {id} has non-package kind {kind:?}")
            }
            Self::Database(err) => write!(f, "external service: {err}"),
        }
    }
}

impl std::error::Error for ExternalServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotFound { .. } => None,
            Self::InvalidKind { .. } => None,
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
SELECT id, kind, display_name, config, encryption_key_id,
       last_sync_at, next_sync_at, unrestricted, has_webhooks, code_host_id
FROM external_services
WHERE id = $1 AND deleted_at IS NULL
"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ExternalServiceError::NotFound { id })?;

        service_from_row(row)
    }

    /// `ExternalServices().Update` restricted to the sync timestamps (the only
    /// `ExternalServiceUpdate` fields the syncer sets). The baseline bumps
    /// `updated_at` and reports a missing/deleted service as not found.
    pub async fn update_sync_timestamps(
        &self,
        id: i64,
        last_sync_at: OffsetDateTime,
        next_sync_at: OffsetDateTime,
    ) -> Result<(), ExternalServiceError> {
        let result = sqlx::query(
            r#"
UPDATE external_services
SET last_sync_at = $2, next_sync_at = $3, updated_at = NOW()
WHERE id = $1 AND deleted_at IS NULL
"#,
        )
        .bind(id)
        .bind(last_sync_at)
        .bind(next_sync_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(ExternalServiceError::NotFound { id });
        }
        Ok(())
    }

    /// `ExternalServices().List(ctx, ExternalServicesListOptions{})`: every
    /// live service, newest first (`List` defaults `OrderByDirection` to
    /// `DESC`). Used by the worker's `rate-limit-config` job.
    pub async fn list(&self) -> Result<Vec<ExternalService>, ExternalServiceError> {
        let rows = sqlx::query(
            r#"
SELECT id, kind, display_name, config, encryption_key_id,
       last_sync_at, next_sync_at, unrestricted, has_webhooks, code_host_id
FROM external_services
WHERE deleted_at IS NULL
ORDER BY id DESC
"#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(service_from_row).collect()
    }

    /// All live external services, oldest first. Used by the syncer scheduler
    /// equivalent and the debug state dump.
    pub async fn list_all(&self) -> Result<Vec<ExternalService>, ExternalServiceError> {
        let rows = sqlx::query(
            r#"
SELECT id, kind, display_name, config, encryption_key_id,
       last_sync_at, next_sync_at, unrestricted, has_webhooks, code_host_id
FROM external_services
WHERE deleted_at IS NULL
ORDER BY id
"#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(service_from_row).collect()
    }

    /// `ExternalServices().List(ctx, ExternalServicesListOptions{Kinds: ...})`:
    /// live services of the given kinds, newest first (the default
    /// `OrderByDirection`). Used by `codeintel-package-filter-applicator` to
    /// mark every package repository external service for an immediate sync.
    pub async fn list_by_kinds(
        &self,
        kinds: &[&str],
    ) -> Result<Vec<ExternalService>, ExternalServiceError> {
        let kinds: Vec<String> = kinds.iter().map(|kind| (*kind).to_owned()).collect();
        let rows = sqlx::query(
            r#"
SELECT id, kind, display_name, config, encryption_key_id,
       last_sync_at, next_sync_at, unrestricted, has_webhooks, code_host_id
FROM external_services
WHERE deleted_at IS NULL AND kind = ANY($1)
ORDER BY id DESC
"#,
        )
        .bind(&kinds)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(service_from_row).collect()
    }

    /// One page of `ExternalServices().List(ctx, ExternalServicesListOptions{
    /// Kinds, AfterID, LimitOffset{Limit}})`: live services of the given kinds,
    /// newest first, excluding ids greater than or equal to `after_id` (the
    /// baseline keeps the previous page's last id as the next `AfterID`).
    /// `providers.ProvidersFromConfig` walks the whole list 500 rows at a time.
    pub async fn list_by_kinds_after(
        &self,
        kinds: &[&str],
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<ExternalService>, ExternalServiceError> {
        let kinds: Vec<String> = kinds.iter().map(|kind| (*kind).to_owned()).collect();
        let rows = sqlx::query(
            r#"
SELECT id, kind, display_name, config, encryption_key_id,
       last_sync_at, next_sync_at, unrestricted, has_webhooks, code_host_id
FROM external_services
WHERE deleted_at IS NULL AND kind = ANY($1) AND ($2 = 0 OR id < $2)
ORDER BY id DESC
LIMIT $3
"#,
        )
        .bind(&kinds)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(service_from_row).collect()
    }

    /// `ExternalServices().Upsert` reduced to what
    /// `codeintel-package-filter-applicator` changes: `next_sync_at` (and the
    /// code host the baseline's `ensureCodeHost` step would attach).
    ///
    /// The baseline calls `Upsert` with the services it just listed, i.e.
    /// every other column is written back unchanged. Rust therefore updates
    /// only the columns the job actually changes: `next_sync_at`,
    /// `code_host_id`, and the two values `recalculateFields` recomputes
    /// (`unrestricted`, `has_webhooks`). See `docs/modules/06_worker.md`
    /// §32.4 for the documented deviations (no config re-validation or
    /// re-encryption, `updated_at` is left untouched exactly like the
    /// baseline's `excluded.updated_at`).
    ///
    /// `unrestricted`/`has_webhooks` are recomputed from the stored config
    /// only when it is plaintext (`encryption_key_id` empty, so
    /// [`recalculate_unrestricted`] can read it); for encrypted configs the
    /// baseline would decrypt it, but the values were already computed when
    /// the service was created, so the existing values are kept.
    ///
    /// Only the six package repository kinds are supported: their
    /// `UniqueCodeHostIdentifier` is the kind string itself.
    pub async fn mark_next_sync_at(
        &self,
        services: &[ExternalService],
        next_sync_at: OffsetDateTime,
    ) -> Result<(), ExternalServiceError> {
        if services.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        for service in services {
            let Some(url) = package_code_host_url(&service.kind) else {
                return Err(ExternalServiceError::InvalidKind {
                    id: service.id,
                    kind: service.kind.clone(),
                });
            };

            // `ensureCodeHost` → `CodeHostStore.Create`: insert-or-get by url.
            sqlx::query(
                r#"
INSERT INTO code_hosts (kind, url) VALUES ($1, $2)
ON CONFLICT (url) DO NOTHING
"#,
            )
            .bind(&service.kind)
            .bind(url)
            .execute(&mut *tx)
            .await?;
            let code_host_id: i32 = sqlx::query_scalar("SELECT id FROM code_hosts WHERE url = $1")
                .bind(url)
                .fetch_one(&mut *tx)
                .await?;

            // `recalculateFields` for a plaintext config; `None` keeps the
            // stored value (encrypted config, decrypted by the baseline).
            let plaintext = service.encryption_key_id.is_empty();
            let unrestricted = plaintext.then(|| recalculate_unrestricted(&service.config));
            // `configurationHasWebhooks` is `false` for every package kind.
            let has_webhooks = plaintext.then_some(false);

            // `upsertExternalServicesQuery`: next_sync_at/code_host_id change,
            // everything else is written back with the loaded value.
            sqlx::query(
                r#"
UPDATE external_services
SET next_sync_at = $2,
    code_host_id = $3,
    unrestricted = COALESCE($4, unrestricted),
    has_webhooks = COALESCE($5, has_webhooks)
WHERE id = $1 AND deleted_at IS NULL
"#,
            )
            .bind(service.id)
            .bind(next_sync_at)
            .bind(code_host_id)
            .bind(unrestricted)
            .bind(has_webhooks)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// `ExternalServices().Upsert` reduced to the only column the dependency
    /// sync scheduler changes: `next_sync_at`. The baseline passes the row it
    /// just listed, so every other column is written back unchanged
    /// (`updated_at = excluded.updated_at` leaves `updated_at` untouched);
    /// the worker job does not re-validate, re-encrypt or re-attach a code
    /// host. A missing row is an error: the baseline's upsert would take its
    /// INSERT branch and fail on the NOT NULL columns.
    ///
    /// See `docs/modules/06_worker.md` §130 for the documented deviations.
    pub async fn set_next_sync_at(
        &self,
        id: i64,
        next_sync_at: OffsetDateTime,
    ) -> Result<(), ExternalServiceError> {
        let result = sqlx::query(
            r#"
UPDATE external_services
SET next_sync_at = $2
WHERE id = $1
"#,
        )
        .bind(id)
        .bind(next_sync_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(ExternalServiceError::NotFound { id });
        }
        Ok(())
    }
}

/// `calcUnrestricted` with `permissionsUserMapping` disabled (the worker's
/// default site configuration): `false` when the config sets
/// `enforcePermissions`, otherwise `true` unless the config has an
/// `authorization` block.
fn recalculate_unrestricted(config: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(config) else {
        return true;
    };
    if let Some(enforce_permissions) = value.get("enforcePermissions") {
        return !enforce_permissions.as_bool().unwrap_or(false);
    }
    value.get("authorization").is_none()
}

/// `uniqueCodeHostIdentifier` for the package repository kinds: the kind
/// string (`GOMODULES`, `JVMPACKAGES`, ...), mirroring `Variant.AsKind()`.
pub fn package_code_host_url(kind: &str) -> Option<&'static str> {
    match kind {
        "GOMODULES" => Some("GOMODULES"),
        "JVMPACKAGES" => Some("JVMPACKAGES"),
        "NPMPACKAGES" => Some("NPMPACKAGES"),
        "PYTHONPACKAGES" => Some("PYTHONPACKAGES"),
        "RUBYPACKAGES" => Some("RUBYPACKAGES"),
        "RUSTPACKAGES" => Some("RUSTPACKAGES"),
        _ => None,
    }
}

fn service_from_row(row: sqlx::postgres::PgRow) -> Result<ExternalService, ExternalServiceError> {
    Ok(ExternalService {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        display_name: row.try_get("display_name")?,
        config: row.try_get("config")?,
        encryption_key_id: row.try_get("encryption_key_id")?,
        last_sync_at: row.try_get("last_sync_at")?,
        next_sync_at: row.try_get("next_sync_at")?,
        unrestricted: row.try_get("unrestricted")?,
        has_webhooks: row.try_get("has_webhooks")?,
        code_host_id: row.try_get("code_host_id")?,
    })
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
                last_sync_at: None,
                next_sync_at: None,
                unrestricted: false,
                has_webhooks: None,
                code_host_id: None,
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
