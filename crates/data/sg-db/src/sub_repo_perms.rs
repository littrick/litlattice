#![forbid(unsafe_code)]

//! `internal/database/sub_repo_perms_store.go`: the sub-repository permission
//! statements the permission syncer uses.
//!
//! Ported statements:
//!
//! * `UpsertWithSpec` / `UpsertWithSpecWithIPs` (external repo spec -> internal
//!   repo id, `version = 1`, `ON CONFLICT (user_id, repo_id, version)`);
//! * `GetByUserAndService` / `GetByUserAndServiceWithIPs` (used to carry
//!   previously synced sub-repository permissions through a temporary fetch
//!   error).
//!
//! The remaining store methods (`Get`, `GetWithIPs`, `GetByUser*`,
//! `RepoIDSupported`, `DeleteByUser`) are used outside the syncer and are not
//! ported yet.

use std::collections::BTreeMap;

use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::repos::ExternalRepoSpec;

/// `database.SubRepoPermsVersion`.
pub const SUB_REPO_PERMS_VERSION: i32 = 1;

/// The `IPsNotSyncedError` message: the row predates IP-aware syncing.
pub const IPS_NOT_SYNCED_MESSAGE: &str =
    "the IP addresses associated with this sub-repository-permissions entry have not been synced yet";

/// `authz.SubRepoPermissions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubRepoPermissions {
    pub paths: Vec<String>,
}

/// `authz.PathWithIP`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathWithIp {
    pub path: String,
    pub ip: String,
}

/// `authz.SubRepoPermissionsWithIPs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubRepoPermissionsWithIps {
    pub paths: Vec<PathWithIp>,
}

/// Failure modes of [`get_by_user_and_service_with_ips`].
#[derive(Debug)]
pub enum SubRepoPermsError {
    /// `database.IPsNotSyncedError` (without the path context).
    IpsNotSynced,
    /// A row has paths but a different number of IPs.
    ConstraintViolation(String),
    Database(sqlx::Error),
}

impl std::fmt::Display for SubRepoPermsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IpsNotSynced => write!(f, "{IPS_NOT_SYNCED_MESSAGE}"),
            Self::ConstraintViolation(message) => write!(f, "{message}"),
            Self::Database(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SubRepoPermsError {}

impl From<sqlx::Error> for SubRepoPermsError {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err)
    }
}

/// `subRepoPermsStore.UpsertWithSpec`: insert/update by external repo spec;
/// no row when the spec does not map to a known repository. `ips` is set to
/// NULL.
pub async fn upsert_with_spec(
    pool: &PgPool,
    user_id: i32,
    spec: &ExternalRepoSpec,
    perms: &SubRepoPermissions,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
INSERT INTO sub_repo_permissions (user_id, repo_id, paths, version, updated_at)
SELECT $1, id, $2, $3, now()
FROM repo
WHERE external_service_id = $4
  AND external_service_type = $5
  AND external_id = $6
ON CONFLICT (user_id, repo_id, version)
DO UPDATE
SET
  user_id = EXCLUDED.user_id,
  repo_id = EXCLUDED.repo_id,
  paths = EXCLUDED.paths,
  version = EXCLUDED.version,
  updated_at = now(),
  ips = NULL
"#,
    )
    .bind(user_id)
    .bind(perms.paths.clone())
    .bind(SUB_REPO_PERMS_VERSION)
    .bind(&spec.service_id)
    .bind(&spec.service_type)
    .bind(&spec.id)
    .execute(pool)
    .await
    .map(|_| ())
}

/// `subRepoPermsStore.UpsertWithSpecWithIPs`: like [`upsert_with_spec`], but
/// stores one IP per path (`ips` is an array, empty when there are no paths).
pub async fn upsert_with_spec_with_ips(
    pool: &PgPool,
    user_id: i32,
    spec: &ExternalRepoSpec,
    perms: &SubRepoPermissionsWithIps,
) -> Result<(), sqlx::Error> {
    let paths: Vec<String> = perms.paths.iter().map(|p| p.path.clone()).collect();
    let ips: Vec<String> = perms.paths.iter().map(|p| p.ip.clone()).collect();
    sqlx::query(
        r#"
INSERT INTO sub_repo_permissions (user_id, repo_id, paths, version, updated_at, ips)
SELECT $1, id, $2, $3, now(), $4
FROM repo
WHERE external_service_id = $5
  AND external_service_type = $6
  AND external_id = $7
ON CONFLICT (user_id, repo_id, version)
DO UPDATE
SET
  user_id = EXCLUDED.user_id,
  repo_id = EXCLUDED.repo_id,
  paths = EXCLUDED.paths,
  version = EXCLUDED.version,
  updated_at = now(),
  ips = EXCLUDED.ips
"#,
    )
    .bind(user_id)
    .bind(paths)
    .bind(SUB_REPO_PERMS_VERSION)
    .bind(ips)
    .bind(&spec.service_id)
    .bind(&spec.service_type)
    .bind(&spec.id)
    .execute(pool)
    .await
    .map(|_| ())
}

/// `subRepoPermsStore.GetByUserAndService`: paths per external repo spec for
/// one user and external service.
pub async fn get_by_user_and_service(
    pool: &PgPool,
    user_id: i32,
    service_type: &str,
    service_id: &str,
) -> Result<BTreeMap<ExternalRepoSpec, SubRepoPermissions>, SubRepoPermsError> {
    let rows = sqlx::query(
        r#"
SELECT r.external_id, paths
FROM sub_repo_permissions
JOIN repo r on r.id = repo_id
WHERE user_id = $1
  AND version = $2
  AND r.external_service_type = $3
  AND r.external_service_id = $4
"#,
    )
    .bind(user_id)
    .bind(SUB_REPO_PERMS_VERSION)
    .bind(service_type)
    .bind(service_id)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        SubRepoPermsError::Database(context_error(
            "getting sub repo permissions by user and service",
            err,
        ))
    })?;

    let mut result = BTreeMap::new();
    for row in rows {
        let external_id: String = row.try_get("external_id")?;
        let paths: Option<Vec<String>> = row.try_get("paths")?;
        result.insert(
            ExternalRepoSpec {
                id: external_id,
                service_type: service_type.to_owned(),
                service_id: service_id.to_owned(),
            },
            SubRepoPermissions {
                paths: paths.unwrap_or_default(),
            },
        );
    }
    Ok(result)
}

/// `subRepoPermsStore.GetByUserAndServiceWithIPs`. Rows written before
/// IP-aware syncing have `ips IS NULL`; those return
/// [`SubRepoPermsError::IpsNotSynced`] unless `backfill_with_wildcard_ip` is
/// set, in which case each path gets `*`.
pub async fn get_by_user_and_service_with_ips(
    pool: &PgPool,
    user_id: i32,
    service_type: &str,
    service_id: &str,
    backfill_with_wildcard_ip: bool,
) -> Result<BTreeMap<ExternalRepoSpec, SubRepoPermissionsWithIps>, SubRepoPermsError> {
    let rows = sqlx::query(
        r#"
SELECT r.external_id, paths, ips
FROM sub_repo_permissions
JOIN repo r on r.id = repo_id
WHERE user_id = $1
  AND version = $2
  AND r.external_service_type = $3
  AND r.external_service_id = $4
"#,
    )
    .bind(user_id)
    .bind(SUB_REPO_PERMS_VERSION)
    .bind(service_type)
    .bind(service_id)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        SubRepoPermsError::Database(context_error(
            "getting sub repo permissions by user and service with IPs",
            err,
        ))
    })?;

    let mut result = BTreeMap::new();
    for row in rows {
        let external_id: String = row.try_get("external_id")?;
        let paths: Option<Vec<String>> = row.try_get("paths")?;
        let ips: Option<Vec<String>> = row.try_get("ips")?;
        let paths = paths.unwrap_or_default();
        let mut ips = ips.unwrap_or_default();

        if paths.len() != ips.len() {
            if !ips.is_empty() {
                return Err(SubRepoPermsError::ConstraintViolation(format!(
                    "database constraint violation for user_id={user_id}, \
                     external_id={external_id}: len(paths) != len(ips)"
                )));
            }
            if !backfill_with_wildcard_ip {
                return Err(SubRepoPermsError::IpsNotSynced);
            }
            ips = vec!["*".to_owned(); paths.len()];
        }

        let perms = paths
            .into_iter()
            .zip(ips)
            .map(|(path, ip)| PathWithIp { path, ip })
            .collect();
        result.insert(
            ExternalRepoSpec {
                id: external_id,
                service_type: service_type.to_owned(),
                service_id: service_id.to_owned(),
            },
            SubRepoPermissionsWithIps { paths: perms },
        );
    }
    Ok(result)
}

fn context_error(context: &str, err: sqlx::Error) -> sqlx::Error {
    sqlx::Error::Protocol(format!("{context}: {err}"))
}
