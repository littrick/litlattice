#![forbid(unsafe_code)]

//! `internal/database/sub_repo_perms_store.go`: the sub-repository permission
//! store.
//!
//! Ported statements:
//!
//! * `Upsert` / `UpsertWithIPs` (internal repo id) and
//!   `UpsertWithSpec` / `UpsertWithSpecWithIPs` (external repo spec ->
//!   internal repo id), all with `version = 1` and
//!   `ON CONFLICT (user_id, repo_id, version)`;
//! * `Get` / `GetWithIPs` for one user + repo;
//! * `GetByUser` / `GetByUserWithIPs` (keyed by repository name, with the
//!   site-admin filter from `authz.enforceForSiteAdmins`);
//! * `GetByUserAndService` / `GetByUserAndServiceWithIPs` (used to carry
//!   previously synced sub-repository permissions through a temporary fetch
//!   error);
//! * `RepoIDSupported` / `RepoSupported` (private Perforce repositories);
//! * `DeleteByUser`.
//!
//! `With`/`Transact`/`Done` are not needed yet; each function takes the pool
//! (or, later, a transaction) directly. `GetByUser*` take
//! `enforce_for_site_admins` as an argument instead of reading the live site
//! configuration.

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

/// Failure modes of the IP-aware reads.
#[derive(Debug)]
pub enum SubRepoPermsError {
    /// `database.IPsNotSyncedError` wrapped with the row context, exactly like
    /// `errors.Wrapf` (`"<context>: <message>"`).
    IpsNotSynced(String),
    /// A row has paths but a different number of IPs.
    ConstraintViolation(String),
    Database(sqlx::Error),
}

impl std::fmt::Display for SubRepoPermsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IpsNotSynced(context) => write!(f, "{context}: {IPS_NOT_SYNCED_MESSAGE}"),
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
                return Err(SubRepoPermsError::IpsNotSynced(format!(
                    "user_id={user_id}, external_id={external_id}"
                )));
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

/// `subRepoPermsStore.Upsert`: insert/update by internal repo id; `ips` is set
/// to NULL.
pub async fn upsert(
    pool: &PgPool,
    user_id: i32,
    repo_id: i32,
    perms: &SubRepoPermissions,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
INSERT INTO sub_repo_permissions (user_id, repo_id, paths, version, updated_at, ips)
VALUES ($1, $2, $3, $4, now(), NULL)
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
    .bind(repo_id)
    .bind(perms.paths.clone())
    .bind(SUB_REPO_PERMS_VERSION)
    .execute(pool)
    .await
    .map(|_| ())
}

/// `subRepoPermsStore.UpsertWithIPs`: like [`upsert`], but stores one IP per
/// path.
pub async fn upsert_with_ips(
    pool: &PgPool,
    user_id: i32,
    repo_id: i32,
    perms: &SubRepoPermissionsWithIps,
) -> Result<(), sqlx::Error> {
    let paths: Vec<String> = perms.paths.iter().map(|p| p.path.clone()).collect();
    let ips: Vec<String> = perms.paths.iter().map(|p| p.ip.clone()).collect();
    sqlx::query(
        r#"
INSERT INTO sub_repo_permissions (user_id, repo_id, paths, version, updated_at, ips)
VALUES ($1, $2, $3, $4, now(), $5)
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
    .bind(repo_id)
    .bind(paths)
    .bind(SUB_REPO_PERMS_VERSION)
    .bind(ips)
    .execute(pool)
    .await
    .map(|_| ())
}

/// `subRepoPermsStore.Get`: paths for one user + repo. Returns empty
/// permissions when there is no row.
///
/// Erratum (baseline `c864f15af2`, `sub_repo_perms_store.go`): the Go query
/// reads `WHERE repo_id = %s AND user_id = %s` but binds `userID, repoID`, so
/// the two arguments are swapped on the wire. We reproduce that exactly and
/// the error context strings keep using the caller-supplied names.
pub async fn get(
    pool: &PgPool,
    user_id: i32,
    repo_id: i32,
) -> Result<SubRepoPermissions, SubRepoPermsError> {
    let rows = sqlx::query(
        r#"
SELECT paths
FROM sub_repo_permissions
WHERE repo_id = $1
  AND user_id = $2
  AND version = $3
"#,
    )
    // Baseline binds `userID` to `repo_id` and `repoID` to `user_id` (see the
    // erratum on this function).
    .bind(user_id)
    .bind(repo_id)
    .bind(SUB_REPO_PERMS_VERSION)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        SubRepoPermsError::Database(context_error("getting sub repo permissions", err))
    })?;

    let mut perms = SubRepoPermissions::default();
    for row in rows {
        let paths: Option<Vec<String>> = row
            .try_get("paths")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        perms.paths.extend(paths.unwrap_or_default());
    }
    Ok(perms)
}

/// `subRepoPermsStore.GetWithIPs`: like [`get`], but with one IP per path.
/// Inherits the baseline argument swap documented on [`get`].
/// Rows whose `ips` is empty return [`SubRepoPermsError::IpsNotSynced`]
/// unless `backfill_with_wildcard_ip` is set.
pub async fn get_with_ips(
    pool: &PgPool,
    user_id: i32,
    repo_id: i32,
    backfill_with_wildcard_ip: bool,
) -> Result<SubRepoPermissionsWithIps, SubRepoPermsError> {
    let rows = sqlx::query(
        r#"
SELECT paths, ips
FROM sub_repo_permissions
WHERE repo_id = $1
  AND user_id = $2
  AND version = $3
"#,
    )
    // Same upstream argument swap as [`get`].
    .bind(user_id)
    .bind(repo_id)
    .bind(SUB_REPO_PERMS_VERSION)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        SubRepoPermsError::Database(context_error("getting sub repo permissions", err))
    })?;

    let mut perms = SubRepoPermissionsWithIps::default();
    for row in rows {
        let paths: Option<Vec<String>> = row
            .try_get("paths")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        let ips: Option<Vec<String>> = row
            .try_get("ips")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        let paths = paths.unwrap_or_default();
        let mut ips = ips.unwrap_or_default();

        if paths.len() != ips.len() {
            if !ips.is_empty() {
                return Err(SubRepoPermsError::ConstraintViolation(format!(
                    "database constraint violation for repo_id={repo_id}, \
                     user_id={user_id}, version={SUB_REPO_PERMS_VERSION}: len(paths)!= len(ips)"
                )));
            }
            if !backfill_with_wildcard_ip {
                return Err(SubRepoPermsError::IpsNotSynced(format!(
                    "repo_id={repo_id}, user_id={user_id}, version={SUB_REPO_PERMS_VERSION}"
                )));
            }
            ips = wildcard_ips(paths.len());
        }

        perms.paths.extend(
            paths
                .into_iter()
                .zip(ips)
                .map(|(path, ip)| PathWithIp { path, ip }),
        );
    }
    Ok(perms)
}

/// `subRepoPermsStore.GetByUser`: all rows for a user keyed by repository
/// name. Site admins are filtered out unless
/// `enforce_for_site_admins` (`authz.enforceForSiteAdmins`) is set.
pub async fn get_by_user(
    pool: &PgPool,
    user_id: i32,
    enforce_for_site_admins: bool,
) -> Result<BTreeMap<String, SubRepoPermissions>, SubRepoPermsError> {
    let rows = sqlx::query(
        r#"
SELECT r.name, paths
FROM sub_repo_permissions
JOIN repo r on r.id = repo_id
JOIN users u on u.id = user_id
WHERE user_id = $1
AND version = $2
-- When user is a site admin and AuthzEnforceForSiteAdmins is FALSE
-- we want to return zero results. This causes us to fall back to
-- repo level checks and allows access to all paths in all repos.
AND NOT (u.site_admin AND NOT $3)
"#,
    )
    .bind(user_id)
    .bind(SUB_REPO_PERMS_VERSION)
    .bind(enforce_for_site_admins)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        SubRepoPermsError::Database(context_error("getting sub repo permissions by user", err))
    })?;

    let mut result = BTreeMap::new();
    for row in rows {
        let repo_name: String = row
            .try_get("name")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        let paths: Option<Vec<String>> = row
            .try_get("paths")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        result.insert(
            repo_name,
            SubRepoPermissions {
                paths: paths.unwrap_or_default(),
            },
        );
    }
    Ok(result)
}

/// `subRepoPermsStore.GetByUserWithIPs`: like [`get_by_user`], but with IPs
/// and the same backfill/constraint rules as [`get_with_ips`].
pub async fn get_by_user_with_ips(
    pool: &PgPool,
    user_id: i32,
    enforce_for_site_admins: bool,
    backfill_with_wildcard_ip: bool,
) -> Result<BTreeMap<String, SubRepoPermissionsWithIps>, SubRepoPermsError> {
    let rows = sqlx::query(
        r#"
SELECT r.name, paths, ips
FROM sub_repo_permissions
JOIN repo r on r.id = repo_id
JOIN users u on u.id = user_id
WHERE user_id = $1
AND version = $2
-- When user is a site admin and AuthzEnforceForSiteAdmins is FALSE
-- we want to return zero results. This causes us to fall back to
-- repo level checks and allows access to all paths in all repos.
AND NOT (u.site_admin AND NOT $3)
"#,
    )
    .bind(user_id)
    .bind(SUB_REPO_PERMS_VERSION)
    .bind(enforce_for_site_admins)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        SubRepoPermsError::Database(context_error(
            "getting sub repo permissions by user with IPs",
            err,
        ))
    })?;

    let mut result = BTreeMap::new();
    for row in rows {
        let repo_name: String = row
            .try_get("name")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        let paths: Option<Vec<String>> = row
            .try_get("paths")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        let ips: Option<Vec<String>> = row
            .try_get("ips")
            .map_err(|err| SubRepoPermsError::Database(context_error("scanning row", err)))?;
        let paths = paths.unwrap_or_default();
        let mut ips = ips.unwrap_or_default();

        if paths.len() != ips.len() {
            if !ips.is_empty() {
                return Err(SubRepoPermsError::ConstraintViolation(format!(
                    "database constraint violation for user_id={user_id}, \
                     repo={repo_name}: len(paths) != len(ips)"
                )));
            }
            if !backfill_with_wildcard_ip {
                return Err(SubRepoPermsError::IpsNotSynced(format!(
                    "user_id={user_id}, repo={repo_name}"
                )));
            }
            ips = wildcard_ips(paths.len());
        }

        result.insert(
            repo_name,
            SubRepoPermissionsWithIps {
                paths: paths
                    .into_iter()
                    .zip(ips)
                    .map(|(path, ip)| PathWithIp { path, ip })
                    .collect(),
            },
        );
    }
    Ok(result)
}

/// `subRepoPermsStore.RepoIDSupported`: the repository is private and its
/// external service type is Perforce.
pub async fn repo_id_supported(pool: &PgPool, repo_id: i32) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        r#"
SELECT EXISTS(
SELECT
FROM repo
WHERE id = $1
AND private = TRUE
AND external_service_type = $2
)
"#,
    )
    .bind(repo_id)
    .bind(PERFORCE_EXTERNAL_SERVICE_TYPE)
    .fetch_one(pool)
    .await
}

/// `subRepoPermsStore.RepoSupported`: like [`repo_id_supported`], keyed by
/// repository name.
pub async fn repo_supported(pool: &PgPool, repo_name: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        r#"
SELECT EXISTS(
SELECT
FROM repo
WHERE name = $1
AND private = TRUE
AND external_service_type = $2
)
"#,
    )
    .bind(repo_name)
    .bind(PERFORCE_EXTERNAL_SERVICE_TYPE)
    .fetch_one(pool)
    .await
}

/// `subRepoPermsStore.DeleteByUser`.
pub async fn delete_by_user(pool: &PgPool, user_id: i32) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM sub_repo_permissions WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .map(|_| ())
}

/// `extsvc.TypePerforce`.
const PERFORCE_EXTERNAL_SERVICE_TYPE: &str = "perforce";

fn wildcard_ips(count: usize) -> Vec<String> {
    vec!["*".to_owned(); count]
}

fn context_error(context: &str, err: sqlx::Error) -> sqlx::Error {
    sqlx::Error::Protocol(format!("{context}: {err}"))
}
