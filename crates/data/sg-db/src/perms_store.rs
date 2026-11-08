#![forbid(unsafe_code)]

//! `internal/database/perms_store.go` write path used by permissions syncing
//! (`permsSyncer.syncRepoPerms` / `syncUserPerms`) and by explicit-permissions
//! jobs.
//!
//! Ported statements:
//!
//! * `setUserRepoPermissions` (shared by `SetRepoPerms` and
//!   `SetUserExternalAccountPerms`), including the chunked upsert and the
//!   `deleteOldUserRepoPermissions` replacement rules (`source = 'api'`
//!   updates only replace API-sourced rows);
//! * `SetRepoPermissionsUnrestricted` (legacy `repo_permissions` upsert plus
//!   the `user_repo_permissions` unrestricted marker rows);
//! * `GetUserIDsByExternalAccounts`;
//! * `permissionSyncJobStore.SaveSyncResult` and
//!   `GetLatestFinishedSyncJob` (used to record job statistics and to compute
//!   consecutive-sync delays).
//!
//! Pending-permission tables (`SetRepoPendingPermissions`) are not ported
//! yet; they land with the repo-perms-syncer job in a later slice.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `authz.SourceRepoSync`.
pub const SOURCE_REPO_SYNC: &str = "repo_sync";
/// `authz.SourceUserSync`.
pub const SOURCE_USER_SYNC: &str = "user_sync";
/// `authz.SourceAPI`.
pub const SOURCE_API: &str = "api";

/// `authz.UserIDWithExternalAccountID`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserIdWithExternalAccountId {
    pub user_id: i32,
    pub external_account_id: i32,
}

/// `authz.PermissionEntity`: the side of the permission pair being replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionEntity {
    pub user_id: i32,
    pub external_account_id: i32,
    pub repo_id: i32,
}

/// `database.SetPermissionsResult`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SetPermissionsResult {
    pub added: usize,
    pub removed: usize,
    pub found: usize,
}

/// `database.PermissionSyncCodeHostState`, serialized into the `json[]`
/// column `permission_sync_jobs.code_host_states`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeHostState {
    pub provider_id: String,
    pub provider_type: String,
    pub status: String,
    pub message: String,
}

/// `database.CodeHostStatusSuccess`.
pub const CODE_HOST_STATUS_SUCCESS: &str = "SUCCESS";
/// `database.CodeHostStatusError`.
pub const CODE_HOST_STATUS_ERROR: &str = "ERROR";

impl CodeHostState {
    /// `database.NewProviderStatus`.
    pub fn new_provider_status(
        provider_id: impl Into<String>,
        provider_type: impl Into<String>,
        err: Option<&str>,
        action: &str,
    ) -> Self {
        match err {
            Some(err) => Self {
                provider_id: provider_id.into(),
                provider_type: provider_type.into(),
                status: CODE_HOST_STATUS_ERROR.to_owned(),
                message: format!("{action}: {err}"),
            },
            None => Self {
                provider_id: provider_id.into(),
                provider_type: provider_type.into(),
                status: CODE_HOST_STATUS_SUCCESS.to_owned(),
                message: action.to_owned(),
            },
        }
    }
}

/// `CodeHostStatusesSet.CountStatuses`: `(total, success, failed)`; every
/// non-success status counts as failed.
pub fn count_statuses(statuses: &[CodeHostState]) -> (usize, usize, usize) {
    let success = statuses
        .iter()
        .filter(|state| state.status == CODE_HOST_STATUS_SUCCESS)
        .count();
    (statuses.len(), success, statuses.len() - success)
}

/// A finished permission sync job, reduced to what
/// `GetLatestFinishedSyncJob` callers use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishedSyncJob {
    pub id: i32,
    pub finished_at: OffsetDateTime,
}

/// `authz.Permission` for the user-repo pair being written.
#[derive(Debug, Clone, Copy)]
struct Permission {
    user_id: i32,
    external_account_id: i32,
    repo_id: i32,
}

/// `PermsStore.SetRepoPerms`.
pub async fn set_repo_perms(
    pool: &PgPool,
    repo_id: i32,
    users: &[UserIdWithExternalAccountId],
    source: &str,
    now: OffsetDateTime,
) -> Result<SetPermissionsResult, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let result = set_repo_perms_tx(&mut tx, repo_id, users, source, now).await?;
    tx.commit().await?;
    Ok(result)
}

/// `SetRepoPerms` inside the caller's transaction (`permsSyncer.syncRepoPerms`
/// writes permissions and pending permissions atomically).
pub async fn set_repo_perms_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repo_id: i32,
    users: &[UserIdWithExternalAccountId],
    source: &str,
    now: OffsetDateTime,
) -> Result<SetPermissionsResult, sqlx::Error> {
    let permissions: Vec<Permission> = users
        .iter()
        .map(|user| Permission {
            user_id: user.user_id,
            external_account_id: user.external_account_id,
            repo_id,
        })
        .collect();
    let entity = PermissionEntity {
        user_id: 0,
        external_account_id: 0,
        repo_id,
    };
    set_user_repo_permissions_tx(tx, &permissions, entity, source, true, now).await
}

/// `PermsStore.SetUserExternalAccountPerms` (the replacing variant; the
/// non-replacing `setUserExternalAccountPerms(..., false)` is only used by
/// `GrantPendingPermissions`).
pub async fn set_user_external_account_perms(
    pool: &PgPool,
    user: UserIdWithExternalAccountId,
    repo_ids: &[i32],
    source: &str,
    now: OffsetDateTime,
) -> Result<SetPermissionsResult, sqlx::Error> {
    set_user_external_account_perms_with_replace(pool, user, repo_ids, source, true, now).await
}

async fn set_user_external_account_perms_with_replace(
    pool: &PgPool,
    user: UserIdWithExternalAccountId,
    repo_ids: &[i32],
    source: &str,
    replace_perms: bool,
    now: OffsetDateTime,
) -> Result<SetPermissionsResult, sqlx::Error> {
    let permissions: Vec<Permission> = repo_ids
        .iter()
        .map(|repo_id| Permission {
            user_id: user.user_id,
            external_account_id: user.external_account_id,
            repo_id: *repo_id,
        })
        .collect();
    let entity = PermissionEntity {
        user_id: user.user_id,
        external_account_id: user.external_account_id,
        repo_id: 0,
    };
    set_user_repo_permissions(pool, &permissions, entity, source, replace_perms, now).await
}

/// `setUserRepoPermissions`: upsert the given pairs, then (optionally) delete
/// the rows of the same entity that were not touched by the upsert.
async fn set_user_repo_permissions(
    pool: &PgPool,
    permissions: &[Permission],
    entity: PermissionEntity,
    source: &str,
    replace_perms: bool,
    now: OffsetDateTime,
) -> Result<SetPermissionsResult, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let result =
        set_user_repo_permissions_tx(&mut tx, permissions, entity, source, replace_perms, now)
            .await?;
    tx.commit().await?;
    Ok(result)
}

async fn set_user_repo_permissions_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    permissions: &[Permission],
    entity: PermissionEntity,
    source: &str,
    replace_perms: bool,
    now: OffsetDateTime,
) -> Result<SetPermissionsResult, sqlx::Error> {
    let updates = if permissions.is_empty() {
        Vec::new()
    } else {
        upsert_user_repo_permissions(tx, permissions, now, source).await?
    };

    let deleted = if replace_perms {
        delete_old_user_repo_permissions(tx, entity, now, source).await?
    } else {
        Vec::new()
    };

    Ok(SetPermissionsResult {
        added: updates.iter().filter(|is_new| **is_new).count(),
        removed: deleted.len(),
        found: permissions.len(),
    })
}

/// `upsertUserRepoPermissions`: `ON CONFLICT (user_id,
/// user_external_account_id, repo_id) DO UPDATE` with
/// `(created_at = updated_at) AS is_new_row`. Baseline binds six parameters
/// per row and chunks at `65535/6`; the equivalent `unnest` form is used here
/// with the same chunk size to keep the parameter limits identical.
async fn upsert_user_repo_permissions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    permissions: &[Permission],
    now: OffsetDateTime,
    source: &str,
) -> Result<Vec<bool>, sqlx::Error> {
    const UPSERT: &str = r#"
INSERT INTO user_repo_permissions
	(user_id, user_external_account_id, repo_id, created_at, updated_at, source)
SELECT NULLIF(t.user_id, 0), NULLIF(t.external_account_id, 0), t.repo_id, $4, $4, $5
FROM unnest($1::integer[], $2::integer[], $3::integer[])
	AS t(user_id, external_account_id, repo_id)
ON CONFLICT (user_id, user_external_account_id, repo_id)
DO UPDATE SET
	updated_at = excluded.updated_at,
	source = excluded.source
RETURNING (created_at = updated_at) AS is_new_row
"#;

    let mut output = Vec::with_capacity(permissions.len());
    for chunk in permissions.chunks(65535 / 6) {
        let user_ids: Vec<i32> = chunk.iter().map(|p| p.user_id).collect();
        let external_ids: Vec<i32> = chunk.iter().map(|p| p.external_account_id).collect();
        let repo_ids: Vec<i32> = chunk.iter().map(|p| p.repo_id).collect();
        let rows = sqlx::query(UPSERT)
            .bind(user_ids)
            .bind(external_ids)
            .bind(repo_ids)
            .bind(now)
            .bind(source)
            .fetch_all(&mut **tx)
            .await?;
        for row in rows {
            output.push(row.try_get("is_new_row")?);
        }
    }
    Ok(output)
}

/// `deleteOldUserRepoPermissions`: remove rows of the given entity whose
/// `updated_at` was not touched by this sync. Updates from `api` only replace
/// API-sourced rows; other sources leave API rows alone.
async fn delete_old_user_repo_permissions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    entity: PermissionEntity,
    now: OffsetDateTime,
    source: &str,
) -> Result<Vec<i64>, sqlx::Error> {
    let (where_entity, source_position) = if entity.user_id > 0 {
        if entity.external_account_id > 0 {
            (
                "user_id = $2 AND user_external_account_id = $3".to_owned(),
                4,
            )
        } else {
            ("user_id = $2".to_owned(), 3)
        }
    } else if entity.repo_id > 0 {
        ("repo_id = $2".to_owned(), 3)
    } else {
        return Err(sqlx::Error::Protocol(
            "invalid entity for which to delete old permissions, need at least RepoID or UserID"
                .to_owned(),
        ));
    };
    let operator = if source == SOURCE_API { "=" } else { "!=" };
    let query = format!(
        "DELETE FROM user_repo_permissions WHERE {where_entity} AND updated_at != $1 AND source {operator} ${source_position} RETURNING id"
    );

    let mut q = sqlx::query(&query).bind(now);
    if entity.user_id > 0 {
        q = q.bind(entity.user_id);
        if entity.external_account_id > 0 {
            q = q.bind(entity.external_account_id);
        }
    } else {
        q = q.bind(entity.repo_id);
    }
    // The baseline always compares against `api`: other sources replace every
    // non-API row, while an API sync replaces only API rows.
    let rows = q.bind(SOURCE_API).fetch_all(&mut **tx).await?;
    rows.into_iter().map(|row| row.try_get("id")).collect()
}

/// `SetRepoPermissionsUnrestricted`: either mark the repositories as
/// unrestricted (legacy `repo_permissions` row + a `user_repo_permissions`
/// row with a NULL `user_id`) or remove those markers.
pub async fn set_repo_permissions_unrestricted(
    pool: &PgPool,
    ids: &[i32],
    unrestricted: bool,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await?;
    legacy_set_repo_permissions_unrestricted(&mut tx, ids, unrestricted).await?;
    if unrestricted {
        set_unrestricted_markers(&mut tx, ids, now).await?;
    } else {
        unset_unrestricted_markers(&mut tx, ids).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// `legacySetRepoPermissionsUnrestricted`. The timestamps come from `NOW()`
/// in the baseline SQL.
async fn legacy_set_repo_permissions_unrestricted(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ids: &[i32],
    unrestricted: bool,
) -> Result<(), sqlx::Error> {
    const UPSERT: &str = r#"
INSERT INTO repo_permissions
  (repo_id, permission, user_ids_ints, updated_at, synced_at, unrestricted)
SELECT unnest($1::int[]), 'read', '{}'::int[], NOW(), NOW(), $2
ON CONFLICT ON CONSTRAINT
  repo_permissions_perm_unique
DO UPDATE SET
   updated_at = NOW(),
   unrestricted = $2
"#;

    for chunk in ids.chunks(65535 / 2) {
        sqlx::query(UPSERT)
            .bind(chunk.to_vec())
            .bind(unrestricted)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// `setRepoPermissionsUnrestricted`: `(NULL, repo, now, now, 'api')` rows,
/// `ON CONFLICT DO NOTHING`.
async fn set_unrestricted_markers(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ids: &[i32],
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    const INSERT: &str = r#"
INSERT INTO user_repo_permissions (user_id, repo_id, created_at, updated_at, source)
SELECT NULL, t.repo_id, $2, $2, $3
FROM unnest($1::int[]) AS t(repo_id)
ON CONFLICT DO NOTHING
"#;
    for chunk in ids.chunks(65535 / 4) {
        sqlx::query(INSERT)
            .bind(chunk.to_vec())
            .bind(now)
            .bind(SOURCE_API)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// `unsetRepoPermissionsUnrestricted`.
async fn unset_unrestricted_markers(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ids: &[i32],
) -> Result<(), sqlx::Error> {
    const DELETE: &str =
        "DELETE FROM user_repo_permissions WHERE repo_id = ANY($1) AND user_id IS NULL";
    for chunk in ids.chunks(65535 - 1) {
        sqlx::query(DELETE)
            .bind(chunk.to_vec())
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// `PermsStore.GetUserIDsByExternalAccounts`: map of external account ID to
/// the internal user/account IDs. Deleted and expired accounts are excluded.
pub async fn get_user_ids_by_external_accounts(
    pool: &PgPool,
    service_type: &str,
    service_id: &str,
    account_ids: &[String],
) -> Result<HashMap<String, UserIdWithExternalAccountId>, sqlx::Error> {
    const QUERY: &str = r#"
SELECT id, user_id, account_id
FROM user_external_accounts
WHERE service_type = $1
AND service_id = $2
AND account_id = ANY($3)
AND deleted_at IS NULL
AND expired_at IS NULL
"#;
    let rows = sqlx::query(QUERY)
        .bind(service_type)
        .bind(service_id)
        .bind(account_ids.to_vec())
        .fetch_all(pool)
        .await?;
    let mut user_ids = HashMap::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let user_id: i32 = row.try_get("user_id")?;
        let account_id: String = row.try_get("account_id")?;
        user_ids.insert(
            account_id,
            UserIdWithExternalAccountId {
                user_id,
                external_account_id: id,
            },
        );
    }
    Ok(user_ids)
}

/// `permissionSyncJobStore.SaveSyncResult`; `is_partial_success` is only set
/// when the job finished successfully with both successful and failed code
/// host statuses.
pub async fn save_sync_result(
    pool: &PgPool,
    id: i32,
    finished_successfully: bool,
    result: Option<&SetPermissionsResult>,
    statuses: &[CodeHostState],
) -> Result<(), sqlx::Error> {
    let (added, removed, found) = result
        .map(|result| (result.added, result.removed, result.found))
        .unwrap_or_default();
    let (_, success, failed) = count_statuses(statuses);
    let partial_success = finished_successfully && success > 0 && failed > 0;

    let mut params = String::new();
    for index in 0..statuses.len() {
        if index > 0 {
            params.push(',');
        }
        params.push_str(&format!("${}::json", index + 4));
    }
    let code_host_states = if statuses.is_empty() {
        "'{}'::json[]".to_owned()
    } else {
        format!("ARRAY[{params}]")
    };

    let query = format!(
        r#"
UPDATE permission_sync_jobs
SET
	permissions_added = $1,
	permissions_removed = $2,
	permissions_found = $3,
	code_host_states = {code_host_states},
	is_partial_success = ${}
WHERE id = ${}
"#,
        statuses.len() + 4,
        statuses.len() + 5
    );

    let mut q = sqlx::query(&query)
        .bind(added as i32)
        .bind(removed as i32)
        .bind(found as i32);
    for status in statuses {
        let json = serde_json::to_string(status)
            .map_err(|err| sqlx::Error::Protocol(format!("serialize code host state: {err}")))?;
        q = q.bind(json);
    }
    q = q.bind(partial_success).bind(id);
    q.execute(pool).await?;
    Ok(())
}

/// `permissionSyncJobStore.GetLatestFinishedSyncJob`: newest finished,
/// non-canceled job for a user or repository. Returns `None` when every job
/// has a NULL `finished_at` (the baseline converts those to a zero time and
/// then returns nil).
pub async fn get_latest_finished_sync_job(
    pool: &PgPool,
    user_id: Option<i32>,
    repo_id: Option<i32>,
    not_canceled: bool,
) -> Result<Option<FinishedSyncJob>, sqlx::Error> {
    let mut conditions: Vec<String> = Vec::new();
    let mut position = 0;
    if user_id.is_some() {
        position += 1;
        conditions.push(format!("user_id = ${position}"));
    }
    if repo_id.is_some() {
        position += 1;
        conditions.push(format!("repository_id = ${position}"));
    }
    if not_canceled {
        conditions.push("cancel = false".to_owned());
    }
    conditions.push("finished_at IS NOT NULL".to_owned());
    let query = format!(
        "SELECT id, finished_at FROM permission_sync_jobs WHERE {} ORDER BY finished_at DESC LIMIT 1",
        conditions.join(" AND ")
    );

    let mut q = sqlx::query(&query);
    if let Some(user_id) = user_id {
        q = q.bind(user_id);
    }
    if let Some(repo_id) = repo_id {
        q = q.bind(repo_id);
    }
    let row = match q.fetch_optional(pool).await? {
        Some(row) => row,
        None => return Ok(None),
    };
    let id: i32 = row.try_get("id")?;
    let finished_at: OffsetDateTime = row.try_get("finished_at")?;
    Ok(Some(FinishedSyncJob { id, finished_at }))
}

/// `authz.PermRepos`: the object type of the repository/external-account
/// to user grant.
pub const PERM_OBJECT_REPOS: &str = "repos";

/// `authz.Read`.
pub const PERM_READ: &str = "read";

/// `authz.UserPendingPermissions`, reduced to the fields the store reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPendingPermissions {
    pub id: i64,
    pub object_ids: Vec<i32>,
    pub updated_at: OffsetDateTime,
}

/// `PermsStore.SetRepoPendingPermissions`: record the external accounts that
/// are pending access to a repository until their user is created. Stub rows
/// are reused when they exist; only missing accounts are upserted (the
/// baseline avoids growing the bigserial id sequence unnecessarily).
#[allow(clippy::too_many_arguments)]
pub async fn set_repo_pending_permissions(
    pool: &PgPool,
    repo_id: i32,
    perm: &str,
    service_type: &str,
    service_id: &str,
    account_ids: &[String],
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    set_repo_pending_permissions_tx(
        &mut tx,
        repo_id,
        perm,
        service_type,
        service_id,
        account_ids,
        now,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// `SetRepoPendingPermissions` inside the caller's transaction.
#[allow(clippy::too_many_arguments)]
pub async fn set_repo_pending_permissions_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repo_id: i32,
    perm: &str,
    service_type: &str,
    service_id: &str,
    account_ids: &[String],
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let mut existing: HashMap<String, i64> = HashMap::new();
    if !account_ids.is_empty() {
        let rows = sqlx::query(
            r#"
SELECT bind_id, id FROM user_pending_permissions
WHERE service_type = $1
AND service_id = $2
AND permission = $3
AND object_type = $4
AND bind_id = ANY($5)
"#,
        )
        .bind(service_type)
        .bind(service_id)
        .bind(perm)
        .bind(PERM_OBJECT_REPOS)
        .bind(account_ids.to_vec())
        .fetch_all(&mut **tx)
        .await?;
        for row in rows {
            existing.insert(row.try_get("bind_id")?, row.try_get("id")?);
        }
    }

    let missing: Vec<String> = account_ids
        .iter()
        .filter(|bind_id| !existing.contains_key(*bind_id))
        .cloned()
        .collect();

    let mut pending_ids: std::collections::HashSet<i64> = existing.values().copied().collect();
    if !missing.is_empty() {
        let rows = sqlx::query(
            r#"
INSERT INTO user_pending_permissions
	(service_type, service_id, bind_id, permission, object_type, updated_at)
SELECT $1::TEXT, $2::TEXT, UNNEST($3::TEXT[]), $4::TEXT, $5::TEXT, $6::TIMESTAMPTZ
ON CONFLICT ON CONSTRAINT
	user_pending_permissions_service_perm_object_unique
DO UPDATE SET
	updated_at = excluded.updated_at
RETURNING id
"#,
        )
        .bind(service_type)
        .bind(service_id)
        .bind(missing)
        .bind(perm)
        .bind(PERM_OBJECT_REPOS)
        .bind(now)
        .fetch_all(&mut **tx)
        .await?;
        for row in rows {
            pending_ids.insert(row.try_get::<i64, _>("id")?);
        }
    }

    // `FOR UPDATE` matches the baseline lock.
    let old_ids = load_repo_pending_permission_ids(tx, repo_id, perm).await?;
    let old_ids: std::collections::HashSet<i64> = old_ids.into_iter().collect();

    let added: Vec<i64> = pending_ids.difference(&old_ids).copied().collect();
    let removed: Vec<i64> = old_ids.difference(&pending_ids).copied().collect();
    if added.is_empty() && removed.is_empty() {
        return Ok(());
    }

    let mut all_ids = added.clone();
    all_ids.extend(removed.iter().copied());
    sqlx::query(
        r#"
UPDATE user_pending_permissions
SET
	object_ids_ints = CASE
		WHEN user_pending_permissions.id = ANY ($1) THEN
			user_pending_permissions.object_ids_ints | $2
		ELSE
			user_pending_permissions.object_ids_ints - $3
		END,
	updated_at = $4
WHERE
	id = ANY ($5)
AND permission = $6
AND object_type = $7
"#,
    )
    .bind(added)
    .bind(vec![repo_id])
    .bind(vec![repo_id])
    .bind(now)
    .bind(all_ids)
    .bind(perm)
    .bind(PERM_OBJECT_REPOS)
    .execute(&mut **tx)
    .await?;

    let mut pending_ids: Vec<i64> = pending_ids.into_iter().collect();
    pending_ids.sort_unstable();
    sqlx::query(
        r#"
INSERT INTO repo_pending_permissions
  (repo_id, permission, user_ids_ints, updated_at)
VALUES
  ($1, $2, $3::bigint[], $4)
ON CONFLICT ON CONSTRAINT
  repo_pending_permissions_perm_unique
DO UPDATE SET
  user_ids_ints = excluded.user_ids_ints,
  updated_at = excluded.updated_at
"#,
    )
    .bind(repo_id)
    .bind(perm)
    .bind(pending_ids)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `loadRepoPendingPermissions` (`FOR UPDATE`): currently stored pending user
/// IDs of a repository; absent rows mean an empty set.
async fn load_repo_pending_permission_ids(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repo_id: i32,
    perm: &str,
) -> Result<Vec<i64>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT user_ids_ints
FROM repo_pending_permissions
WHERE repo_id = $1
AND permission = $2
FOR UPDATE
"#,
    )
    .bind(repo_id)
    .bind(perm)
    .fetch_optional(&mut **tx)
    .await?;
    match row {
        Some(row) => row.try_get("user_ids_ints"),
        None => Ok(Vec::new()),
    }
}

/// `PermsStore.GrantPendingPermissions`: turn the pending repository IDs of an
/// external account into real user permissions. Returns `false` when the
/// account has no pending permissions (the baseline is a no-op).
pub async fn grant_pending_permissions(
    pool: &PgPool,
    user: UserIdWithExternalAccountId,
    service_type: &str,
    service_id: &str,
    bind_id: &str,
    now: OffsetDateTime,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let pending = sqlx::query(
        r#"
SELECT id, object_ids_ints, updated_at
FROM user_pending_permissions
WHERE service_type = $1
AND service_id = $2
AND permission = $3
AND object_type = $4
AND bind_id = $5
FOR UPDATE
"#,
    )
    .bind(service_type)
    .bind(service_id)
    .bind(PERM_READ)
    .bind(PERM_OBJECT_REPOS)
    .bind(bind_id)
    .fetch_optional(&mut *tx)
    .await?;
    let pending: UserPendingPermissions = match pending {
        Some(row) => UserPendingPermissions {
            id: row.try_get("id")?,
            object_ids: row.try_get("object_ids_ints")?,
            updated_at: row.try_get("updated_at")?,
        },
        None => {
            tx.commit().await?;
            return Ok(false);
        }
    };

    let mut repo_ids = pending.object_ids.clone();
    repo_ids.sort_unstable();
    repo_ids.dedup();

    // Non-replacing: granting adds to whatever the user already has.
    let permissions: Vec<Permission> = repo_ids
        .iter()
        .map(|repo_id| Permission {
            user_id: user.user_id,
            external_account_id: user.external_account_id,
            repo_id: *repo_id,
        })
        .collect();
    upsert_user_repo_permissions(&mut tx, &permissions, now, SOURCE_USER_SYNC).await?;

    if repo_ids.is_empty() {
        tx.commit().await?;
        return Ok(true);
    }

    // `upsertRepoPermissionsBatchQuery`: every repository in the page is an
    // "added" one, so the union with excluded user IDs is equivalent.
    sqlx::query(
        r#"
INSERT INTO repo_permissions
	(repo_id, permission, user_ids_ints, updated_at)
SELECT unnest($1::int[]), $2, $3::int[], $4
ON CONFLICT ON CONSTRAINT
	repo_permissions_perm_unique
DO UPDATE SET
	user_ids_ints = repo_permissions.user_ids_ints | excluded.user_ids_ints,
	updated_at = excluded.updated_at
"#,
    )
    .bind(repo_ids.clone())
    .bind(PERM_READ)
    .bind(vec![user.user_id])
    .bind(now)
    .execute(&mut *tx)
    .await?;

    // `upsertUserPermissionsBatchQuery` with only "added" rows.
    sqlx::query(
        r#"
INSERT INTO user_permissions
	(user_id, permission, object_type, object_ids_ints, updated_at)
SELECT unnest($1::int[]), $2, $3, $4::int[], $5
ON CONFLICT ON CONSTRAINT
	user_permissions_perm_object_unique
DO UPDATE SET
	object_ids_ints = user_permissions.object_ids_ints | excluded.object_ids_ints,
	updated_at = excluded.updated_at
"#,
    )
    .bind(vec![user.user_id])
    .bind(PERM_READ)
    .bind(PERM_OBJECT_REPOS)
    .bind(repo_ids)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    // The pending row is deleted, which invalidates `repo_pending_permissions`
    // entries for this user (the baseline relies on this to avoid deadlocks).
    sqlx::query(
        r#"
DELETE FROM user_pending_permissions
WHERE service_type = $1
AND service_id = $2
AND permission = $3
AND object_type = $4
AND bind_id = $5
"#,
    )
    .bind(service_type)
    .bind(service_id)
    .bind(PERM_READ)
    .bind(PERM_OBJECT_REPOS)
    .bind(bind_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

/// `PermsStore.ListPendingUsers`: bind IDs with non-empty pending repository
/// IDs. Stub rows with no pending permissions are skipped.
pub async fn list_pending_users(
    pool: &PgPool,
    service_type: &str,
    service_id: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
SELECT bind_id, object_ids_ints
FROM user_pending_permissions
WHERE service_type = $1
AND service_id = $2
ORDER BY bind_id
"#,
    )
    .bind(service_type)
    .bind(service_id)
    .fetch_all(pool)
    .await?;
    let mut bind_ids = Vec::new();
    for row in rows {
        let ids: Vec<i32> = row.try_get("object_ids_ints")?;
        if ids.is_empty() {
            continue;
        }
        bind_ids.push(row.try_get("bind_id")?);
    }
    Ok(bind_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_host_states_serialize_like_the_baseline() {
        let success = CodeHostState::new_provider_status(
            "https://github.com/",
            "github",
            None,
            "FetchRepoPerms",
        );
        assert_eq!(
            serde_json::to_string(&success).expect("serialize"),
            r#"{"provider_id":"https://github.com/","provider_type":"github","status":"SUCCESS","message":"FetchRepoPerms"}"#
        );
        let error =
            CodeHostState::new_provider_status("id", "gitlab", Some("boom"), "FetchUserPerms");
        assert_eq!(error.status, CODE_HOST_STATUS_ERROR);
        assert_eq!(error.message, "FetchUserPerms: boom");
    }

    #[test]
    fn count_statuses_treats_every_non_success_as_failed() {
        let statuses = vec![
            CodeHostState::new_provider_status("a", "github", None, "Fetch"),
            CodeHostState::new_provider_status("b", "gitlab", Some("err"), "Fetch"),
            CodeHostState::new_provider_status("c", "gerrit", None, "Fetch"),
        ];
        assert_eq!(count_statuses(&statuses), (3, 2, 1));
        assert_eq!(count_statuses(&[]), (0, 0, 0));
    }
}
