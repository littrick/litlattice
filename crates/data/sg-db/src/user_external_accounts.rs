//! `internal/database/external_accounts.go`: the `List` and `Delete` paths the
//! expired Sourcegraph Operator account cleaner uses.
//!
//! `List` returns every live account of a user ordered by id (`deleted_at IS
//! NULL ... ORDER BY id ASC`); `account_data` is returned as stored, with the
//! `encryption_key_id` the caller needs to decrypt it (`Unmigrated`/empty key
//! ids are plaintext). `Delete` with `HardDelete` removes the matching rows.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use crate::users::DbError;

/// One `user_external_accounts` row, reduced to the columns the cleaner
/// reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserExternalAccount {
    pub id: i32,
    pub user_id: i32,
    pub service_type: String,
    /// `account_data` (encrypted at rest). `None` means the column is NULL,
    /// which the baseline maps to a nil `extsvc.AccountData`.
    pub account_data: Option<String>,
    pub encryption_key_id: String,
}

/// `userExternalAccountsStore.Count(ctx, {UserID, ServiceType})`
/// (`internal/database/external_accounts.go:486`): the SOAP account count the
/// access-token middleware uses.
pub async fn count_for_user_service(
    pool: &PgPool,
    user_id: i32,
    service_type: &str,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "SELECT COUNT(*) FROM user_external_accounts \
         WHERE deleted_at IS NULL AND user_id = $1 AND service_type = $2",
    )
    .bind(user_id)
    .bind(service_type)
    .fetch_one(pool)
    .await?;
    Ok(row.get("count"))
}

/// `userExternalAccountsStore.List(ctx, {UserID})`.
pub async fn list_for_user(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<UserExternalAccount>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
SELECT t.id, t.user_id, t.service_type, t.account_data, t.encryption_key_id
FROM user_external_accounts t
WHERE deleted_at IS NULL AND user_id = $1
ORDER BY id ASC"#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| UserExternalAccount {
            id: row.get("id"),
            user_id: row.get("user_id"),
            service_type: row.get("service_type"),
            account_data: row.get("account_data"),
            encryption_key_id: row.get("encryption_key_id"),
        })
        .collect())
}

/// `userExternalAccountsStore.Delete` with `HardDelete: true`.
///
/// The baseline wraps the SQL error with `executing delete`; callers add
/// their own context.
///
/// Deliberate deviation: an empty `ids` list is a no-op. The baseline would
/// delete every live account matching `service_type` in that case (the bug
/// its own test suite guards against); the cleaner only calls this with the
/// ids it collected, so the difference is unreachable from the ported job.
pub async fn hard_delete(pool: &PgPool, ids: &[i32], service_type: &str) -> Result<(), DbError> {
    if ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "DELETE FROM user_external_accounts \
         WHERE deleted_at IS NULL AND id = ANY($1) AND service_type = $2",
    )
    .bind(ids.to_vec())
    .bind(service_type)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|err| DbError {
        message: format!("executing delete: {err}"),
        not_found: false,
    })
}

/// One full `user_external_accounts` row as `Database.UserExternalAccounts().List`
/// returns it (`extsvc.Account`).
///
/// `auth_data`/`account_data` are returned as stored (encrypted at rest);
/// `encryption_key_id` is the key version the caller needs to decrypt them.
/// `None` means the column is NULL, which the baseline maps to a nil
/// `EncryptableData` — distinct from an empty string, which is decrypted
/// (and then fails to parse as JSON) when the key ID is set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccount {
    pub id: i32,
    pub user_id: i32,
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub account_id: String,
    /// `auth_data` (encrypted at rest).
    pub auth_data: Option<String>,
    /// `account_data` (encrypted at rest).
    pub account_data: Option<String>,
    pub encryption_key_id: String,
    /// `expired_at IS NOT NULL` (the timestamp itself is not exposed).
    pub expired: bool,
}

/// `database.ExternalAccountsListOptions`, restricted to the fields the
/// permission syncer and the authz provider lookup use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalAccountsListOptions {
    pub user_id: Option<i32>,
    pub service_type: Option<String>,
    pub service_id: Option<String>,
    pub client_id: Option<String>,
    pub account_id: Option<String>,
    /// `expired_at IS NULL`.
    pub exclude_expired: bool,
    /// `expired_at IS NOT NULL`.
    pub only_expired: bool,
}

fn account_from_row(row: &sqlx::postgres::PgRow) -> Result<ExternalAccount, sqlx::Error> {
    Ok(ExternalAccount {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        service_type: row.try_get("service_type")?,
        service_id: row.try_get("service_id")?,
        client_id: row.try_get("client_id")?,
        account_id: row.try_get("account_id")?,
        auth_data: row.try_get("auth_data")?,
        account_data: row.try_get("account_data")?,
        encryption_key_id: row.try_get("encryption_key_id")?,
        expired: row.try_get("expired")?,
    })
}

/// The columns every `ExternalAccount` query selects, in `account_from_row`
/// order (`listBySQL`'s column list, plus the expiry predicate).
const ACCOUNT_COLUMNS: &str = "id, user_id, service_type, service_id, client_id, account_id, \
     auth_data, account_data, encryption_key_id, (expired_at IS NOT NULL) AS expired";

/// `userExternalAccountsStore.List`. Live accounts only, ordered by id.
pub async fn list(
    pool: &PgPool,
    opts: &ExternalAccountsListOptions,
) -> Result<Vec<ExternalAccount>, sqlx::Error> {
    let mut query = sqlx::QueryBuilder::new(format!(
        "SELECT {ACCOUNT_COLUMNS} FROM user_external_accounts WHERE deleted_at IS NULL"
    ));
    if let Some(user_id) = opts.user_id {
        query.push(" AND user_id = ").push_bind(user_id);
    }
    if let Some(service_type) = &opts.service_type {
        query
            .push(" AND service_type = ")
            .push_bind(service_type.clone());
    }
    if let Some(service_id) = &opts.service_id {
        query
            .push(" AND service_id = ")
            .push_bind(service_id.clone());
    }
    if let Some(client_id) = &opts.client_id {
        query.push(" AND client_id = ").push_bind(client_id.clone());
    }
    if let Some(account_id) = &opts.account_id {
        query
            .push(" AND account_id = ")
            .push_bind(account_id.clone());
    }
    if opts.exclude_expired {
        query.push(" AND expired_at IS NULL");
    }
    if opts.only_expired {
        query.push(" AND expired_at IS NOT NULL");
    }
    query.push(" ORDER BY id ASC");

    let rows = query.build().fetch_all(pool).await?;
    rows.iter().map(account_from_row).collect()
}

/// `userExternalAccountsStore.Get(id)`: one live account including the
/// encrypted data columns.
pub async fn get_by_id(pool: &PgPool, id: i32) -> Result<Option<ExternalAccount>, sqlx::Error> {
    let row = sqlx::query(&format!(
        "SELECT {ACCOUNT_COLUMNS} FROM user_external_accounts \
         WHERE id = $1 AND deleted_at IS NULL LIMIT 1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(account_from_row).transpose()
}

/// `userExternalAccountsStore.Update`: replace the account data columns,
/// matched by `AccountSpec` (not by id), and clear `expired_at`.
///
/// The caller supplies already-encrypted values (the baseline encrypts with
/// the user-external-account keyring inside the store); `Ok(false)` mirrors
/// the baseline's `userExternalAccountNotFoundError` when no row matches.
pub async fn update_account_data(
    pool: &PgPool,
    spec: &ExternalAccountSpec,
    auth_data: &str,
    account_data: &str,
    encryption_key_id: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
UPDATE user_external_accounts
SET auth_data = $5,
    account_data = $6,
    encryption_key_id = $7,
    updated_at = now(),
    expired_at = NULL
WHERE service_type = $1
  AND service_id = $2
  AND client_id = $3
  AND account_id = $4
  AND deleted_at IS NULL
"#,
    )
    .bind(&spec.service_type)
    .bind(&spec.service_id)
    .bind(&spec.client_id)
    .bind(&spec.account_id)
    .bind(auth_data)
    .bind(account_data)
    .bind(encryption_key_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// `extsvc.AccountSpec` as carried by `authz.Provider.FetchAccount`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountSpec {
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub account_id: String,
}

/// `userExternalAccountsStore.Upsert` failure modes.
#[derive(Debug)]
pub enum UpsertError {
    Database(sqlx::Error),
    /// The account already belongs to a different user.
    ConflictingUser {
        existing_user_id: i32,
        requested_user_id: i32,
    },
}

impl std::fmt::Display for UpsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(err) => write!(f, "{err}"),
            Self::ConflictingUser {
                existing_user_id,
                requested_user_id,
            } => write!(
                f,
                "unable to change association of external account from user \
                 {existing_user_id} to user {requested_user_id} (delete the external \
                 account and then try again)"
            ),
        }
    }
}

impl std::error::Error for UpsertError {}

impl From<sqlx::Error> for UpsertError {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err)
    }
}

/// `userExternalAccountsStore.Upsert`: insert the account when the
/// `(service_type, service_id, client_id, account_id)` tuple is new, otherwise
/// update its data and clear `expired_at`.
///
/// The account data columns are written empty: this port has no encryption
/// keyring yet, and the permission syncer only consumes account identity. The
/// baseline encrypts `auth_data`/`account_data` here.
pub async fn upsert(
    pool: &PgPool,
    user_id: i32,
    spec: &ExternalAccountSpec,
) -> Result<ExternalAccount, UpsertError> {
    let mut tx = pool.begin().await?;

    let existing = sqlx::query(
        r#"
SELECT id, user_id
FROM user_external_accounts
WHERE service_type = $1
  AND service_id = $2
  AND client_id = $3
  AND account_id = $4
  AND deleted_at IS NULL
"#,
    )
    .bind(&spec.service_type)
    .bind(&spec.service_id)
    .bind(&spec.client_id)
    .bind(&spec.account_id)
    .fetch_optional(&mut *tx)
    .await?;

    let row = match existing {
        Some(row) => {
            let existing_user_id: i32 = row.try_get("user_id")?;
            if existing_user_id != user_id {
                return Err(UpsertError::ConflictingUser {
                    existing_user_id,
                    requested_user_id: user_id,
                });
            }
            sqlx::query(
                r#"
UPDATE user_external_accounts
SET auth_data = '',
    account_data = '',
    encryption_key_id = '',
    updated_at = now(),
    expired_at = NULL
WHERE service_type = $1
  AND service_id = $2
  AND client_id = $3
  AND account_id = $4
  AND user_id = $5
  AND deleted_at IS NULL
RETURNING id, user_id, service_type, service_id, client_id, account_id,
          auth_data, account_data, encryption_key_id,
          (expired_at IS NOT NULL) AS expired
"#,
            )
            .bind(&spec.service_type)
            .bind(&spec.service_id)
            .bind(&spec.client_id)
            .bind(&spec.account_id)
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?
        }
        None => {
            sqlx::query(
                r#"
INSERT INTO user_external_accounts
    (user_id, service_type, service_id, client_id, account_id,
     auth_data, account_data, encryption_key_id)
VALUES ($1, $2, $3, $4, $5, '', '', '')
RETURNING id, user_id, service_type, service_id, client_id, account_id,
          auth_data, account_data, encryption_key_id,
          (expired_at IS NOT NULL) AS expired
"#,
            )
            .bind(user_id)
            .bind(&spec.service_type)
            .bind(&spec.service_id)
            .bind(&spec.client_id)
            .bind(&spec.account_id)
            .fetch_one(&mut *tx)
            .await?
        }
    };

    tx.commit().await?;
    account_from_row(&row).map_err(UpsertError::Database)
}

/// One live `user_external_accounts` row with the `created_at`/`updated_at`
/// columns the GraphQL `ExternalAccount` node exposes (M9.8ka).
///
/// Kept separate from [`ExternalAccount`] so the many worker test literals that
/// construct that type keep compiling; the columns are identical apart from the
/// two timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountWithTimestamps {
    pub id: i32,
    pub user_id: i32,
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub account_id: String,
    /// `account_data` (encrypted at rest). `None` is the SQL NULL.
    pub account_data: Option<String>,
    pub encryption_key_id: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

const ACCOUNT_WITH_TIMESTAMPS_COLUMNS: &str = "id, user_id, service_type, service_id, \
     client_id, account_id, account_data, encryption_key_id, created_at, updated_at";

fn account_with_timestamps_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ExternalAccountWithTimestamps, sqlx::Error> {
    Ok(ExternalAccountWithTimestamps {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        service_type: row.try_get("service_type")?,
        service_id: row.try_get("service_id")?,
        client_id: row.try_get("client_id")?,
        account_id: row.try_get("account_id")?,
        account_data: row.try_get("account_data")?,
        encryption_key_id: row.try_get("encryption_key_id")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// `userExternalAccountsStore.List` for `User.externalAccounts` (M9.8ka):
/// live accounts of one user, `ORDER BY id ASC`, no pagination (the GraphQL
/// connection applies `first`/+1 next-page probing in memory).
pub async fn list_for_user_with_timestamps(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<ExternalAccountWithTimestamps>, sqlx::Error> {
    let rows = sqlx::query(&format!(
        "SELECT {ACCOUNT_WITH_TIMESTAMPS_COLUMNS} FROM user_external_accounts \
         WHERE deleted_at IS NULL AND user_id = $1 ORDER BY id ASC"
    ))
    .bind(user_id)
    .fetch_all(pool)
    .await?;
    rows.iter().map(account_with_timestamps_from_row).collect()
}

/// `userExternalAccountsStore.Count` for `User.externalAccounts` (M9.8ka):
/// ignores the connection's pagination.
pub async fn count_for_user(pool: &PgPool, user_id: i32) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "SELECT COUNT(*) FROM user_external_accounts \
         WHERE deleted_at IS NULL AND user_id = $1",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get("count"))
}

/// `userExternalAccountsStore.Get` for `externalAccountByID` (M9.8ka).
pub async fn get_with_timestamps(
    pool: &PgPool,
    id: i32,
) -> Result<Option<ExternalAccountWithTimestamps>, sqlx::Error> {
    let row = sqlx::query(&format!(
        "SELECT {ACCOUNT_WITH_TIMESTAMPS_COLUMNS} FROM user_external_accounts \
         WHERE id = $1 AND deleted_at IS NULL LIMIT 1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.as_ref()
        .map(account_with_timestamps_from_row)
        .transpose()
}

/// `userExternalAccountsStore.List` for `Site.externalAccounts` (M9.9b):
/// arbitrary filters, id ascending, optional `LIMIT` (the connection has
/// already applied its `Limit++` next-page probe).
pub async fn list_with_timestamps(
    pool: &PgPool,
    opts: &ExternalAccountsListOptions,
    limit: Option<i64>,
) -> Result<Vec<ExternalAccountWithTimestamps>, sqlx::Error> {
    let mut query = sqlx::QueryBuilder::new(format!(
        "SELECT {ACCOUNT_WITH_TIMESTAMPS_COLUMNS} FROM user_external_accounts \
         WHERE deleted_at IS NULL"
    ));
    if let Some(user_id) = opts.user_id {
        query.push(" AND user_id = ").push_bind(user_id);
    }
    if let Some(service_type) = &opts.service_type {
        query
            .push(" AND service_type = ")
            .push_bind(service_type.clone());
    }
    if let Some(service_id) = &opts.service_id {
        query
            .push(" AND service_id = ")
            .push_bind(service_id.clone());
    }
    if let Some(client_id) = &opts.client_id {
        query.push(" AND client_id = ").push_bind(client_id.clone());
    }
    if let Some(account_id) = &opts.account_id {
        query
            .push(" AND account_id = ")
            .push_bind(account_id.clone());
    }
    if opts.exclude_expired {
        query.push(" AND expired_at IS NULL");
    }
    if opts.only_expired {
        query.push(" AND expired_at IS NOT NULL");
    }
    query.push(" ORDER BY id ASC");
    if let Some(limit) = limit {
        query.push(" LIMIT ").push_bind(limit);
    }
    let rows = query.build().fetch_all(pool).await?;
    rows.iter().map(account_with_timestamps_from_row).collect()
}

/// `userExternalAccountsStore.Count` with arbitrary filters.
pub async fn count_matching(
    pool: &PgPool,
    opts: &ExternalAccountsListOptions,
) -> Result<i64, sqlx::Error> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT COUNT(*) AS count FROM user_external_accounts WHERE deleted_at IS NULL",
    );
    if let Some(user_id) = opts.user_id {
        query.push(" AND user_id = ").push_bind(user_id);
    }
    if let Some(service_type) = &opts.service_type {
        query
            .push(" AND service_type = ")
            .push_bind(service_type.clone());
    }
    if let Some(service_id) = &opts.service_id {
        query
            .push(" AND service_id = ")
            .push_bind(service_id.clone());
    }
    if let Some(client_id) = &opts.client_id {
        query.push(" AND client_id = ").push_bind(client_id.clone());
    }
    if let Some(account_id) = &opts.account_id {
        query
            .push(" AND account_id = ")
            .push_bind(account_id.clone());
    }
    if opts.exclude_expired {
        query.push(" AND expired_at IS NULL");
    }
    if opts.only_expired {
        query.push(" AND expired_at IS NOT NULL");
    }
    let row = query.build().fetch_one(pool).await?;
    Ok(row.get("count"))
}

/// `userExternalAccountsStore.Delete` with `IDs: [id]` (soft delete),
/// including the baseline's `executing delete` wrap.
pub async fn soft_delete(pool: &PgPool, id: i32) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE user_external_accounts SET deleted_at = now() \
         WHERE deleted_at IS NULL AND id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|err| DbError {
        message: format!("executing delete: {err}"),
        not_found: false,
    })
}

/// `userExternalAccountsStore.Upsert` with caller-supplied (already
/// encrypted) data columns. Mirrors [`upsert`], including the conflicting-user
/// error and the update's `expired_at` reset.
pub async fn upsert_with_data(
    pool: &PgPool,
    user_id: i32,
    spec: &ExternalAccountSpec,
    auth_data: &str,
    account_data: &str,
    encryption_key_id: &str,
) -> Result<ExternalAccount, UpsertError> {
    let mut tx = pool.begin().await?;

    let existing = sqlx::query(
        r#"
SELECT id, user_id
FROM user_external_accounts
WHERE service_type = $1
  AND service_id = $2
  AND client_id = $3
  AND account_id = $4
  AND deleted_at IS NULL
"#,
    )
    .bind(&spec.service_type)
    .bind(&spec.service_id)
    .bind(&spec.client_id)
    .bind(&spec.account_id)
    .fetch_optional(&mut *tx)
    .await?;

    let row = match existing {
        Some(row) => {
            let existing_user_id: i32 = row.try_get("user_id")?;
            if existing_user_id != user_id {
                return Err(UpsertError::ConflictingUser {
                    existing_user_id,
                    requested_user_id: user_id,
                });
            }
            sqlx::query(
                r#"
UPDATE user_external_accounts
SET auth_data = $6,
    account_data = $7,
    encryption_key_id = $8,
    updated_at = now(),
    expired_at = NULL
WHERE service_type = $1
  AND service_id = $2
  AND client_id = $3
  AND account_id = $4
  AND user_id = $5
  AND deleted_at IS NULL
RETURNING id, user_id, service_type, service_id, client_id, account_id,
          auth_data, account_data, encryption_key_id,
          (expired_at IS NOT NULL) AS expired
"#,
            )
            .bind(&spec.service_type)
            .bind(&spec.service_id)
            .bind(&spec.client_id)
            .bind(&spec.account_id)
            .bind(user_id)
            .bind(auth_data)
            .bind(account_data)
            .bind(encryption_key_id)
            .fetch_one(&mut *tx)
            .await?
        }
        None => {
            sqlx::query(
                r#"
INSERT INTO user_external_accounts
    (user_id, service_type, service_id, client_id, account_id,
     auth_data, account_data, encryption_key_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id, user_id, service_type, service_id, client_id, account_id,
          auth_data, account_data, encryption_key_id,
          (expired_at IS NOT NULL) AS expired
"#,
            )
            .bind(user_id)
            .bind(&spec.service_type)
            .bind(&spec.service_id)
            .bind(&spec.client_id)
            .bind(&spec.account_id)
            .bind(auth_data)
            .bind(account_data)
            .bind(encryption_key_id)
            .fetch_one(&mut *tx)
            .await?
        }
    };

    tx.commit().await?;
    account_from_row(&row).map_err(UpsertError::Database)
}

/// `userExternalAccountsStore.TouchExpired`: mark the accounts expired.
pub async fn touch_expired(pool: &PgPool, ids: &[i32]) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("UPDATE user_external_accounts SET expired_at = now() WHERE id = ANY($1)")
        .bind(ids.to_vec())
        .execute(pool)
        .await
        .map(|_| ())
}

/// `userExternalAccountsStore.TouchLastValid`: clear expiry and record the
/// last successful validation.
pub async fn touch_last_valid(pool: &PgPool, id: i32) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE user_external_accounts \
         SET expired_at = NULL, last_valid_at = now() WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .map(|_| ())
}

/// `extsvc.AccountSpec` reduced to the fields the OAuth provisioning path
/// matches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthAccountSpec<'a> {
    pub service_type: &'a str,
    pub service_id: &'a str,
    pub client_id: &'a str,
    pub account_id: &'a str,
}

/// `userExternalAccountsStore.Update` for the OAuth login path: returns the
/// associated user ID when a live account matches the spec.
///
/// `Ok(None)` is the baseline's `userExternalAccountNotFoundError`. As in the
/// baseline the matching row has its `updated_at` bumped and `expired_at`
/// cleared.
pub async fn touch_for_auth(
    pool: &PgPool,
    spec: &AuthAccountSpec<'_>,
) -> Result<Option<i32>, sqlx::Error> {
    let row = sqlx::query(
        r#"
UPDATE user_external_accounts
SET updated_at = now(), expired_at = NULL
WHERE service_type = $1
  AND service_id = $2
  AND client_id = $3
  AND account_id = $4
  AND deleted_at IS NULL
RETURNING user_id
"#,
    )
    .bind(spec.service_type)
    .bind(spec.service_id)
    .bind(spec.client_id)
    .bind(spec.account_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| row.get("user_id")))
}

/// `userExternalAccountsStore.Upsert` for the OAuth login path: associates the
/// external account with `user_id`, inserting it if missing.
///
/// A spec already linked to a different user returns the baseline's
/// `unable to change association of external account ...` error.
pub async fn upsert_for_auth(
    pool: &PgPool,
    user_id: i32,
    spec: &AuthAccountSpec<'_>,
) -> Result<(), DbError> {
    let existing = sqlx::query(
        r#"
SELECT id, user_id
FROM user_external_accounts
WHERE service_type = $1
  AND service_id = $2
  AND client_id = $3
  AND account_id = $4
  AND deleted_at IS NULL
"#,
    )
    .bind(spec.service_type)
    .bind(spec.service_id)
    .bind(spec.client_id)
    .bind(spec.account_id)
    .fetch_optional(pool)
    .await
    .map_err(DbError::from_sqlx)?;

    let Some(row) = existing else {
        sqlx::query(
            r#"
INSERT INTO user_external_accounts (user_id, service_type, service_id, client_id, account_id, encryption_key_id)
VALUES ($1, $2, $3, $4, $5, '')
"#,
        )
        .bind(user_id)
        .bind(spec.service_type)
        .bind(spec.service_id)
        .bind(spec.client_id)
        .bind(spec.account_id)
        .execute(pool)
        .await
        .map_err(DbError::from_sqlx)?;
        return Ok(());
    };

    let associated_user_id: i32 = row.get("user_id");
    if associated_user_id != user_id {
        return Err(DbError {
            message: format!(
                "unable to change association of external account from user {associated_user_id} to user {user_id} (delete the external account and then try again)"
            ),
            not_found: false,
        });
    }

    sqlx::query(
        "UPDATE user_external_accounts SET updated_at = now(), expired_at = NULL WHERE id = $1",
    )
    .bind(row.get::<i32, _>("id"))
    .execute(pool)
    .await
    .map_err(DbError::from_sqlx)?;
    Ok(())
}
