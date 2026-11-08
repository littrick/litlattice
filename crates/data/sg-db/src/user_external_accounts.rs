//! `internal/database/external_accounts.go`: the `List` and `Delete` paths the
//! expired Sourcegraph Operator account cleaner uses.
//!
//! `List` returns every live account of a user ordered by id (`deleted_at IS
//! NULL ... ORDER BY id ASC`); `account_data` is returned as stored, with the
//! `encryption_key_id` the caller needs to decrypt it (`Unmigrated`/empty key
//! ids are plaintext). `Delete` with `HardDelete` removes the matching rows.

use sqlx::postgres::PgPool;
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
