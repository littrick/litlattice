//! `internal/database/authz.go`: the database-backed part of
//! `AuthzStore.GrantPendingPermissions`.
//!
//! It enumerates the user's live external accounts, then the site-configuration
//! user-mapping identity (verified email addresses or the username), and hands
//! each as a `PermsStore.GrantPendingPermissions` call inside one transaction.
//! The site mapping itself is a configuration concern: callers pass the
//! already-normalized `bind_id` (`conf.PermissionsUserMapping` maps a missing
//! or invalid `bindID` to `email`), exactly like the Go orchestrator reads
//! `conf.PermissionsUserMapping().BindID`.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use crate::perms_store::{grant_pending_permissions_tx, UserIdWithExternalAccountId};
use crate::user_external_accounts::{self, ExternalAccountsListOptions};
use crate::{user_emails, users};

/// `authz.SourcegraphServiceType` (`internal/authz/consts.go:6`).
pub const SOURCEGRAPH_SERVICE_TYPE: &str = "sourcegraph";
/// `authz.SourcegraphServiceID` (`internal/authz/consts.go:7`).
pub const SOURCEGRAPH_SERVICE_ID: &str = "https://sourcegraph.com/";

/// Errors of [`grant_pending_permissions`]; the wrappers mirror the contexts
/// the Go orchestrator adds (`internal/database/authz.go:103-175`).
#[derive(Debug, thiserror::Error)]
pub enum GrantPendingPermissionsError {
    #[error("list external accounts: {0}")]
    ListExternalAccounts(#[source] sqlx::Error),
    #[error("list verified emails: {0}")]
    ListVerifiedEmails(#[source] sqlx::Error),
    #[error("get user: user not found")]
    UserNotFound,
    #[error("get user: {0}")]
    GetUser(#[source] sqlx::Error),
    #[error("start transaction: {0}")]
    StartTransaction(#[source] sqlx::Error),
    #[error("grant pending permissions: {0}")]
    GrantPendingPermissions(#[source] sqlx::Error),
    #[error("commit transaction: {0}")]
    Commit(#[source] sqlx::Error),
    #[error("unrecognized user mapping bind ID type {0:?}")]
    UnrecognizedBindId(String),
}

/// One `authz.UserGrantPermissions` row.
struct Grant {
    user: UserIdWithExternalAccountId,
    service_type: String,
    service_id: String,
    bind_id: String,
}

/// `authzStore.GrantPendingPermissions` (`internal/database/authz.go:98-177`).
///
/// `user_id <= 0` is a no-op. Every live, non-expired external account is
/// granted regardless of `bind_id`; the mapping adds either all verified email
/// addresses or the username under the fixed `sourcegraph` service identity.
/// All grants run in one transaction (`authz.go:164-175`), so a failure rolls
/// the whole user's pending permissions back.
pub async fn grant_pending_permissions(
    pool: &PgPool,
    user_id: i32,
    bind_id: &str,
    now: OffsetDateTime,
) -> Result<(), GrantPendingPermissionsError> {
    if user_id <= 0 {
        return Ok(());
    }

    // Gather external accounts associated to the user (`authz.go:103-125`).
    // `ExcludeExpired` also filters `deleted_at IS NOT NULL` in the store.
    let accounts = user_external_accounts::list(
        pool,
        &ExternalAccountsListOptions {
            user_id: Some(user_id),
            exclude_expired: true,
            ..Default::default()
        },
    )
    .await
    .map_err(GrantPendingPermissionsError::ListExternalAccounts)?;

    let mut grants: Vec<Grant> = Vec::with_capacity(accounts.len() + 1);
    for account in accounts {
        grants.push(Grant {
            user: UserIdWithExternalAccountId {
                user_id,
                external_account_id: account.id,
            },
            service_type: account.service_type,
            service_id: account.service_id,
            bind_id: account.account_id,
        });
    }

    // Gather username or verified email based on site configuration
    // (`authz.go:127-162`).
    match bind_id {
        "email" => {
            // SECURITY: only verified addresses are grantable (`authz.go:131`).
            let emails = user_emails::list_by_user(pool, user_id, true)
                .await
                .map_err(GrantPendingPermissionsError::ListVerifiedEmails)?;
            for email in emails {
                grants.push(Grant {
                    user: UserIdWithExternalAccountId {
                        user_id,
                        external_account_id: 0,
                    },
                    service_type: SOURCEGRAPH_SERVICE_TYPE.to_owned(),
                    service_id: SOURCEGRAPH_SERVICE_ID.to_owned(),
                    bind_id: email.email,
                });
            }
        }
        "username" => {
            let user = users::get_by_id(pool, user_id)
                .await
                .map_err(GrantPendingPermissionsError::GetUser)?
                .ok_or(GrantPendingPermissionsError::UserNotFound)?;
            grants.push(Grant {
                user: UserIdWithExternalAccountId {
                    user_id,
                    external_account_id: 0,
                },
                service_type: SOURCEGRAPH_SERVICE_TYPE.to_owned(),
                service_id: SOURCEGRAPH_SERVICE_ID.to_owned(),
                bind_id: user.username,
            });
        }
        other => return Err(GrantPendingPermissionsError::UnrecognizedBindId(other.to_owned())),
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(GrantPendingPermissionsError::StartTransaction)?;
    for grant in grants {
        grant_pending_permissions_tx(
            &mut tx,
            grant.user,
            &grant.service_type,
            &grant.service_id,
            &grant.bind_id,
            now,
        )
        .await
        .map_err(GrantPendingPermissionsError::GrantPendingPermissions)?;
    }
    tx.commit()
        .await
        .map_err(GrantPendingPermissionsError::Commit)?;
    Ok(())
}

/// [`grant_pending_permissions`] inside the caller's transaction.
///
/// This is the `tx.Authz().GrantPendingPermissions` shape: Go's
/// `SetVerified` runs the verified-email write and the grant in one
/// `WithTransact` (`internal/database/user_emails.go:241-260`). The
/// enumeration therefore reads the same transaction, so a newly verified
/// email is grantable immediately, and the grants share the email write's
/// commit/rollback.
pub async fn grant_pending_permissions_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user_id: i32,
    bind_id: &str,
    now: OffsetDateTime,
) -> Result<(), GrantPendingPermissionsError> {
    if user_id <= 0 {
        return Ok(());
    }

    // Enumeration runs on the caller's transaction (`authz.go:103-162` with
    // `s.store` bound to `tx`).
    let mut grants: Vec<Grant> = Vec::new();
    {
        let conn = &mut **tx;

        // Gather external accounts (`authz.go:103-125`). `ExcludeExpired`
        // filters `deleted_at IS NULL` and `expired_at IS NULL`.
        let rows = sqlx::query(
            "SELECT id, service_type, service_id, account_id FROM user_external_accounts \
             WHERE user_id = $1 AND deleted_at IS NULL AND expired_at IS NULL ORDER BY id ASC",
        )
        .bind(user_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(GrantPendingPermissionsError::ListExternalAccounts)?;
        for row in rows {
            grants.push(Grant {
                user: UserIdWithExternalAccountId {
                    user_id,
                    external_account_id: row.get("id"),
                },
                service_type: row.get("service_type"),
                service_id: row.get("service_id"),
                bind_id: row.get("account_id"),
            });
        }

        match bind_id {
            "email" => {
                // SECURITY: only verified addresses are grantable (`authz.go:131`).
                let rows = sqlx::query(
                    "SELECT email FROM user_emails \
                     WHERE user_id = $1 AND verified_at IS NOT NULL \
                     ORDER BY created_at ASC, email ASC",
                )
                .bind(user_id)
                .fetch_all(&mut *conn)
                .await
                .map_err(GrantPendingPermissionsError::ListVerifiedEmails)?;
                for row in rows {
                    grants.push(Grant {
                        user: UserIdWithExternalAccountId {
                            user_id,
                            external_account_id: 0,
                        },
                        service_type: SOURCEGRAPH_SERVICE_TYPE.to_owned(),
                        service_id: SOURCEGRAPH_SERVICE_ID.to_owned(),
                        bind_id: row.get("email"),
                    });
                }
            }
            "username" => {
                let username: Option<String> = sqlx::query_scalar(
                    "SELECT username FROM users WHERE id = $1 AND deleted_at IS NULL LIMIT 1",
                )
                .bind(user_id)
                .fetch_optional(&mut *conn)
                .await
                .map_err(GrantPendingPermissionsError::GetUser)?;
                let username = username.ok_or(GrantPendingPermissionsError::UserNotFound)?;
                grants.push(Grant {
                    user: UserIdWithExternalAccountId {
                        user_id,
                        external_account_id: 0,
                    },
                    service_type: SOURCEGRAPH_SERVICE_TYPE.to_owned(),
                    service_id: SOURCEGRAPH_SERVICE_ID.to_owned(),
                    bind_id: username,
                });
            }
            other => {
                return Err(GrantPendingPermissionsError::UnrecognizedBindId(other.to_owned()))
            }
        }
    }

    for grant in grants {
        grant_pending_permissions_tx(
            tx,
            grant.user,
            &grant.service_type,
            &grant.service_id,
            &grant.bind_id,
            now,
        )
        .await
        .map_err(GrantPendingPermissionsError::GrantPendingPermissions)?;
    }
    Ok(())
}
