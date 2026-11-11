//! `internal/database/user_emails.go`: `GetInitialSiteAdminInfo` (plus the
//! `global_state` pre-check it performs, `SiteInitialized`) and
//! `GetPrimaryEmail`, which the code monitor e-mail action reads.
//!
//! `get_initial_site_admin_info` ports the path the usage telemetry exporter
//! reads: the email and terms-of-service flag of the active site admin with
//! the lowest user ID, which is the baseline's best guess for the initial
//! installer. A site that has not been initialized yields an empty email; a
//! lookup failure is replaced with the baseline's fixed message.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `database.UserEmail`, reduced to the fields commit-search username expansion
/// reads (`internal/database/user_emails.go:17-25`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserEmail {
    pub user_id: i32,
    pub email: String,
    /// `VerifiedAt`; only verified addresses are expanded to commit-author
    /// regexes (`internal/search/commit/commit.go:190-193`).
    pub verified_at: Option<OffsetDateTime>,
}

/// `userEmailsStore.ListByUser` (`internal/database/user_emails.go:338-356`):
/// the user's addresses ordered by `created_at ASC, email ASC`, optionally
/// restricted to verified ones.
pub async fn list_by_user(
    pool: &PgPool,
    user_id: i32,
    only_verified: bool,
) -> Result<Vec<UserEmail>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT user_id, email, verified_at FROM user_emails \
         WHERE user_id=$1 AND (NOT $2 OR verified_at IS NOT NULL) \
         ORDER BY created_at ASC, email ASC",
    )
    .bind(user_id)
    .bind(only_verified)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(UserEmail {
                user_id: row.try_get("user_id")?,
                email: row.try_get("email")?,
                verified_at: row.try_get("verified_at")?,
            })
        })
        .collect()
}

/// `userEmailsStore.GetInitialSiteAdminInfo`.
///
/// `Err` carries the message the baseline produces for any lookup failure:
/// it discards the driver error (`errors.New("initial site admin email not
/// found")`).
pub async fn get_initial_site_admin_info(pool: &PgPool) -> Result<(String, bool), String> {
    // `GlobalStateWith(s).SiteInitialized`: `coalesce(bool_or(initialized), false)`.
    let initialized: bool =
        sqlx::query_scalar("SELECT coalesce(bool_or(gs.initialized), false) FROM global_state gs")
            .fetch_one(pool)
            .await
            .map_err(|err| err.to_string())?;
    if !initialized {
        return Ok((String::new(), false));
    }

    let row = sqlx::query(
        "SELECT email, tos_accepted FROM user_emails \
         JOIN users ON user_emails.user_id=users.id \
         WHERE users.site_admin AND users.deleted_at IS NULL \
         ORDER BY users.id ASC LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(|_| "initial site admin email not found".to_owned())?;

    match row {
        Some(row) => Ok((
            row.try_get("email")
                .map_err(|_| "initial site admin email not found".to_owned())?,
            row.try_get("tos_accepted")
                .map_err(|_| "initial site admin email not found".to_owned())?,
        )),
        None => Err("initial site admin email not found".to_owned()),
    }
}

/// `userEmailsStore.GetPrimaryEmail`: the user's `is_primary` address and
/// whether it has been verified (`verified_at IS NOT NULL`).
///
/// `None` is the baseline's `userEmailNotFoundError`, which
/// `errcode.IsNotFound` reports as not-found (the code monitor e-mail action
/// turns it into `unable to send email to user ID %d with unknown email
/// address`). Other failures keep the driver error text.
pub async fn get_primary_email(
    pool: &PgPool,
    user_id: i32,
) -> Result<Option<(String, bool)>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT email, verified_at IS NOT NULL AS verified FROM user_emails \
         WHERE user_id=$1 AND is_primary",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| {
        Ok((
            row.try_get::<String, _>("email")?,
            row.try_get::<bool, _>("verified")?,
        ))
    })
    .transpose()
}
