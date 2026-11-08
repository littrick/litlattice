//! `internal/database/user_emails.go` (`GetInitialSiteAdminInfo`) and the
//! `global_state` pre-check it performs (`SiteInitialized`).
//!
//! Only the path the usage telemetry exporter reads is ported: the email and
//! terms-of-service flag of the active site admin with the lowest user ID,
//! which is the baseline's best guess for the initial installer. A site that
//! has not been initialized yields an empty email; a lookup failure is
//! replaced with the baseline's fixed message.

use sqlx::postgres::PgPool;
use sqlx::Row;

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
