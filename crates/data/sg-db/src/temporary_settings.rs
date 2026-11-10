#![forbid(unsafe_code)]

//! `db.TemporarySettings()` (`internal/database/temporary_settings.go:30-80`):
//! the per-user JSON blob the web UI stores in `temporary_settings.contents`
//! (jsonb). The table is part of the M6 storage contract; this module does not
//! change its shape.

use sqlx::PgPool;

/// `GetTemporarySettings`: the stored jsonb text, or `{}` when the user has
/// no row yet. A row whose `contents` is SQL NULL is a scan error in the
/// baseline (`database/sql` cannot scan NULL into `string`), so it is reported
/// here instead of being confused with "no row".
pub async fn get_temporary_settings(pool: &PgPool, user_id: i32) -> Result<String, String> {
    let contents: Option<Option<String>> = sqlx::query_scalar(
        "SELECT contents::text FROM temporary_settings WHERE user_id = $1 LIMIT 1",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    match contents {
        None => Ok("{}".to_owned()),
        Some(Some(text)) => Ok(text),
        Some(None) => Err("converting NULL to string is unsupported".to_owned()),
    }
}

/// `OverwriteTemporarySettings`: replace the user's blob, creating the row.
///
/// The parameter is cast to `jsonb` because sqlx binds `text` and Postgres has
/// no assignment cast from `text` to `jsonb`; the baseline binds an untyped
/// literal.
pub async fn overwrite_temporary_settings(
    pool: &PgPool,
    user_id: i32,
    contents: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO temporary_settings (user_id, contents) VALUES ($1, $2::jsonb) \
         ON CONFLICT (user_id) DO UPDATE SET contents = $2::jsonb, updated_at = now()",
    )
    .bind(user_id)
    .bind(contents)
    .execute(pool)
    .await?;
    Ok(())
}

/// `EditTemporarySettings`: jsonb top-level merge (`||`), creating the row
/// when absent. Invalid JSON or a non-object operand fails like the baseline
/// (the error text comes from the driver).
pub async fn edit_temporary_settings(
    pool: &PgPool,
    user_id: i32,
    settings_to_edit: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO temporary_settings AS t (user_id, contents) VALUES ($1, $2::jsonb) \
         ON CONFLICT (user_id) DO UPDATE SET contents = t.contents || $2::jsonb, updated_at = now()",
    )
    .bind(user_id)
    .bind(settings_to_edit)
    .execute(pool)
    .await?;
    Ok(())
}
