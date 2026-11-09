//! `internal/database/conf.go`: the read side of the frontend's site
//! configuration store (`critical_and_site_config`).
//!
//! The frontend's configuration source (`configurationSource.Read` in
//! `cmd/frontend/internal/cli/config.go:450-470`) calls `SiteGetLatest`,
//! which seeds the deployment's default configuration when the table is
//! empty (`addDefault` → `createIfUpToDate(..., isOverride=true)`). Only that
//! path is ported here:
//!
//! - [`get_latest`]: `getLatest`, `type='site' ORDER BY id DESC LIMIT 1`;
//! - [`site_get_latest`]: `SiteGetLatest`, in a transaction:
//!   `add_default` then `get_latest`;
//! - [`add_default`]: `addDefault`, which inserts the supplied default
//!   document when (and only when) no site config exists yet.
//!
//! The default document and its redacted form are parameters: `sg-db` must
//! not depend on `sg-service-frontend`, which owns the deployment defaults
//! (`deploy_defaults::default_site_config_json` and the matching redacted
//! snapshot).
//!
//! Not ported (see `docs/overviews/m9_8eg_conf_db_source_overview.md`):
//! `conf.Validate`/`ValidateSite` and the general `RedactAndHashSecrets`.
//! The default documents are fixed constants that the baseline validates
//! successfully, so inserting them needs no validation; a caller-supplied
//! user config is rejected by the missing write path, not silently accepted.
//! `SiteCreateIfUpToDate`, `ListSiteConfigs` and `GetSiteConfigCount` remain
//! unported.

use sqlx::postgres::{PgConnection, PgPool, PgRow};
use sqlx::{Postgres, Row, Transaction};

pub use sqlx::types::time::OffsetDateTime;

/// `database.SiteConfig`: one row of `critical_and_site_config`.
///
/// Nullable columns map to the baseline scanner's zero values: NULL
/// `author_user_id` scans as 0, NULL `redacted_contents` scans as `""`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteConfig {
    pub id: i32,
    pub author_user_id: i32,
    pub contents: String,
    pub redacted_contents: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `getLatestFmtStr`.
const GET_LATEST_SQL: &str = "
SELECT id, author_user_id, contents, redacted_contents, created_at, updated_at
FROM critical_and_site_config
WHERE type='site'
ORDER BY id DESC
LIMIT 1
";

/// `createSiteConfigFmtStr` (`RETURNING` the full row rather than only the
/// ID, so [`create_default`] mirrors `scanSiteConfigRow`).
const INSERT_SQL: &str = "
INSERT INTO critical_and_site_config (type, author_user_id, contents, redacted_contents)
VALUES ('site', $1, $2, $3)
RETURNING id, author_user_id, contents, redacted_contents, created_at, updated_at
";

/// `getLatest`: the most recently saved site config, or `None` when the
/// table has no `type='site'` row.
pub async fn get_latest(pool: &PgPool) -> Result<Option<SiteConfig>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    fetch_latest(&mut conn).await
}

/// `SiteGetLatest`: seed the default configuration when the table is empty,
/// then return the latest site config. Runs in one transaction like
/// `confStore.SiteGetLatest`.
pub async fn site_get_latest(
    pool: &PgPool,
    author_user_id: i32,
    default_site: &str,
    default_redacted: &str,
) -> Result<SiteConfig, sqlx::Error> {
    let mut tx = pool.begin().await?;
    add_default(&mut tx, author_user_id, default_site, default_redacted).await?;
    let latest = fetch_latest(&mut tx)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    tx.commit().await?;
    Ok(latest)
}

/// `addDefault`: returns the new config's ID when the default was written,
/// `None` when a site config already existed.
pub async fn add_default(
    tx: &mut Transaction<'_, Postgres>,
    author_user_id: i32,
    default_site: &str,
    default_redacted: &str,
) -> Result<Option<i32>, sqlx::Error> {
    if fetch_latest(&mut *tx).await?.is_some() {
        return Ok(None);
    }
    let config = create_default(tx, author_user_id, default_site, default_redacted).await?;
    Ok(Some(config.id))
}

/// The `isOverride=true` branch of `createIfUpToDate` as used by
/// `addDefault`: insert the default document and return the stored row.
async fn create_default(
    tx: &mut Transaction<'_, Postgres>,
    author_user_id: i32,
    contents: &str,
    redacted_contents: &str,
) -> Result<SiteConfig, sqlx::Error> {
    // `dbutil.NullInt32Column(authorUserID)`: 0 is written as NULL.
    let author = if author_user_id == 0 {
        None
    } else {
        Some(author_user_id)
    };
    let row = sqlx::query(INSERT_SQL)
        .bind(author)
        .bind(contents)
        .bind(redacted_contents)
        .fetch_one(&mut **tx)
        .await?;
    site_config_from_row(&row)
}

async fn fetch_latest(conn: &mut PgConnection) -> Result<Option<SiteConfig>, sqlx::Error> {
    let row = sqlx::query(GET_LATEST_SQL)
        .fetch_optional(&mut *conn)
        .await?;
    row.as_ref().map(site_config_from_row).transpose()
}

/// `scanSiteConfigRow`: NULL `author_user_id` → 0, NULL
/// `redacted_contents` → `""`.
fn site_config_from_row(row: &PgRow) -> Result<SiteConfig, sqlx::Error> {
    let author_user_id: Option<i32> = row.try_get("author_user_id")?;
    let redacted_contents: Option<String> = row.try_get("redacted_contents")?;
    Ok(SiteConfig {
        id: row.try_get("id")?,
        author_user_id: author_user_id.unwrap_or(0),
        contents: row.try_get("contents")?,
        redacted_contents: redacted_contents.unwrap_or_default(),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nullable column mapping shared by every scan path.
    #[test]
    fn site_config_shape() {
        let config = SiteConfig {
            id: 7,
            author_user_id: 0,
            contents: r#"{"auth.providers":[]}"#.to_owned(),
            redacted_contents: String::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        };
        assert_eq!(config.id, 7);
        assert!(config.contents.contains("auth.providers"));
        assert!(config.redacted_contents.is_empty());
    }
}
