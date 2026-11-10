//! `settingsStore.GetLatest` (`internal/database/settings.go:88-120`) and
//! `settingsStore.CreateIfUpToDate` (`:48-86`) for the site, organization and
//! user subjects, backing `latestSettings`
//! (`cmd/frontend/graphqlbackend/{site,org,user}.go`) and
//! `Mutation.settingsMutation` (`settings_mutation.go`).

use sqlx::postgres::{PgConnection, PgPool};
use sqlx::types::time::OffsetDateTime;
use sqlx::{Postgres, Row, Transaction};

/// The subject of a settings row (`api.SettingsSubject`,
/// `internal/api/api.go:115-120`). `Default` is not writable: the baseline's
/// `nodeByID` has no `DefaultSettings` kind, so mutations never reach it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsSubjectKey {
    /// Global site settings (`user_id IS NULL AND org_id IS NULL`).
    Site,
    Org(i32),
    User(i32),
}

/// The `settings` row fields `settingsResolver` reads.
#[derive(Debug, Clone, PartialEq)]
pub struct SettingsRecord {
    pub id: i32,
    /// Raw JSONC contents (comments/trailing commas preserved).
    pub contents: String,
    pub created_at: OffsetDateTime,
    /// `CASE WHEN users.deleted_at IS NULL THEN s.author_user_id ELSE NULL
    /// END`: a soft-deleted author is reported as "no author".
    pub author_user_id: Option<i32>,
}

fn record_from_row(row: sqlx::postgres::PgRow) -> Result<SettingsRecord, sqlx::Error> {
    Ok(SettingsRecord {
        id: row.try_get("id")?,
        contents: row.try_get("contents")?,
        created_at: row.try_get("created_at")?,
        author_user_id: row.try_get("author_user_id")?,
    })
}

const LATEST_USER_SQL: &str = r#"
SELECT
	s.id,
	s.contents,
	s.created_at,
	CASE WHEN users.deleted_at IS NULL THEN s.author_user_id ELSE NULL END
		AS author_user_id
FROM settings s
LEFT JOIN users ON users.id = s.author_user_id
WHERE s.user_id = $1
  AND EXISTS (
	SELECT 1 FROM users
	WHERE id = $1 AND deleted_at IS NULL
  )
ORDER BY s.id DESC
LIMIT 1
"#;

const LATEST_ORG_SQL: &str = r#"
SELECT
	s.id,
	s.contents,
	s.created_at,
	CASE WHEN users.deleted_at IS NULL THEN s.author_user_id ELSE NULL END
		AS author_user_id
FROM settings s
LEFT JOIN users ON users.id = s.author_user_id
WHERE s.org_id = $1
ORDER BY s.id DESC
LIMIT 1
"#;

const LATEST_SITE_SQL: &str = r#"
SELECT
	s.id,
	s.contents,
	s.created_at,
	CASE WHEN users.deleted_at IS NULL THEN s.author_user_id ELSE NULL END
		AS author_user_id
FROM settings s
LEFT JOIN users ON users.id = s.author_user_id
WHERE s.user_id IS NULL AND s.org_id IS NULL
ORDER BY s.id DESC
LIMIT 1
"#;

const INSERT_SQL: &str = r#"
INSERT INTO settings(org_id, user_id, author_user_id, contents)
VALUES($1, $2, $3, $4)
RETURNING id, created_at
"#;

/// `settingsStore.GetLatest` for any writable subject, on a plain connection
/// (used both standalone and inside `CreateIfUpToDate`'s transaction).
pub async fn latest_for_subject(
    conn: &mut PgConnection,
    subject: SettingsSubjectKey,
) -> Result<Option<SettingsRecord>, sqlx::Error> {
    let row = match subject {
        SettingsSubjectKey::User(user_id) => {
            sqlx::query(LATEST_USER_SQL)
                .bind(user_id)
                .fetch_optional(&mut *conn)
                .await?
        }
        SettingsSubjectKey::Org(org_id) => {
            sqlx::query(LATEST_ORG_SQL)
                .bind(org_id)
                .fetch_optional(&mut *conn)
                .await?
        }
        SettingsSubjectKey::Site => {
            sqlx::query(LATEST_SITE_SQL)
                .fetch_optional(&mut *conn)
                .await?
        }
    };
    row.map(record_from_row).transpose()
}

/// `settingsStore.GetLatest({User: id})`: the newest row for the user, only
/// while the user itself is not soft-deleted; a soft-deleted author becomes
/// `None`. Never returns a row for a missing/deleted user.
pub async fn latest_for_user(
    pool: &PgPool,
    user_id: i32,
) -> Result<Option<SettingsRecord>, sqlx::Error> {
    let row = sqlx::query(LATEST_USER_SQL)
        .bind(user_id)
        .fetch_optional(pool)
        .await?;

    row.map(record_from_row).transpose()
}

/// `settingsStore.GetLatest({Org: id})`: the newest row for the organization.
pub async fn latest_for_org(
    pool: &PgPool,
    org_id: i32,
) -> Result<Option<SettingsRecord>, sqlx::Error> {
    let row = sqlx::query(LATEST_ORG_SQL)
        .bind(org_id)
        .fetch_optional(pool)
        .await?;

    row.map(record_from_row).transpose()
}

/// `settingsStore.GetLatest` with neither org nor user: global site settings.
pub async fn latest_for_site(pool: &PgPool) -> Result<Option<SettingsRecord>, sqlx::Error> {
    let row = sqlx::query(LATEST_SITE_SQL).fetch_optional(pool).await?;

    row.map(record_from_row).transpose()
}

/// `settingsStore.CreateIfUpToDate` (`internal/database/settings.go:48-86`)
/// minus `conf.ValidateSettings`, which the caller runs first (observable
/// order: validation errors are returned before the transaction opens).
///
/// Inserts a new `settings` row when there is no latest row or the client's
/// `last_id` matches it; otherwise returns the concurrently written row.
pub async fn create_if_up_to_date(
    pool: &PgPool,
    subject: SettingsSubjectKey,
    last_id: Option<i32>,
    author_user_id: Option<i32>,
    contents: &str,
) -> Result<SettingsRecord, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;
    let latest = latest_for_subject(&mut tx, subject).await?;

    let creator_is_up_to_date = latest
        .as_ref()
        .is_some_and(|latest| last_id == Some(latest.id));
    let created = match latest {
        Some(latest) if !creator_is_up_to_date => latest,
        _ => {
            let (org_id, user_id) = match subject {
                SettingsSubjectKey::Site => (None, None),
                SettingsSubjectKey::Org(org_id) => (Some(org_id), None),
                SettingsSubjectKey::User(user_id) => (None, Some(user_id)),
            };
            let row = sqlx::query(INSERT_SQL)
                .bind(org_id)
                .bind(user_id)
                .bind(author_user_id)
                .bind(contents)
                .fetch_one(&mut *tx)
                .await?;
            SettingsRecord {
                id: row.try_get("id")?,
                contents: contents.to_owned(),
                created_at: row.try_get("created_at")?,
                author_user_id,
            }
        }
    };

    tx.commit().await?;
    Ok(created)
}
