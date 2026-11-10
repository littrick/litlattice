//! `settingsStore.GetLatest` for a user subject
//! (`internal/database/settings.go:88-120`), backing
//! `User.latestSettings` (`cmd/frontend/graphqlbackend/user.go:374-389`).

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

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

/// `settingsStore.GetLatest` with `{User: id}`: the newest row for the user,
/// only while the user itself is not soft-deleted; a soft-deleted author
/// becomes `None`. Never returns a row for a missing/deleted user.
pub async fn latest_for_user(
    pool: &PgPool,
    user_id: i32,
) -> Result<Option<SettingsRecord>, sqlx::Error> {
    let row = sqlx::query(
        r#"
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
"#,
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        Ok(SettingsRecord {
            id: row.try_get("id")?,
            contents: row.try_get("contents")?,
            created_at: row.try_get("created_at")?,
            author_user_id: row.try_get("author_user_id")?,
        })
    })
    .transpose()
}
