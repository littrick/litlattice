//! `internal/database/access_tokens.go`: the `Lookup` path the external
//! access-token middleware uses (M9.8jc).
//!
//! Only the read/last-used path is ported: the token prefix parsing and
//! SHA-256 hashing live in the frontend adapter (they are pure functions and
//! `sg-db` has no hashing/hex dependencies), while the SQL mirrors
//! `TokenLookupOpts.toGetQuery` and `toUpdateLastUsedQuery` verbatim:
//!
//! * subject and creator users must exist and not be deleted;
//! * the token must not be deleted, must not be expired, and must carry the
//!   required scope;
//! * `OnlyAdmin` additionally requires `subject_user.site_admin`;
//! * `last_used_at` is refreshed when it is NULL or older than five minutes;
//!   a failed refresh is logged by the caller and ignored (the baseline does
//!   the same).

use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};

/// One active token row (`id`, `subject_user_id`, `last_used_at`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessTokenLookup {
    pub id: i64,
    pub subject_user_id: i32,
    pub last_used_at: Option<OffsetDateTime>,
}

/// `TokenLookupOpts.toGetQuery()`: `Ok(None)` is `ErrAccessTokenNotFound`.
pub async fn lookup_access_token(
    pool: &PgPool,
    token_hash: &[u8],
    required_scope: &str,
    only_admin: bool,
) -> Result<Option<AccessTokenLookup>, sqlx::Error> {
    let mut query = String::from(
        r#"
SELECT t.id, t.subject_user_id, t.last_used_at
FROM access_tokens t
JOIN users subject_user ON t.subject_user_id=subject_user.id AND subject_user.deleted_at IS NULL
JOIN users creator_user ON t.creator_user_id=creator_user.id AND creator_user.deleted_at IS NULL
WHERE
    t.value_sha256=$1
    AND
	t.deleted_at IS NULL
	AND
	(t.expires_at IS NULL OR t.expires_at > NOW())
	AND
    $2::text = ANY (t.scopes)
"#,
    );
    if only_admin {
        query.push_str("AND subject_user.site_admin");
    }
    let row = sqlx::query(&query)
        .bind(token_hash)
        .bind(required_scope)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| AccessTokenLookup {
        id: row.get("id"),
        subject_user_id: row.get("subject_user_id"),
        last_used_at: row.get("last_used_at"),
    }))
}

/// `TokenLookupOpts.toUpdateLastUsedQuery()`.
pub async fn update_access_token_last_used(
    pool: &PgPool,
    token_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE access_tokens t SET last_used_at=now() WHERE t.id=$1 AND t.deleted_at IS NULL",
    )
    .bind(token_id)
    .execute(pool)
    .await
    .map(|_| ())
}

/// `audit.SecurityEventLocation(conf.SiteConfig())` re-exported for the
/// adapter's write gate (the existing `security_event_logs` module owns the
/// predicate).
pub use crate::security_event_logs::{
    insert_security_event, security_event_logs_in_database, SecurityEvent,
};

/// A convenience alias for the argument JSON the adapter builds.
pub type SecurityEventArgument = Value;
