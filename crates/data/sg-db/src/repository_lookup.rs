//! `repoStore.GetByName` for the frontend GraphQL `Query.repository` field
//! (`internal/database/repos.go:206-240`, `internal/database/repos_perm.go`).
//!
//! Unlike the gitserver-facing [`crate::repos::RepoStore`] (internal actor,
//! no permission filter), this lookup applies the baseline's repository
//! permission predicate: anonymous and regular users only see unrestricted
//! repositories (public repos, repos from `unrestricted` external services,
//! and repos with a `user_repo_permissions` row for `user_id IS NULL`);
//! authenticated users additionally see repos granted to their user ID.
//! Site admins bypass the predicate when `authz.enforceForSiteAdmins` is not
//! enabled (the default), which the caller passes as `bypass_authz`.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// The `repo` columns the `Repository` GraphQL resolvers read.
#[derive(Debug, Clone, PartialEq)]
pub struct RepoLookupRecord {
    pub id: i32,
    pub name: String,
    /// `repo.uri` (NULL → empty string); the deprecated `Repository.uri`.
    pub uri: String,
    /// `repo.description` (NULL → empty string).
    pub description: String,
    pub fork: bool,
    pub archived: bool,
    pub private: bool,
    pub stars: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: Option<OffsetDateTime>,
    /// `types.Repo.ExternalRepo.ID` (`repo.external_id`).
    pub external_id: String,
    /// `types.Repo.ExternalRepo.ServiceType`.
    pub external_service_type: String,
    /// `types.Repo.ExternalRepo.ServiceID`.
    pub external_service_id: String,
    /// `types.RepoBlock.Reason` when the repository is blocked; `None` is
    /// `repo.blocked IS NULL` (the baseline's `IsBlocked() == nil`).
    pub blocked_reason: Option<String>,
}

impl RepoLookupRecord {
    /// `BlockedRepoError.Error()` (`internal/types/types.go:186-188`).
    pub fn blocked_error(&self) -> Option<String> {
        self.blocked_reason.as_ref().map(|reason| {
            format!(
                "repository {} has been blocked. reason: {}",
                self.name, reason
            )
        })
    }
}

/// `repoStore.GetByName`: name first, `uri` as a fallback, blocked
/// repositories returned (with their reason) rather than hidden.
///
/// The baseline issues two queries (`Names`, then `URIs`) because `uri` is
/// not unique; this form ranks a name match first. `ORDER BY id` only breaks
/// ties between duplicate `uri` values, where the baseline's second query has
/// no deterministic order.
pub async fn find_by_name_or_uri(
    pool: &PgPool,
    input: &str,
    authenticated_user_id: i32,
    bypass_authz: bool,
) -> Result<Option<RepoLookupRecord>, sqlx::Error> {
    // `lower(name::text) COLLATE "C" = ANY(lower(inputs))` in the baseline.
    let lowered = input.to_lowercase();
    let row = sqlx::query(&format!(
        r#"
SELECT
{REPO_COLUMNS}
FROM repo r
WHERE r.deleted_at IS NULL
  AND (lower(r.name::text) COLLATE "C" = $1 OR r.uri = $2::citext)
  AND (
	$3
	OR EXISTS (
		SELECT FROM user_repo_permissions
		WHERE repo_id = r.id AND user_id IS NULL
	)
	OR NOT r.private
	OR EXISTS (
		SELECT FROM external_services es
		JOIN external_service_repos esr ON (
			esr.external_service_id = es.id
			AND esr.repo_id = r.id
			AND es.unrestricted = TRUE
			AND es.deleted_at IS NULL
		)
	)
	OR EXISTS (
		SELECT repo_id FROM user_repo_permissions
		WHERE repo_id = r.id AND user_id = $4
	)
  )
ORDER BY (lower(r.name::text) COLLATE "C" = $1) DESC, r.id
LIMIT 1
"#
    ))
    .bind(lowered)
    .bind(input)
    .bind(bypass_authz)
    .bind(authenticated_user_id)
    .fetch_optional(pool)
    .await?;

    row.map(record_from_row).transpose()
}

/// `repoStore.Get` (`internal/database/repos.go:145-166`): `listRepos` by ID
/// with `Limit: 1`, `IncludeBlocked: true` and the same `AuthzQueryConds`
/// predicate as `GetByName` (`internal/database/repos_perm.go:71-117`).
/// `bypass_authz` is the actor's site-admin status (unless
/// `authz.enforceForSiteAdmins` is enabled, which is not read here).
pub async fn find_by_id(
    pool: &PgPool,
    id: i32,
    authenticated_user_id: i32,
    bypass_authz: bool,
) -> Result<Option<RepoLookupRecord>, sqlx::Error> {
    let row = sqlx::query(&format!(
        r#"
SELECT
{REPO_COLUMNS}
FROM repo r
WHERE r.deleted_at IS NULL
  AND r.id = $1
  AND (
	$2
	OR EXISTS (
		SELECT FROM user_repo_permissions
		WHERE repo_id = r.id AND user_id IS NULL
	)
	OR NOT r.private
	OR EXISTS (
		SELECT FROM external_services es
		JOIN external_service_repos esr ON (
			esr.external_service_id = es.id
			AND esr.repo_id = r.id
			AND es.unrestricted = TRUE
			AND es.deleted_at IS NULL
		)
	)
	OR EXISTS (
		SELECT repo_id FROM user_repo_permissions
		WHERE repo_id = r.id AND user_id = $3
	)
  )
LIMIT 1
"#
    ))
    .bind(id)
    .bind(bypass_authz)
    .bind(authenticated_user_id)
    .fetch_optional(pool)
    .await?;

    row.map(record_from_row).transpose()
}

/// The `repo` columns shared by the by-name/URI and by-ID lookups.
const REPO_COLUMNS: &str = r#"
	r.id,
	r.name,
	COALESCE(r.uri, '') AS uri,
	COALESCE(r.description, '') AS description,
	r.fork,
	r.archived,
	r.private,
	r.stars,
	r.created_at,
	r.updated_at,
	COALESCE(r.external_id, '') AS external_id,
	COALESCE(r.external_service_type, '') AS external_service_type,
	COALESCE(r.external_service_id, '') AS external_service_id,
	CASE WHEN r.blocked IS NULL THEN NULL
	     ELSE COALESCE(r.blocked ->> 'reason', '') END AS blocked_reason"#;

fn record_from_row(row: sqlx::postgres::PgRow) -> Result<RepoLookupRecord, sqlx::Error> {
    Ok(RepoLookupRecord {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        uri: row.try_get("uri")?,
        description: row.try_get("description")?,
        fork: row.try_get("fork")?,
        archived: row.try_get("archived")?,
        private: row.try_get("private")?,
        stars: row.try_get("stars")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        external_id: row.try_get("external_id")?,
        external_service_type: row.try_get("external_service_type")?,
        external_service_id: row.try_get("external_service_id")?,
        blocked_reason: row.try_get::<Option<String>, _>("blocked_reason")?,
    })
}
