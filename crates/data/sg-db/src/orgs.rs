#![forbid(unsafe_code)]

//! `internal/database/orgs.go` (`GetByID`, `GetByName`, `GetByUserID`, `List`,
//! `Count`) and `internal/database/org_members.go` (`GetByOrgIDAndUserID`,
//! `GetByUserID`) for the organization resolvers.
//!
//! Only the read paths the GraphQL resolvers need are ported; `orgs.name` is
//! citext, so `GetByName` binds `$1::citext` (sqlx sends TEXT, and
//! `citext = text` would degrade to a case-sensitive comparison).

use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, QueryBuilder, Row};

/// One `orgs` row (`internal/types/types.go:947-953`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgRecord {
    pub id: i32,
    pub name: String,
    /// `NULL` stays `None`; the GraphQL `displayName` is nullable, while an
    /// empty string is returned as-is.
    pub display_name: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

fn scan_org_row(row: &sqlx::postgres::PgRow) -> OrgRecord {
    OrgRecord {
        id: row.get("id"),
        name: row.get("name"),
        display_name: row.get("display_name"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

/// `orgStore.GetByName` (`internal/database/orgs.go:114-123`): live
/// organizations only; `Ok(None)` is the baseline `OrgNotFoundError`.
pub async fn find_org_by_name(pool: &PgPool, name: &str) -> Result<Option<OrgRecord>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT id, name, display_name, created_at, updated_at
FROM orgs
WHERE deleted_at IS NULL AND name = $1::citext
LIMIT 1
"#,
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(scan_org_row))
}

/// `orgStore.GetByID` (`internal/database/orgs.go:103-112`): live
/// organizations only; `Ok(None)` is the baseline `OrgNotFoundError`.
pub async fn find_org_by_id(pool: &PgPool, id: i32) -> Result<Option<OrgRecord>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT id, name, display_name, created_at, updated_at
FROM orgs
WHERE deleted_at IS NULL AND id = $1
LIMIT 1
"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(scan_org_row))
}

/// `orgStore.GetByUserID` (`internal/database/orgs.go:73-102`): the baseline
/// query has no `ORDER BY`, so the row order is whatever PostgreSQL returns.
pub async fn orgs_for_user(pool: &PgPool, user_id: i32) -> Result<Vec<OrgRecord>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
SELECT orgs.id, orgs.name, orgs.display_name, orgs.created_at, orgs.updated_at
FROM org_members
LEFT OUTER JOIN orgs ON org_members.org_id = orgs.id
WHERE user_id = $1
  AND orgs.deleted_at IS NULL
"#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(scan_org_row).collect())
}

/// `database.OrgsListOptions` (`internal/database/orgs.go:136-141`). An empty
/// `query` does not filter (the baseline checks `opt.Query != ""`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrgsListOptions {
    pub query: String,
    /// `LimitOffset`: `None` means unpaginated.
    pub limit_offset: Option<(i32, i32)>,
}

/// `(*orgStore).listSQL` (`internal/database/orgs.go:151-158`): the ILIKE
/// patterns are not escaped, matching the baseline.
fn push_orgs_where(builder: &mut QueryBuilder<'_, sqlx::Postgres>, opt: &OrgsListOptions) {
    builder.push("deleted_at IS NULL");
    if !opt.query.is_empty() {
        let pattern = format!("%{}%", opt.query);
        builder
            .push(" AND (name ILIKE ")
            .push_bind(pattern.clone())
            .push(" OR display_name ILIKE ")
            .push_bind(pattern)
            .push(")");
    }
}

/// `orgStore.List` (`internal/database/orgs.go:143-149`): `ORDER BY id ASC`
/// plus the optional `LIMIT/OFFSET`.
pub async fn list_orgs(
    pool: &PgPool,
    opt: &OrgsListOptions,
) -> Result<Vec<OrgRecord>, sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> =
        QueryBuilder::new("SELECT id, name, display_name, created_at, updated_at FROM orgs WHERE ");
    push_orgs_where(&mut builder, opt);
    builder.push(" ORDER BY id ASC");
    if let Some((limit, offset)) = opt.limit_offset {
        builder.push(" LIMIT ").push_bind(limit);
        builder.push(" OFFSET ").push_bind(offset);
    }
    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows.iter().map(scan_org_row).collect())
}

/// `orgStore.Count` (`internal/database/orgs.go:125-134`): the same predicate,
/// ignoring `LimitOffset` (as the baseline does).
pub async fn count_orgs(pool: &PgPool, opt: &OrgsListOptions) -> Result<i64, sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> =
        QueryBuilder::new("SELECT COUNT(*) FROM orgs WHERE ");
    push_orgs_where(&mut builder, opt);
    let row = builder.build().fetch_one(pool).await?;
    Ok(row.get::<i64, _>(0))
}

/// `orgMembersStore.GetByOrgIDAndUserID` existence for a set of organizations
/// (`internal/database/org_members.go:70`). The baseline queries one
/// organization at a time; a single `ANY($2)` returns the same membership set.
pub async fn org_members_for_user(
    pool: &PgPool,
    user_id: i32,
    org_ids: &[i32],
) -> Result<Vec<i32>, sqlx::Error> {
    if org_ids.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar("SELECT org_id FROM org_members WHERE user_id = $1 AND org_id = ANY($2)")
        .bind(user_id)
        .bind(org_ids.to_vec())
        .fetch_all(pool)
        .await
}

/// One `org_members` row (`internal/types/types.go:955-961`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgMembershipRecord {
    pub id: i32,
    pub org_id: i32,
    pub user_id: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `orgMembersStore.GetByUserID` (`internal/database/org_members.go:66-68`):
/// memberships of live users; the baseline has **no** `ORDER BY`, so rows come
/// back in whatever order PostgreSQL returns.
pub async fn org_memberships_for_user(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<OrgMembershipRecord>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
SELECT org_members.id, org_members.org_id, org_members.user_id,
       org_members.created_at, org_members.updated_at
FROM org_members
INNER JOIN users ON org_members.user_id = users.id
WHERE org_members.user_id = $1 AND users.deleted_at IS NULL
"#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|row| OrgMembershipRecord {
            id: row.get("id"),
            org_id: row.get("org_id"),
            user_id: row.get("user_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect())
}
