//! `internal/database/users.go` (`HardDeleteList`, `SetIsSiteAdmin`) and the
//! parts of `internal/database/security_event_logs.go` they call
//! (`LogEventList`/`InsertList`) and `internal/database/event_logs.go`
//! (`BulkInsert`) they feed.
//!
//! Only what the expired Sourcegraph Operator account cleaner needs is
//! ported, but the transaction keeps every statement of `HardDeleteList` —
//! including the anonymisation of the user's `event_logs` rows and the
//! `settings.author_user_id` reset — so a partially deleted user cannot be
//! observed. The security-event store honours the
//! `log.securityEventLog.location` gate: the caller passes whether the
//! `security_event_logs` table is a destination (`database`/`all`); the
//! `auditlog` sink is a log line, not a database write.
//!
//! Baseline behaviour reproduced deliberately: `HardDeleteList` reports a
//! not-found error when fewer rows than requested were deleted, and the
//! caller ignores it (`errcode.IsNotFound`).

use std::fmt;

use serde_json::{json, Value};
use sqlx::postgres::PgConnection;
use sqlx::types::time::{Date, OffsetDateTime};
use sqlx::{PgPool, QueryBuilder, Row};

use crate::telemetry_events::{bulk_insert_event_logs_on, EventLogRow};

/// `auth.SourcegraphOperatorProviderType`.
pub const SOURCEGRAPH_OPERATOR_PROVIDER_TYPE: &str = "sourcegraph-operator";

/// `types.SiteAdministratorSystemRole`.
pub const SITE_ADMINISTRATOR_ROLE: &str = "SITE_ADMINISTRATOR";

/// `entities.SecurityEventNameAccountNuked`.
pub const SECURITY_EVENT_NAME_ACCOUNT_NUKED: &str = "AccountNuked";

/// `database.EventLogsSourcegraphOperatorKey`.
pub const EVENT_LOGS_SOURCEGRAPH_OPERATOR_KEY: &str = "sourcegraph_operator";

/// A database failure with the baseline's not-found classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbError {
    pub message: String,
    /// `errcode.IsNotFound(err)`.
    pub not_found: bool,
}

impl DbError {
    fn database(err: sqlx::Error) -> Self {
        Self {
            message: err.to_string(),
            not_found: false,
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.not_found
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DbError {}

/// `actor.Actor` reduced to the two fields the deletion paths read. The
/// cleaner installs `&actor.Actor{SourcegraphOperator: true}`, whose `UID` is
/// zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendActor {
    pub uid: i32,
    pub sourcegraph_operator: bool,
}

/// The request context and site configuration `logUserDeletionEvents` reads.
pub struct DeletionEventContext<'a> {
    pub actor: BackendActor,
    /// `version.Version()`.
    pub version: &'a str,
    /// Shared by every event of one deletion batch (`now := time.Now()`).
    pub now: OffsetDateTime,
    /// `audit.SecurityEventLocation(cfg)` is `Database` or `All`.
    pub security_event_logs_in_database: bool,
}

/// The baseline `sourcegraphOperatorCleanHandler` query: users with a live
/// Sourcegraph Operator external account older than the cutoff.
pub async fn list_expired_sourcegraph_operator_user_ids(
    pool: &PgPool,
    cutoff: OffsetDateTime,
) -> Result<Vec<i32>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
SELECT user_id
FROM users
JOIN user_external_accounts ON user_external_accounts.user_id = users.id
WHERE
	user_external_accounts.service_type = $1
	AND user_external_accounts.created_at <= $2
	AND user_external_accounts.deleted_at IS NULL
GROUP BY user_id
"#,
    )
    .bind(SOURCEGRAPH_OPERATOR_PROVIDER_TYPE)
    .bind(cutoff)
    .fetch_all(pool)
    .await
}

/// `userStore.HardDeleteList`. The caller ignores a not-found error exactly
/// like `errcode.IsNotFound`.
pub async fn hard_delete_list(
    pool: &PgPool,
    ids: &[i32],
    ctx: &DeletionEventContext<'_>,
) -> Result<(), DbError> {
    if ids.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await.map_err(DbError::database)?;
    let ids_vec = ids.to_vec();

    sqlx::query("DELETE FROM names WHERE user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    sqlx::query(
        "DELETE FROM access_tokens \
         WHERE subject_user_id = ANY($1) OR creator_user_id = ANY($1)",
    )
    .bind(&ids_vec)
    .execute(&mut *tx)
    .await
    .map_err(DbError::database)?;
    sqlx::query("DELETE FROM user_emails WHERE user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    sqlx::query("DELETE FROM user_external_accounts WHERE user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    sqlx::query("DELETE FROM survey_responses WHERE user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    sqlx::query(
        "DELETE FROM registry_extension_releases WHERE registry_extension_id IN \
         (SELECT id FROM registry_extensions WHERE publisher_user_id = ANY($1))",
    )
    .bind(&ids_vec)
    .execute(&mut *tx)
    .await
    .map_err(DbError::database)?;
    sqlx::query("DELETE FROM registry_extensions WHERE publisher_user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    sqlx::query(
        "DELETE FROM org_invitations \
         WHERE sender_user_id = ANY($1) OR recipient_user_id = ANY($1)",
    )
    .bind(&ids_vec)
    .execute(&mut *tx)
    .await
    .map_err(DbError::database)?;
    sqlx::query("DELETE FROM org_members WHERE user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    sqlx::query("DELETE FROM settings WHERE user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    sqlx::query("DELETE FROM saved_searches WHERE user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;

    // Anonymize all entries for the deleted user (`uuid.New().String()` is a
    // fresh value per user).
    for uid in ids {
        sqlx::query("UPDATE event_logs SET user_id = 0, anonymous_user_id = $1 WHERE user_id = $2")
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(uid)
            .execute(&mut *tx)
            .await
            .map_err(DbError::database)?;
    }

    // Settings merely authored by this user may be global or org settings;
    // only the author link is cleared.
    sqlx::query("UPDATE settings SET author_user_id = NULL WHERE author_user_id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;

    let result = sqlx::query("DELETE FROM users WHERE id = ANY($1)")
        .bind(&ids_vec)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    if result.rows_affected() != ids.len() as u64 {
        return Err(DbError {
            message: format!(
                "Some users were not found. Expected to hard delete {} users, but deleted only {}",
                ids.len(),
                result.rows_affected()
            ),
            not_found: true,
        });
    }

    log_user_deletion_events(&mut tx, ids, ctx)
        .await
        .map_err(DbError::database)?;

    tx.commit().await.map_err(DbError::database)?;
    Ok(())
}

/// `logUserDeletionEvents`: one security event and one analytics event per
/// deleted user, inside the deletion transaction.
async fn log_user_deletion_events(
    tx: &mut PgConnection,
    ids: &[i32],
    ctx: &DeletionEventContext<'_>,
) -> Result<(), sqlx::Error> {
    if ctx.security_event_logs_in_database {
        let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO security_event_logs(name, url, user_id, anonymous_user_id, \
             source, argument, version, timestamp) ",
        );
        builder.push_values(ids, |mut builder, id| {
            builder
                .push_bind(SECURITY_EVENT_NAME_ACCOUNT_NUKED)
                .push_bind("")
                .push_bind(*id)
                .push_bind("")
                .push_bind("BACKEND")
                .push_bind(security_event_argument(ctx.actor))
                .push_bind(ctx.version)
                .push_bind(ctx.now);
        });
        builder.build().execute(&mut *tx).await?;
    }

    let rows: Vec<EventLogRow> = ids
        .iter()
        .map(|id| deletion_event_log_row(*id, ctx))
        .collect();
    bulk_insert_event_logs_on(&mut *tx, &rows).await
}

/// `argument` for the account-deletion security event, including the
/// Sourcegraph Operator attribution `jsonc.Edit` adds.
fn security_event_argument(actor: BackendActor) -> Value {
    argument_object(&json!({ "deleter": actor.uid }), actor)
}

fn deletion_event_log_row(id: i32, ctx: &DeletionEventContext<'_>) -> EventLogRow {
    EventLogRow {
        name: SECURITY_EVENT_NAME_ACCOUNT_NUKED.to_owned(),
        url: String::new(),
        user_id: 0,
        anonymous_user_id: "backend".to_owned(),
        source: "BACKEND".to_owned(),
        argument: json!({ "deleter": ctx.actor.uid, "deleted": id }),
        public_argument: argument_object(&json!({}), ctx.actor),
        version: ctx.version.to_owned(),
        timestamp: ctx.now,
        feature_flags: Value::Null,
        cohort_id: None,
        first_source_url: None,
        last_source_url: None,
        referrer: None,
        device_id: uuid::Uuid::new_v4().to_string(),
        insert_id: uuid::Uuid::new_v4().to_string(),
        client: None,
        billing_product_category: None,
        billing_event_id: None,
    }
}

/// `jsonc.Edit(object, true, "sourcegraph_operator")` when the actor is the
/// Sourcegraph Operator; the public argument of a backend event starts as
/// `{}`.
fn argument_object(base: &Value, actor: BackendActor) -> Value {
    if !actor.sourcegraph_operator {
        return base.clone();
    }
    let mut object = base.as_object().cloned().unwrap_or_default();
    object.insert(
        EVENT_LOGS_SOURCEGRAPH_OPERATOR_KEY.to_owned(),
        Value::Bool(true),
    );
    Value::Object(object)
}

/// `userStore.SetIsSiteAdmin`, including the `user_roles` grant/revoke and the
/// `RoleChangeGranted` analytics event of the granting branch.
pub async fn set_is_site_admin(
    pool: &PgPool,
    id: i32,
    is_site_admin: bool,
    ctx: &DeletionEventContext<'_>,
) -> Result<(), DbError> {
    let mut tx = pool.begin().await.map_err(DbError::database)?;

    sqlx::query("UPDATE users SET site_admin = $1 WHERE id = $2")
        .bind(is_site_admin)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;

    if is_site_admin {
        // `AssignSystemRole`: no rows returned means the role was already
        // assigned, which is not an error.
        sqlx::query(
            "INSERT INTO user_roles (user_id, role_id) \
             VALUES ($1, (SELECT id FROM roles WHERE name = $2)) \
             ON CONFLICT DO NOTHING \
             RETURNING user_roles.user_id, user_roles.role_id, user_roles.created_at",
        )
        .bind(id)
        .bind(SITE_ADMINISTRATOR_ROLE)
        .fetch_optional(&mut *tx)
        .await
        .map_err(DbError::database)?;

        let argument = json!({
            "assigner": ctx.actor.uid,
            "assignee": id,
            "role": SITE_ADMINISTRATOR_ROLE,
        });
        let row = EventLogRow {
            name: "RoleChangeGranted".to_owned(),
            url: String::new(),
            user_id: 0,
            anonymous_user_id: "backend".to_owned(),
            source: "BACKEND".to_owned(),
            argument,
            public_argument: argument_object(&json!({}), ctx.actor),
            version: ctx.version.to_owned(),
            timestamp: ctx.now,
            feature_flags: Value::Null,
            cohort_id: None,
            first_source_url: None,
            last_source_url: None,
            referrer: None,
            device_id: uuid::Uuid::new_v4().to_string(),
            insert_id: uuid::Uuid::new_v4().to_string(),
            client: None,
            billing_product_category: None,
            billing_event_id: None,
        };
        // `_ = db.EventLogs().Insert(ctx, logEvent)`: the baseline discards the
        // error, but a failed statement still aborts the transaction.
        let _ = bulk_insert_event_logs_on(&mut *tx, &[row]).await;
    } else {
        // `RevokeSystemRole`
        sqlx::query(
            "DELETE FROM user_roles \
             WHERE user_id = $1 AND role_id = (SELECT id FROM roles WHERE name = $2)",
        )
        .bind(id)
        .bind(SITE_ADMINISTRATOR_ROLE)
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;
    }

    tx.commit().await.map_err(DbError::database)?;
    Ok(())
}

/// Subset of `types.User` used by the user-centric permission sync
/// (`database.Users().GetByID`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRecord {
    pub id: i32,
    pub username: String,
    pub created_at: OffsetDateTime,
}

/// `userStore.GetByID` without the site-admin/role fields: live users only.
/// `Ok(None)` is the baseline's not-found error; callers decide whether that
/// is fatal (the permission syncer wraps it as `get user`).
pub async fn get_by_id(pool: &PgPool, id: i32) -> Result<Option<UserRecord>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT id, username, created_at
FROM users
WHERE id = $1 AND deleted_at IS NULL
LIMIT 1
"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| UserRecord {
        id: row.get("id"),
        username: row.get("username"),
        created_at: row.get("created_at"),
    }))
}

/// `users.site_admin` for a live user (`database.Users().GetByID` narrowed to
/// the site-admin check used by `internal/auth/site_admin.go`). `Ok(None)` is
/// the baseline's not-found error (the caller decides whether that is
/// "not authenticated" or a 500).
pub async fn get_site_admin(pool: &PgPool, id: i32) -> Result<Option<bool>, sqlx::Error> {
    let row =
        sqlx::query("SELECT site_admin FROM users WHERE id = $1 AND deleted_at IS NULL LIMIT 1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|row| row.get("site_admin")))
}

/// The token subject's username for the `AccessTokenImpersonated` audit
/// argument (`database.Users().GetByID`; the baseline ignores lookup errors
/// and leaves the name empty).
pub async fn get_username_by_id(pool: &PgPool, id: i32) -> Result<Option<String>, sqlx::Error> {
    let row =
        sqlx::query("SELECT username FROM users WHERE id = $1 AND deleted_at IS NULL LIMIT 1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|row| row.get("username")))
}

/// `database.Users().GetByUsername` reduced to the two fields sudo needs.
/// `Ok(None)` is the baseline's not-found error (`errcode.IsNotFound`).
pub async fn lookup_by_username(
    pool: &PgPool,
    username: &str,
) -> Result<Option<(i32, String)>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT id, username FROM users WHERE username = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(username)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| (row.get("id"), row.get("username"))))
}

/// The `users` fields session authentication and the DB-backed `User`
/// resolvers read (`internal/database/users.go:1330-1360`).
/// `display_name`/`avatar_url` are nullable columns; the baseline's
/// `types.User` fields are plain strings, so NULL maps to the empty string
/// (which the resolvers turn back into null). `email` is the primary
/// `user_emails.email` (`userEmailsStore.GetPrimaryEmail`), empty when the
/// user has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSessionRecord {
    pub id: i32,
    pub username: String,
    pub display_name: String,
    pub avatar_url: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub invalidated_sessions_at: OffsetDateTime,
    pub site_admin: bool,
    pub tos_accepted: bool,
    pub email: String,
}

fn scan_user_session_row(row: &sqlx::postgres::PgRow) -> UserSessionRecord {
    UserSessionRecord {
        id: row.get("id"),
        username: row.get("username"),
        display_name: row
            .get::<Option<String>, _>("display_name")
            .unwrap_or_default(),
        avatar_url: row
            .get::<Option<String>, _>("avatar_url")
            .unwrap_or_default(),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        invalidated_sessions_at: row.get("invalidated_sessions_at"),
        site_admin: row.get("site_admin"),
        tos_accepted: row.get("tos_accepted"),
        email: row.get("email"),
    }
}

/// `userStore.GetByID`: live users only (`deleted_at IS NULL`). `Ok(None)` is
/// the baseline's not-found error.
pub async fn get_session_user(
    pool: &PgPool,
    id: i32,
) -> Result<Option<UserSessionRecord>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT u.id, u.username, u.display_name, u.avatar_url, u.created_at,
       u.updated_at, u.invalidated_sessions_at, u.site_admin, u.tos_accepted,
       COALESCE(ue.email, '') AS email
FROM users u
LEFT JOIN user_emails ue ON (ue.user_id = u.id AND ue.is_primary)
WHERE u.id = $1 AND u.deleted_at IS NULL
LIMIT 1
"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(scan_user_session_row))
}

/// `userStore.InvalidateSessionsByIDs` (`internal/database/users.go:1026-1060`):
/// one transaction, `updated_at` and `invalidated_sessions_at` both bumped by
/// the database clock. Fewer affected rows than requested is the baseline's
/// `userNotFoundErr` (`errcode.IsNotFound`); the transaction is rolled back.
pub async fn invalidate_sessions_by_ids(pool: &PgPool, ids: &[i32]) -> Result<(), DbError> {
    // The baseline would render `IN ()` (a SQL error) for an empty list; no
    // caller does that (`InvalidateSessionCurrentUser` always passes one ID).
    if ids.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await.map_err(DbError::database)?;
    let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
        "UPDATE users SET updated_at=now(), invalidated_sessions_at=now() WHERE id IN (",
    );
    {
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(")");
    }
    let result = builder
        .build()
        .execute(&mut *tx)
        .await
        .map_err(DbError::database)?;

    let invalidated = result.rows_affected();
    if invalidated != ids.len() as u64 {
        let _ = tx.rollback().await;
        return Err(DbError {
            message: format!(
                "user not found: [Some users were not found. Expected to invalidate sessions of {} users, but invalidated sessions only {}]",
                ids.len(),
                invalidated
            ),
            not_found: true,
        });
    }

    tx.commit().await.map_err(DbError::database)?;
    Ok(())
}

/// `userStore.GetByUsername` (`internal/database/users.go:996-998`):
/// `username` is citext, so the comparison is case-insensitive.
pub async fn find_user_by_username(
    pool: &PgPool,
    username: &str,
) -> Result<Option<UserSessionRecord>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT u.id, u.username, u.display_name, u.avatar_url, u.created_at,
       u.updated_at, u.invalidated_sessions_at, u.site_admin, u.tos_accepted,
       COALESCE(ue.email, '') AS email
FROM users u
LEFT JOIN user_emails ue ON (ue.user_id = u.id AND ue.is_primary)
WHERE u.username = $1::citext AND u.deleted_at IS NULL
LIMIT 1
"#,
    )
    .bind(username)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(scan_user_session_row))
}

/// `userStore.GetByVerifiedEmail` with the full `types.User` row: unverified
/// matches are ignored; at most one user has a given verified address.
pub async fn find_user_by_verified_email(
    pool: &PgPool,
    email: &str,
) -> Result<Option<UserSessionRecord>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT u.id, u.username, u.display_name, u.avatar_url, u.created_at,
       u.updated_at, u.invalidated_sessions_at, u.site_admin, u.tos_accepted,
       COALESCE(ue.email, '') AS email
FROM users u
LEFT JOIN user_emails ue ON (ue.user_id = u.id AND ue.is_primary)
WHERE u.id = (SELECT user_id FROM user_emails WHERE email = $1::citext AND verified_at IS NOT NULL)
  AND u.deleted_at IS NULL
LIMIT 1
"#,
    )
    .bind(email)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(scan_user_session_row))
}

/// `database.UsersListOptions` as `Query.users` fills it
/// (`cmd/frontend/graphqlbackend/users.go:33-49`). `include_deleted` is not
/// reachable from that resolver; `org_id` is set by `Org.members`'s
/// `Count` (`org.go:129-141`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsersListOptions {
    /// `Query`: `username ILIKE %q% OR display_name ILIKE %q% OR id = n`.
    pub query: Option<String>,
    /// `database.maybeQueryIsID(query)`, precomputed by the caller: sg-db does
    /// not depend on `base64`. Only consulted when `query` is set.
    pub query_id: Option<i32>,
    /// `UserIDs`; an empty list must return no rows (baseline `FALSE`).
    pub user_ids: Option<Vec<i32>>,
    /// `OrgID`: only users with an `org_members` row for this organization.
    pub org_id: Option<i32>,
    /// `InactiveSince`: users without a newer `event_logs` row.
    pub inactive_since: Option<OffsetDateTime>,
    /// `ExcludeSourcegraphOperators`.
    pub exclude_sourcegraph_operators: bool,
    /// `LimitOffset.SQL()`: `Some((limit, offset))` emits
    /// `LIMIT ... OFFSET ...`; `None` emits nothing.
    pub limit_offset: Option<(i32, i32)>,
}

fn push_users_where(builder: &mut QueryBuilder<'_, sqlx::Postgres>, opt: &UsersListOptions) {
    builder.push("u.deleted_at IS NULL");
    if let Some(query) = &opt.query {
        // `newQueryCond` ignores an empty query.
        if !query.is_empty() {
            let pattern = format!("%{query}%");
            builder
                .push(" AND (u.username ILIKE ")
                .push_bind(pattern.clone())
                .push(" OR u.display_name ILIKE ")
                .push_bind(pattern);
            if let Some(id) = opt.query_id {
                builder.push(" OR u.id = ").push_bind(id);
            }
            builder.push(")");
        }
    }
    if let Some(user_ids) = &opt.user_ids {
        if user_ids.is_empty() {
            builder.push(" AND FALSE");
        } else {
            builder.push(" AND u.id = ANY(").push_bind(user_ids.clone());
            builder.push(")");
        }
    }
    if let Some(org_id) = opt.org_id {
        // `orgMembershipCond` (`internal/database/users.go:1199-1216`); the
        // baseline only adds it when `OrgID != 0`.
        if org_id != 0 {
            builder
                .push(
                    " AND EXISTS (SELECT 1 FROM org_members WHERE org_members.user_id = \
                     u.id AND org_members.org_id = ",
                )
                .push_bind(org_id)
                .push(")");
        }
    }
    if let Some(inactive_since) = opt.inactive_since {
        builder
            .push(
                " AND NOT EXISTS (SELECT 1 FROM event_logs WHERE event_logs.user_id = u.id \
                 AND event_logs.timestamp >= ",
            )
            .push_bind(inactive_since)
            .push(")");
    }
    if opt.exclude_sourcegraph_operators {
        builder
            .push(" AND NOT EXISTS (SELECT FROM user_external_accounts WHERE service_type = ")
            .push_bind(SOURCEGRAPH_OPERATOR_PROVIDER_TYPE)
            .push(" AND user_id = u.id)");
    }
}

/// `userStore.List` (`internal/database/users.go:1116-1127`): the `listSQL`
/// conditions, `ORDER BY id ASC` and the optional limit/offset.
pub async fn list_users(
    pool: &PgPool,
    opt: &UsersListOptions,
) -> Result<Vec<UserSessionRecord>, sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
        "SELECT u.id, u.username, u.display_name, u.avatar_url, u.created_at, \
         u.updated_at, u.invalidated_sessions_at, u.site_admin, u.tos_accepted, \
         COALESCE(ue.email, '') AS email \
         FROM users u \
         LEFT JOIN user_emails ue ON (ue.user_id = u.id AND ue.is_primary) \
         WHERE ",
    );
    push_users_where(&mut builder, opt);
    builder.push(" ORDER BY u.id ASC");
    if let Some((limit, offset)) = opt.limit_offset {
        builder.push(" LIMIT ").push_bind(limit);
        builder.push(" OFFSET ").push_bind(offset);
    }
    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows.iter().map(scan_user_session_row).collect())
}

/// `userStore.Count` (`internal/database/users.go:1070-1082`): the same
/// conditions without ordering or pagination.
pub async fn count_users(pool: &PgPool, opt: &UsersListOptions) -> Result<i64, sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> =
        QueryBuilder::new("SELECT COUNT(*) FROM users u WHERE ");
    push_users_where(&mut builder, opt);
    let row = builder.build().fetch_one(pool).await?;
    Ok(row.get::<i64, _>(0))
}

/// `database.PaginationArgs` as `membersConnectionStore.ComputeNodes` fills it
/// (`internal/database/helpers.go:134-250`). The members resolver always
/// orders by `id`; `first`/`last` already include the `+1` over-fetch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsersPaginationArgs {
    /// `PaginationArgs.First` (SQL `LIMIT`, `pageSize + 1`).
    pub first: Option<i32>,
    /// `PaginationArgs.Last` (SQL `LIMIT`, `pageSize + 1`; reversed order).
    pub last: Option<i32>,
    /// `PaginationArgs.After` (already unmarshalled cursor value).
    pub after: Option<i32>,
    /// `PaginationArgs.Before` (already unmarshalled cursor value).
    pub before: Option<i32>,
    /// `PaginationArgs.Ascending`; `false` for `Org.members`.
    pub ascending: bool,
}

/// `userStore.ListByOrg` (`internal/database/users.go:1170-1190`): the
/// `orgMembershipCond`, `u.deleted_at IS NULL`, the query conditions and the
/// cursor conditions, then `PaginationArgs.SQL()`'s ordering and `LIMIT`.
pub async fn list_users_by_org(
    pool: &PgPool,
    org_id: i32,
    query: Option<&str>,
    query_id: Option<i32>,
    pagination: &UsersPaginationArgs,
) -> Result<Vec<UserSessionRecord>, sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
        "SELECT u.id, u.username, u.display_name, u.avatar_url, u.created_at, \
         u.updated_at, u.invalidated_sessions_at, u.site_admin, u.tos_accepted, \
         COALESCE(ue.email, '') AS email \
         FROM users u \
         LEFT JOIN user_emails ue ON (ue.user_id = u.id AND ue.is_primary) \
         WHERE EXISTS (SELECT 1 FROM org_members WHERE org_members.user_id = u.id \
         AND org_members.org_id = ",
    );
    builder.push_bind(org_id);
    builder.push(") AND u.deleted_at IS NULL");
    // `newQueryCond` ignores `nil`/empty queries.
    if let Some(query) = query {
        if !query.is_empty() {
            let pattern = format!("%{query}%");
            builder
                .push(" AND (u.username ILIKE ")
                .push_bind(pattern.clone())
                .push(" OR u.display_name ILIKE ")
                .push_bind(pattern);
            if let Some(id) = query_id {
                builder.push(" OR u.id = ").push_bind(id);
            }
            builder.push(")");
        }
    }
    // `PaginationArgs.SQL()`: tuple comparisons against the (single) `id`
    // order column. `Ascending` is false for `Org.members`, so `after` uses
    // `>` and `before` uses `<`; both flip when ascending.
    if let Some(after) = pagination.after {
        builder
            .push(if pagination.ascending {
                " AND (u.id) > ("
            } else {
                " AND (u.id) < ("
            })
            .push_bind(after)
            .push(")");
    }
    if let Some(before) = pagination.before {
        builder
            .push(if pagination.ascending {
                " AND (u.id) < ("
            } else {
                " AND (u.id) > ("
            })
            .push_bind(before)
            .push(")");
    }
    let limit = if pagination.first.is_some() {
        pagination.first
    } else if pagination.last.is_some() {
        pagination.last
    } else {
        None
    };
    // `first` → `OrderBy.Ascending`; `last` → the inverse; neither → the
    // configured direction (with no `LIMIT`).
    let ascending = match (pagination.first.is_some(), pagination.last.is_some()) {
        (true, _) => pagination.ascending,
        (false, true) => !pagination.ascending,
        (false, false) => pagination.ascending,
    };
    builder.push(if ascending {
        " ORDER BY u.id ASC"
    } else {
        " ORDER BY u.id DESC"
    });
    if let Some(limit) = limit {
        builder.push(" LIMIT ").push_bind(limit);
    }
    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows.iter().map(scan_user_session_row).collect())
}

/// `eventLogStore.ListUniqueUsersAll` (`internal/database/event_logs.go:875-897`):
/// distinct positive user IDs with an event whose UTC date falls in the
/// inclusive `[start, end]` range. The baseline passes dates as `time.Time`
/// midnights; binding `DATE` keeps the `DATE(...)` comparison identical.
pub async fn list_unique_users_between(
    pool: &PgPool,
    start: Date,
    end: Date,
) -> Result<Vec<i32>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT user_id FROM event_logs \
         WHERE user_id > 0 \
           AND DATE(TIMEZONE('UTC', timestamp)) >= $1 \
           AND DATE(TIMEZONE('UTC', timestamp)) <= $2 \
         GROUP BY user_id",
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
}

/// Subset of `types.User` used by `stream_blame.go`'s
/// `BlameHunkUserResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserProfile {
    pub id: i32,
    pub username: String,
    pub display_name: String,
    pub avatar_url: String,
}

/// `userStore.GetByVerifiedEmail`
/// (`internal/database/users.go:989-994`): the user with the specified
/// verified email address, if any. Users whose matching email is unverified
/// are not returned; at most one user may have a given verified address.
pub async fn get_by_verified_email(
    pool: &PgPool,
    email: &str,
) -> Result<Option<UserProfile>, sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT u.id, u.username, u.display_name, u.avatar_url
FROM users u
WHERE u.id = (SELECT user_id FROM user_emails WHERE email = $1::citext AND verified_at IS NOT NULL)
  AND u.deleted_at IS NULL
LIMIT 1
"#,
    )
    .bind(email)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| UserProfile {
        id: row.get("id"),
        username: row.get("username"),
        display_name: row.get("display_name"),
        avatar_url: row.get("avatar_url"),
    }))
}
