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
use sqlx::types::time::OffsetDateTime;
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
