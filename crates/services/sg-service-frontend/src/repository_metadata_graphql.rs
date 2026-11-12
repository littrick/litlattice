#![forbid(unsafe_code)]

//! `Mutation.setUserCompletionsQuota` / `setUserCodeCompletionsQuota`
//! (`cmd/frontend/graphqlbackend/user.go:835/891`) and
//! `Mutation.setAutoUpgrade`
//! (`cmd/frontend/graphqlbackend/site.go:585`,
//! `internal/version/upgradestore/store.go:177-188`).
//!
//! Repo metadata (`addRepoMetadata`/`addRepoKeyValuePair` and friends,
//! `repository_metadata.go:45-183`) needs the RBAC `REPO_METADATA#WRITE`
//! permission check and the `repo_kvps` store; that part is not ported in this
//! slice (recorded in the overview). This module keeps the quota/version
//! mutations that only need the existing `users`/`versions` tables.

use sg_graphql::exec::{Actor, RootField};
use sqlx::PgPool;
use std::sync::Arc;

use crate::admin_store::check_current_user_is_site_admin;
use crate::graphql_resolvers::{unmarshal_relay_id, UserData, UserObject};

#[derive(Debug, Clone)]
pub enum InventoryMutationOutcome {
    /// `EmptyResponse` (`alwaysNil: null`).
    Empty,
    /// The `users` row returned by `UserByIDInt32` after a quota update.
    User(Arc<UserData>),
    Error(String),
}

/// Execute the inventory admin mutations in document order.
pub async fn prefetch_inventory_admin_mutations(
    pool: &PgPool,
    users: Option<&dyn crate::graphql_resolvers::UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    auditlog_expansion: bool,
) -> Vec<(String, InventoryMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "setUserCompletionsQuota" => {
                set_quota(
                    pool,
                    users,
                    field,
                    actor,
                    viewer_site_admin,
                    true,
                    auditlog_expansion,
                )
                .await
            }
            "setUserCodeCompletionsQuota" => {
                set_quota(
                    pool,
                    users,
                    field,
                    actor,
                    viewer_site_admin,
                    false,
                    auditlog_expansion,
                )
                .await
            }
            "setAutoUpgrade" => set_auto_upgrade(pool, field, actor, viewer_site_admin).await,
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

#[allow(clippy::too_many_arguments)]
async fn set_quota(
    pool: &PgPool,
    users: Option<&dyn crate::graphql_resolvers::UserLookupStore>,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    chat: bool,
    auditlog_expansion: bool,
) -> InventoryMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryMutationOutcome::Error(message);
    }
    let quota = field.arguments.i64("quota");
    if let Some(quota) = quota {
        if quota <= 0 {
            return InventoryMutationOutcome::Error("quota must be 1 or greater".to_owned());
        }
    }
    // `UnmarshalUserID` requires the relay kind to be `User`.
    let Some(user_id) = field
        .arguments
        .string("user")
        .filter(|id| {
            matches!(
                crate::graphql_resolvers::relay_kind(id).as_deref(),
                Some("User")
            )
        })
        .and_then(|id| unmarshal_relay_id(id).ok())
    else {
        return InventoryMutationOutcome::Error("invalid graphql.ID".to_owned());
    };
    // `Users().GetByID` verifies the row exists before the update.
    let exists: Option<(i32,)> =
        match sqlx::query_as("SELECT id FROM users WHERE id = $1 AND deleted_at IS NULL")
            .bind(user_id)
            .fetch_optional(pool)
            .await
        {
            Ok(row) => row,
            Err(error) => return InventoryMutationOutcome::Error(error.to_string()),
        };
    if exists.is_none() {
        return InventoryMutationOutcome::Error(format!("user not found: id {user_id}"));
    }
    let column = if chat {
        "completions_quota"
    } else {
        "code_completions_quota"
    };
    let query = format!("UPDATE users SET {column} = $2 WHERE id = $1");
    if let Err(error) = sqlx::query(&query)
        .bind(user_id)
        .bind(quota.map(|quota| quota as i32))
        .execute(pool)
        .await
    {
        return InventoryMutationOutcome::Error(error.to_string());
    }
    // `auditlog-expansion` gates both quota events (`user.go:878,923`);
    // `argument = SetUser{Code,}CompletionsQuotaArgs{User, Quota}` and
    // `user_id` is the target user, not the requesting actor.
    if auditlog_expansion {
        let name = if chat {
            crate::security_event_logs::SECURITY_EVENT_USER_COMPLETION_QUOTA_UPDATED
        } else {
            crate::security_event_logs::SECURITY_EVENT_USER_CODE_COMPLETION_QUOTA_UPDATED
        };
        crate::security_event_logs::write_backend_security_event(
            pool,
            name,
            user_id,
            serde_json::json!({
                "User": field.arguments.string("user"),
                "Quota": quota,
            }),
        )
        .await;
    }
    // `UserByIDInt32` reloads the row (with the primary email).
    match users {
        Some(store) => match store
            .lookup(&crate::graphql_resolvers::UserKey::DatabaseId(user_id))
            .await
        {
            Ok(Some(user)) => InventoryMutationOutcome::User(Arc::new(user)),
            Ok(None) => InventoryMutationOutcome::Error(format!("user not found: id {user_id}")),
            Err(message) => InventoryMutationOutcome::Error(message),
        },
        None => InventoryMutationOutcome::Error(
            "no user lookup store available for the quota response".to_owned(),
        ),
    }
}

async fn set_auto_upgrade(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryMutationOutcome::Error(message);
    }
    let Some(enable) = field.arguments.bool("enable") else {
        return InventoryMutationOutcome::Error("missing enable argument".to_owned());
    };
    match sqlx::query("UPDATE versions SET auto_upgrade = $1")
        .bind(enable)
        .execute(pool)
        .await
    {
        Ok(_) => InventoryMutationOutcome::Empty,
        Err(error) => InventoryMutationOutcome::Error(error.to_string()),
    }
}

/// `InventoryMutationOutcome::User` as a `User` response object.
pub fn user_object(data: &UserData, viewer_site_admin: bool) -> UserObject {
    UserObject::from_data_as_viewer(data.clone(), viewer_site_admin)
}
