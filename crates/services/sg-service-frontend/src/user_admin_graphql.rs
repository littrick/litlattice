#![forbid(unsafe_code)]

//! `Mutation.createUser` / `updateUser` / `deleteUser` / `deleteUsers`
//! (`cmd/frontend/graphqlbackend/users_create.go:16-111`,
//! `user.go:424-498`, `site_admin.go:60-189`).
//!
//! The executor is synchronous, so these async write paths run in
//! [`prefetch_user_admin_mutations`] and `MutationRoot` replays the outcome.
//! Every error string and authorization gate mirrors the baseline; database
//! and e-mail/audit side effects go through the [`AdminStore`] /
//! [`AdminEffects`] seams.

use std::sync::Arc;

use sg_graphql::exec::{Actor, RootField};

use crate::admin_store::{
    check_current_user_is_site_admin, check_name_allowed_for_user_or_organization,
    check_site_admin_or_same_user, AdminEffects, AdminMutationOutcome, AdminStore, NewUser,
    UserUpdate,
};
use crate::graphql_resolvers::unmarshal_relay_id;

/// Run every user-admin mutation root field in document order.
pub async fn prefetch_user_admin_mutations(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, AdminMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "createUser" => create_user(store, effects, field, actor, viewer_site_admin).await,
            "updateUser" => update_user(store, effects, field, actor, viewer_site_admin).await,
            "deleteUser" => delete_users(store, effects, field, actor, viewer_site_admin).await,
            "deleteUsers" => delete_users(store, effects, field, actor, viewer_site_admin).await,
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

async fn create_user(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(username) = field.arguments.string("username") else {
        return AdminMutationOutcome::Error("missing username argument".to_owned());
    };
    let email = field.arguments.string("email").unwrap_or("").to_owned();

    // `needsEmailVerification = email != "" && conf.CanSendEmail() &&
    // userpasswd.ResetPasswordEnabled()`, forced false when `verifiedEmail` is
    // absent or true.
    let mut needs_email_verification =
        !email.is_empty() && effects.can_send_email() && effects.reset_password_enabled();
    match field.arguments.bool("verifiedEmail") {
        None => needs_email_verification = false,
        Some(true) => needs_email_verification = false,
        Some(false) => {}
    }

    let email_verification_code = if needs_email_verification {
        match effects.make_email_verification_code() {
            Ok(code) => Some(code),
            Err(error) => {
                return AdminMutationOutcome::Error(format!(
                    "failed to generate email verification code: {error}"
                ))
            }
        }
    } else {
        None
    };

    let new = NewUser {
        username: username.to_owned(),
        password: String::new(),
        email: email.clone(),
        email_is_verified: !needs_email_verification,
        email_verification_code: email_verification_code.clone(),
    };
    let user = match store.create_user_as(&new, actor.uid().unwrap_or(0)).await {
        Ok(user) => user,
        Err(error) => return AdminMutationOutcome::Error(format!("failed to create user: {error}")),
    };

    // `Authz().GrantPendingPermissions`: failures are logged only.
    let _ = store.grant_pending_permissions(user.id).await;

    // `createUserResult.resetPasswordURL` is resolved lazily by the baseline;
    // only perform its side effects when the field is selected.
    let wants_reset_url = field
        .selections
        .iter()
        .any(|selection| selection.name == "resetPasswordURL");
    let reset_password_url = if wants_reset_url {
        match effects
            .reset_password_url(&user, &email, !needs_email_verification)
            .await
        {
            Ok(url) => url,
            Err(error) => return AdminMutationOutcome::Error(error),
        }
    } else {
        None
    };

    AdminMutationOutcome::CreateUser {
        user: Arc::new(user),
        reset_password_url,
    }
}

async fn update_user(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let Some(raw_id) = field.arguments.string("user") else {
        return AdminMutationOutcome::Error("missing user argument".to_owned());
    };
    let user_id = match unmarshal_relay_id(raw_id) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, user_id) {
        return AdminMutationOutcome::Error(message);
    }

    let username = field.arguments.string("username").map(str::to_owned);
    if let Some(username) = &username {
        if let Err(message) =
            check_name_allowed_for_user_or_organization(effects.dotcom(), username)
        {
            return AdminMutationOutcome::Error(message);
        }
    }

    let avatar_url = field.arguments.string("avatarURL").map(str::to_owned);
    if let Some(avatar) = &avatar_url {
        if !avatar.is_empty() {
            if avatar.len() > 3000 {
                return AdminMutationOutcome::Error(
                    "avatar URL exceeded 3000 characters".to_owned(),
                );
            }
            if let Err(message) = check_avatar_url(avatar) {
                return AdminMutationOutcome::Error(message);
            }
        }
    }

    let user = match store.get_user_by_id(user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!(
                "getting user from the database: user not found: [{user_id}]"
            ))
        }
        Err(error) => {
            return AdminMutationOutcome::Error(format!(
                "getting user from the database: {error}"
            ))
        }
    };

    let username_changed = username.as_deref().is_some_and(|name| name != user.username);
    let display_name = field.arguments.string("displayName").map(str::to_owned);

    if user.scim_controlled {
        let scim_error = "cannot update externally managed user";
        if username_changed {
            return AdminMutationOutcome::Error(scim_error.to_owned());
        }
        if let Some(display_name) = &display_name {
            if *display_name != user.display_name {
                return AdminMutationOutcome::Error(scim_error.to_owned());
            }
        }
    }

    if username_changed {
        // `viewerCanChangeUsername`: same-user/site-admin passed above; with
        // `auth.enableUsernameChanges` false only site admins may change it.
        if !effects.auth_enable_username_changes() && !viewer_site_admin {
            return AdminMutationOutcome::Error(
                "unable to change username because auth.enableUsernameChanges is false in site configuration"
                    .to_owned(),
            );
        }
    }

    let audit_username = username.clone();
    let audit_display_name = display_name.clone();
    let audit_avatar_url = avatar_url.clone();
    let update = UserUpdate {
        username: if username_changed { username } else { None },
        display_name: display_name.map(|value| {
            if value.is_empty() {
                None
            } else {
                Some(value)
            }
        }),
        avatar_url: avatar_url.map(|value| {
            if value.is_empty() {
                None
            } else {
                Some(value)
            }
        }),
    };
    match store.update_user(user_id, &update).await {
        Ok(user) => {
            if effects.auditlog_expansion() {
                // Baseline passes the raw `updateUserArgs` struct (not the
                // derived `UserUpdate`), so `Username` is the argument as
                // received whether or not it actually changed.
                let arguments = serde_json::json!({
                    "User": raw_id,
                    "Username": audit_username,
                    "DisplayName": audit_display_name,
                    "AvatarURL": audit_avatar_url,
                });
                effects.audit("AccountModified", user_id, arguments).await;
            }
            AdminMutationOutcome::User(Arc::new(user))
        }
        Err(error) => AdminMutationOutcome::Error(error),
    }
}

async fn delete_users(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }

    let raw_ids: Vec<String> = if field.name == "deleteUser" {
        match field.arguments.string("user") {
            Some(id) => vec![id.to_owned()],
            None => return AdminMutationOutcome::Error("missing user argument".to_owned()),
        }
    } else {
        match field.arguments.get("users") {
            Some(sg_graphql::exec::InputValue::List(items)) => items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        }
    };
    if raw_ids.is_empty() {
        return AdminMutationOutcome::Error("must specify at least one user ID".to_owned());
    }

    let mut ids = Vec::with_capacity(raw_ids.len());
    for raw in &raw_ids {
        match unmarshal_relay_id(raw) {
            Ok(id) => {
                if actor.uid() == Some(id) {
                    return AdminMutationOutcome::Error(
                        "unable to delete current user".to_owned(),
                    );
                }
                ids.push(id);
            }
            Err(error) => return AdminMutationOutcome::Error(error),
        }
    }

    let users = match store.get_users_by_ids(&ids).await {
        Ok(users) => users,
        Err(error) => {
            return AdminMutationOutcome::Error(format!("list users by IDs: {error}"))
        }
    };

    // SOAP protection: a non-SOAP actor can never delete a SOAP user. The
    // actor in this process is never a Sourcegraph Operator (the baseline
    // reads `actor.SourcegraphOperator`).
    for user in &users {
        let accounts = match store.external_accounts_for_user(user.id).await {
            Ok(accounts) => accounts,
            Err(error) => {
                return AdminMutationOutcome::Error(format!("list external accounts: {error}"))
            }
        };
        if accounts
            .iter()
            .any(|account| account.service_type == "sourcegraph-operator")
        {
            return AdminMutationOutcome::Error(format!(
                "\"sourcegraph-operator\" user {} cannot be deleted by a non-\"sourcegraph-operator\" user",
                user.id
            ));
        }
    }

    let hard = field.arguments.bool("hard").unwrap_or(false);
    if let Err(error) = store
        .delete_users_as(&ids, hard, actor.uid().unwrap_or(0))
        .await
    {
        return AdminMutationOutcome::Error(error);
    }
    // The baseline builds one revoke argument per *existing* user (ids that
    // were not found are silently skipped), so only those are revoked.
    let existing_ids: Vec<i32> = users.iter().map(|user| user.id).collect();
    if let Err(error) = store.revoke_user_permissions(&existing_ids).await {
        return AdminMutationOutcome::Error(error);
    }
    let _ = effects;
    AdminMutationOutcome::Empty
}

/// `url.Parse` + scheme check (`user.go:440-451`).
fn check_avatar_url(raw: &str) -> Result<(), String> {
    // Go's `url.Parse` accepts relative references, so a missing scheme falls
    // through to the scheme check rather than the parse-error branch.
    match url::Url::parse(raw) {
        Ok(parsed) => {
            if parsed.scheme() != "http" && parsed.scheme() != "https" {
                Err("avatar URL must be an HTTP or HTTPS URL".to_owned())
            } else {
                Ok(())
            }
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            Err("avatar URL must be an HTTP or HTTPS URL".to_owned())
        }
        Err(error) => Err(format!("unable to parse avatar URL: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avatar_url_scheme_errors_match() {
        assert_eq!(
            check_avatar_url("ftp://example.com/a.png"),
            Err("avatar URL must be an HTTP or HTTPS URL".to_owned())
        );
        assert_eq!(
            check_avatar_url("example.com/a.png"),
            Err("avatar URL must be an HTTP or HTTPS URL".to_owned())
        );
        assert!(check_avatar_url("https://example.com/a.png").is_ok());
    }
}
