#![forbid(unsafe_code)]

//! `Mutation.addUserEmail` / `removeUserEmail` / `setUserEmailPrimary` /
//! `setUserEmailVerified` / `resendVerificationEmail`
//! (`cmd/frontend/graphqlbackend/user_emails.go:113-231`,
//! `cmd/frontend/internal/backend/user_emails.go:62-318`).
//!
//! The service-layer authorization, abuse checks, transaction side effects
//! (password-reset-code deletion, stale Perforce accounts, perms sync) and
//! error wrapping are reproduced; e-mail transport and the `dotcom` quota are
//! [`AdminEffects`] seams.

use std::time::Duration;

use sg_graphql::exec::{Actor, RootField};
use sqlx::types::time::OffsetDateTime;

use crate::admin_store::{
    check_current_user_is_site_admin, check_site_admin_or_same_user, AdminEffects,
    AdminMutationOutcome, AdminStore,
};
use crate::graphql_resolvers::unmarshal_relay_id;

pub async fn prefetch_user_email_mutations(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, AdminMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "addUserEmail" => {
                add_user_email(store, effects, field, actor, viewer_site_admin).await
            }
            "removeUserEmail" => {
                remove_user_email(store, effects, field, actor, viewer_site_admin).await
            }
            "setUserEmailPrimary" => {
                set_primary_email(store, effects, field, actor, viewer_site_admin).await
            }
            "setUserEmailVerified" => {
                set_email_verified(store, effects, field, actor, viewer_site_admin).await
            }
            "resendVerificationEmail" => {
                resend_verification_email(store, effects, field, actor, viewer_site_admin).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

fn user_and_email(field: &RootField) -> Result<(i32, String), String> {
    let Some(raw_user) = field.arguments.string("user") else {
        return Err("missing user argument".to_owned());
    };
    let user_id = unmarshal_relay_id(raw_user)?;
    let Some(email) = field.arguments.string("email") else {
        return Err("missing email argument".to_owned());
    };
    Ok((user_id, email.to_owned()))
}

async fn add_user_email(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let (user_id, email) = match user_and_email(field) {
        Ok(values) => values,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, user_id) {
        return AdminMutationOutcome::Error(message);
    }

    if !viewer_site_admin && !matches!(actor, Actor::Internal) {
        match check_email_abuse(store, effects, user_id).await {
            Ok(None) => {}
            Ok(Some(message)) => return AdminMutationOutcome::Error(message),
            Err(error) => return AdminMutationOutcome::Error(error),
        }
    }

    let code = if effects.email_verification_required() {
        match effects.make_email_verification_code() {
            Ok(code) => Some(code),
            Err(error) => return AdminMutationOutcome::Error(error),
        }
    } else {
        None
    };

    match store.get_by_verified_email(&email).await {
        Ok(Some(_)) => {
            return AdminMutationOutcome::Error(
                "a user with this email already exists".to_owned(),
            )
        }
        Ok(None) => {}
        Err(error) => return AdminMutationOutcome::Error(error),
    }

    if let Err(error) = store.add_user_email(user_id, &email, code.as_deref()).await {
        return AdminMutationOutcome::Error(error);
    }

    if effects.auditlog_expansion() {
        let arguments = serde_json::json!({"UserID": user_id, "email": email});
        effects.audit("EmailAdded", user_id, arguments).await;
    }

    if effects.email_verification_required() {
        let user = match store.get_user_by_id(user_id).await {
            Ok(Some(user)) => user,
            Ok(None) => return AdminMutationOutcome::Error(format!("user not found: [{user_id}]")),
            Err(error) => return AdminMutationOutcome::Error(error),
        };
        // The baseline sends the e-mail, then (in a defer) marks the
        // verification as sent regardless of send success.
        let send = effects
            .send_email(user.id, &email, &format!("verification:{}", user.username))
            .await;
        let code = code.unwrap_or_default();
        let _ = store
            .set_last_verification(user_id, &email, &code, OffsetDateTime::now_utc())
            .await;
        if let Err(error) = send {
            return AdminMutationOutcome::Error(format!("SendUserEmailVerificationEmail: {error}"));
        }
    }
    AdminMutationOutcome::Empty
}

async fn remove_user_email(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let (user_id, email) = match user_and_email(field) {
        Ok(values) => values,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, user_id) {
        return AdminMutationOutcome::Error(message);
    }
    if let Err(error) = store.remove_user_email(user_id, &email).await {
        return AdminMutationOutcome::Error(format!("removing user e-mail: {error}"));
    }
    if effects.auditlog_expansion() {
        let arguments = serde_json::json!({"UserID": user_id, "email": email});
        effects.audit("EmailRemoved", user_id, arguments).await;
    }
    if let Err(error) = effects.delete_password_reset_code(user_id).await {
        return AdminMutationOutcome::Error(format!("deleting reset codes: {error}"));
    }
    if let Err(error) = effects.delete_stale_perforce(user_id, &email).await {
        return AdminMutationOutcome::Error(format!(
            "removing stale perforce external account: {error}"
        ));
    }
    if effects.can_send_email() {
        let _ = effects.send_email(user_id, &email, "removed an email").await;
    }
    let _ = effects
        .schedule_perms_sync(&[user_id], "REASON_USER_EMAIL_REMOVED")
        .await;
    AdminMutationOutcome::Empty
}

async fn set_primary_email(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let (user_id, email) = match user_and_email(field) {
        Ok(values) => values,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, user_id) {
        return AdminMutationOutcome::Error(message);
    }
    if let Err(error) = store.set_primary_email(user_id, &email).await {
        return AdminMutationOutcome::Error(error);
    }
    if effects.can_send_email() {
        let _ = effects
            .send_email(user_id, &email, "changed primary email")
            .await;
    }
    AdminMutationOutcome::Empty
}

async fn set_email_verified(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let (user_id, email) = match user_and_email(field) {
        Ok(values) => values,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(verified) = field.arguments.bool("verified") else {
        return AdminMutationOutcome::Error("missing verified argument".to_owned());
    };

    // `backend.NewUserEmailsService(db).SetVerified`: the email write and the
    // `verified` grant (or the Perforce cleanup) share one transaction
    // (`user_emails.go:223-273`). Grant failures are logged only.
    if let Err(error) = store
        .set_email_verified_and_grant(user_id, &email, verified)
        .await
    {
        return AdminMutationOutcome::Error(error);
    }
    if effects.auditlog_expansion() {
        let arguments = serde_json::json!({"email": email, "verified": verified});
        effects
            .audit("EmailVerificationChanged", user_id, arguments)
            .await;
    }
    let _ = effects
        .schedule_perms_sync(&[user_id], "REASON_USER_EMAIL_VERIFIED")
        .await;
    AdminMutationOutcome::Empty
}

async fn resend_verification_email(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let (user_id, email) = match user_and_email(field) {
        Ok(values) => values,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, user_id) {
        return AdminMutationOutcome::Error(message);
    }
    let user = match store.get_user_by_id(user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => return AdminMutationOutcome::Error(format!("user not found: [{user_id}]")),
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    let last_sent = match store.latest_verification_sent_email(&email).await {
        Ok(value) => value,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Some(last_sent) = last_sent {
        if let Some(sent_at) = last_sent.last_verification_sent_at {
            let now = OffsetDateTime::now_utc();
            if now - sent_at < Duration::from_secs(60) {
                return AdminMutationOutcome::Error(
                    "Last verification email sent too recently".to_owned(),
                );
            }
        }
    }
    let email_row = match store.get_user_email(user_id, &email).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!(
                "user email not found: [userID {user_id} email {email:?}]"
            ))
        }
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if email_row.verified {
        return AdminMutationOutcome::Empty;
    }
    let code = match effects.make_email_verification_code() {
        Ok(code) => code,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    let now = OffsetDateTime::now_utc();
    if let Err(error) = store
        .set_last_verification(user_id, &email, &code, now)
        .await
    {
        return AdminMutationOutcome::Error(error);
    }
    match effects
        .send_email(user_id, &email, &format!("verification:{}", user.username))
        .await
    {
        Ok(()) => AdminMutationOutcome::Empty,
        Err(error) => AdminMutationOutcome::Error(error),
    }
}

/// `checkEmailAbuse` (offline subset: the verified/unverified rules; the
/// dotcom user-creation quota is a documented seam).
async fn check_email_abuse(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    user_id: i32,
) -> Result<Option<String>, String> {
    if effects.email_verification_required() {
        let emails = store.list_user_emails(user_id, false).await?;
        let verified_count = emails.iter().filter(|email| email.verified).count();
        let unverified_count = emails.len() - verified_count;
        if verified_count == 0 && !emails.is_empty() {
            return Ok(Some(
                "refusing to add email address because a verified email is required before you can add additional email addressed to your account"
                    .to_owned(),
            ));
        }
        if unverified_count >= 3 {
            return Ok(Some(
                "refusing to add email address because too many existing unverified email addresses"
                    .to_owned(),
            ));
        }
    }
    Ok(None)
}
