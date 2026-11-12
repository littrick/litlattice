#![forbid(unsafe_code)]

//! `Mutation.createAccessToken` / `deleteAccessToken`
//! (`cmd/frontend/graphqlbackend/access_tokens.go:34-280`).
//!
//! The authorization switch, duration/scope validation and error strings are
//! byte-for-byte the baseline's. Token generation and the `access_tokens`
//! write live in [`AdminStore`]; e-mail notification is a best-effort
//! [`AdminEffects`] side effect.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use sg_graphql::exec::{Actor, InputValue, RootField};
use sqlx::types::time::OffsetDateTime;

use crate::admin_store::{
    check_current_user_is_site_admin, check_same_user, check_site_admin_or_same_user,
    AccessTokenExpirationOptions, AccessTokensAllowMode, AdminEffects, AdminMutationOutcome,
    AdminStore, CreatedAccessToken,
};
use crate::graphql_resolvers::unmarshal_relay_id;

pub const SCOPE_USER_ALL: &str = "user:all";
pub const SCOPE_SITE_ADMIN_SUDO: &str = "site-admin:sudo";

/// `relay.MarshalID("AccessToken", id)`.
pub fn marshal_access_token_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("AccessToken:{id}"))
}

/// `unmarshalAccessTokenID` (the kind is not validated).
fn unmarshal_access_token_id(raw: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(raw)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

pub async fn prefetch_access_token_mutations(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, AdminMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "createAccessToken" => {
                create_access_token(store, effects, field, actor, viewer_site_admin).await
            }
            "deleteAccessToken" => {
                delete_access_token(store, effects, field, actor, viewer_site_admin).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

async fn create_access_token(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let Some(raw_user) = field.arguments.string("user") else {
        return AdminMutationOutcome::Error("missing user argument".to_owned());
    };
    let user_id = match unmarshal_relay_id(raw_user) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };

    match effects.access_tokens_allow() {
        AccessTokensAllowMode::All => {
            if let Err(message) = check_same_user(actor, user_id) {
                return AdminMutationOutcome::Error(message);
            }
        }
        AccessTokensAllowMode::Admin => {
            if check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
                return AdminMutationOutcome::Error(
                    "Access token creation has been restricted to admin users. Contact an admin user to create a new access token."
                        .to_owned(),
                );
            }
            if effects.dotcom() && check_same_user(actor, user_id).is_err() {
                return AdminMutationOutcome::Error(
                    "access token creation for other users is disabled on Sourcegraph.com"
                        .to_owned(),
                );
            }
        }
        // `case conf.AccessTokensNone:` has an empty body in the baseline, so
        // it does not reject; the `default` branch is unreachable through
        // `AccessTokensAllow()`.
        AccessTokensAllowMode::None => {}
        AccessTokensAllowMode::Other => {
            return AdminMutationOutcome::Error(
                "Access token creation is disabled. Contact an admin user to enable.".to_owned(),
            )
        }
    }

    let Some(duration) = field.arguments.i64("durationSeconds") else {
        return create_token_with_expiry(store, effects, field, actor, viewer_site_admin, user_id, None).await;
    };
    let Ok(duration) = i32::try_from(duration) else {
        return AdminMutationOutcome::Error("not a 32-bit integer".to_owned());
    };
    let options = effects.access_token_expiration_options();
    let max_duration = match max_duration(options) {
        Ok(max) => max,
        Err(message) => return AdminMutationOutcome::Error(message),
    };
    if duration <= 0 {
        return AdminMutationOutcome::Error("expiry must be in the future".to_owned());
    }
    if duration > max_duration {
        return AdminMutationOutcome::Error("expiry exceeds maximum allowed".to_owned());
    }
    let expires_at = OffsetDateTime::from_unix_timestamp(
        unix_seconds_now().saturating_add(duration as i64),
    )
    .ok();
    create_token_with_expiry(store, effects, field, actor, viewer_site_admin, user_id, expires_at).await
}

async fn create_token_with_expiry(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    user_id: i32,
    expires_at: Option<OffsetDateTime>,
) -> AdminMutationOutcome {
    let options = effects.access_token_expiration_options();
    if expires_at.is_none() && !options.allow_no_expiration {
        return AdminMutationOutcome::Error(
            "Access token creation requires a valid expiration.".to_owned(),
        );
    }

    let mut scopes: Vec<String> = match field.arguments.get("scopes") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    };
    let Some(note) = field.arguments.string("note") else {
        return AdminMutationOutcome::Error("missing note argument".to_owned());
    };

    // `sort.Strings(args.Scopes)`.
    scopes.sort();

    let mut has_user_all = false;
    let mut seen = std::collections::HashSet::new();
    for scope in &scopes {
        match scope.as_str() {
            SCOPE_USER_ALL => has_user_all = true,
            SCOPE_SITE_ADMIN_SUDO => {
                if let Err(message) =
                    check_current_user_is_site_admin(actor, viewer_site_admin)
                {
                    return AdminMutationOutcome::Error(message);
                }
                if effects.dotcom() {
                    return AdminMutationOutcome::Error(format!(
                        "creation of access tokens with scope \"site-admin:sudo\" is disabled on Sourcegraph.com"
                    ));
                }
            }
            other => {
                return AdminMutationOutcome::Error(format!(
                    "unknown access token scope {other:?} (valid scopes: [\"user:all\" \"site-admin:sudo\"])"
                ))
            }
        }
        if !seen.insert(scope.clone()) {
            return AdminMutationOutcome::Error(format!(
                "access token scope {scope:?} may not be specified multiple times"
            ));
        }
    }
    if !has_user_all {
        return AdminMutationOutcome::Error(format!(
            "all access tokens must have scope \"user:all\""
        ));
    }

    let creator = actor.uid().unwrap_or(0);
    let created: Result<CreatedAccessToken, String> =
        store.create_access_token(user_id, &scopes, note, creator, expires_at).await;
    match created {
        Ok(created) => {
            if effects.can_send_email() {
                // Background e-mail; failures are logged by the baseline.
                let _ = effects
                    .send_email(user_id, "", "access token created")
                    .await;
            }
            AdminMutationOutcome::AccessToken(created)
        }
        Err(error) => AdminMutationOutcome::Error(error),
    }
}

async fn delete_access_token(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let by_id = field.arguments.string("byID");
    let by_token = field.arguments.string("byToken");
    if by_id.is_none() && by_token.is_none() {
        return AdminMutationOutcome::Error(
            "either byID or byToken must be specified".to_owned(),
        );
    }
    if by_id.is_some() && by_token.is_some() {
        return AdminMutationOutcome::Error(
            "exactly one of byID or byToken must be specified".to_owned(),
        );
    }

    let token = if let Some(raw_id) = by_id {
        let id = match unmarshal_access_token_id(raw_id) {
            Ok(id) => id,
            Err(error) => return AdminMutationOutcome::Error(error),
        };
        let token = match store.get_access_token_by_id(id).await {
            Ok(Some(token)) => token,
            Ok(None) => return AdminMutationOutcome::Error("access token not found".to_owned()),
            Err(error) => return AdminMutationOutcome::Error(error),
        };
        if let Err(message) =
            check_site_admin_or_same_user(actor, viewer_site_admin, token.subject_user_id)
        {
            return AdminMutationOutcome::Error(message);
        }
        if actor.uid() != Some(token.subject_user_id) {
            let accounts = match store.external_accounts_for_user(token.subject_user_id).await {
                Ok(accounts) => accounts,
                Err(error) => {
                    return AdminMutationOutcome::Error(format!(
                        "list external accounts for token owner: {error}"
                    ))
                }
            };
            if accounts
                .iter()
                .any(|account| account.service_type == "sourcegraph-operator")
            {
                return AdminMutationOutcome::Error(format!(
                    "\"sourcegraph-operator\" user {}'s token cannot be deleted by a non-\"sourcegraph-operator\" user",
                    token.subject_user_id
                ));
            }
        }
        if let Err(error) = store.delete_access_token_by_id(id).await {
            return AdminMutationOutcome::Error(error);
        }
        token
    } else {
        let token_value = by_token.unwrap_or_default();
        let token = match store.get_access_token_by_token(token_value).await {
            Ok(Some(token)) => token,
            Ok(None) => return AdminMutationOutcome::Error("access token not found".to_owned()),
            Err(error) => return AdminMutationOutcome::Error(error),
        };
        if let Err(error) = store.delete_access_token_by_token(token_value).await {
            return AdminMutationOutcome::Error(error);
        }
        token
    };

    if effects.can_send_email() {
        let _ = effects
            .send_email(token.subject_user_id, "", "access token deleted")
            .await;
    }
    AdminMutationOutcome::Empty
}

/// `getMaxExpiryDuration` (`access_tokens.go:351-362`).
fn max_duration(options: AccessTokenExpirationOptions) -> Result<i32, String> {
    match options.max_duration_seconds {
        Ok(seconds) => i32::try_from(seconds)
            .map_err(|_| "no expiry options available".to_owned()),
        Err(message) => Err(message),
    }
}

fn unix_seconds_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_token_id_roundtrip() {
        assert_eq!(
            unmarshal_access_token_id(&marshal_access_token_id(42)),
            Ok(42)
        );
    }

    #[test]
    fn max_duration_uses_days() {
        assert_eq!(
            max_duration(AccessTokenExpirationOptions {
                allow_no_expiration: true,
                max_duration_seconds: Ok(90 * 86400),
            }),
            Ok(7_776_000)
        );
        assert_eq!(
            max_duration(AccessTokenExpirationOptions {
                allow_no_expiration: true,
                max_duration_seconds: Err("no expiry options available".to_owned()),
            }),
            Err("no expiry options available".to_owned())
        );
    }
}
