#![forbid(unsafe_code)]

//! `Mutation.updateSiteConfiguration`
//! (`cmd/frontend/graphqlbackend/site.go:294-362`).
//!
//! The executor resolves fields synchronously while the configuration service
//! is async, so the whole mutation runs in
//! [`prefetch_update_site_configuration`] before execution; the
//! [`MutationRoot`](crate::settings_mutation::MutationRoot) only replays the
//! `Boolean!` result or the resolver error.
//!
//! The baseline's flow, in order:
//! 1. `auth.CheckCurrentUserIsSiteAdmin`;
//! 2. `canUpdateSiteConfiguration` (`SITE_CONFIG_FILE` +
//!    `SITE_CONFIG_ALLOW_EDITS`);
//! 3. reject a blank (`strings.TrimSpace`) input;
//! 4. `prev := conf.Raw()` and its redacted form for the audit argument;
//! 5. `conf.UnredactSecrets`;
//! 6. `cloud.SiteConfig()` allowlist (errata §4.3: OSS is a no-op);
//! 7. `configurationServer.Write(ctx, prev, lastID, uid)`, which waits for the
//!    update loop to adopt the new raw configuration;
//! 8. with `auditlog-expansion`, insert a `SiteConfigUpdated` security event;
//! 9. return `configurationServer.NeedServerRestart()`.
//!
//! The restart flag is set by the asynchronous configuration watcher, exactly
//! like the baseline's `conf.Watch` goroutine, so the returned value races the
//! watcher in both implementations (the differential harness polls the final
//! flag; see the overview §5).

use std::sync::{Arc, OnceLock};

use sg_config::store::ConfigStore;
use sg_db::feature_flags::{get_global_feature_flags, get_user_flags};
use sg_db::security_event_logs::{
    insert_security_event, security_event_logs_in_database, SecurityEvent,
    SECURITY_EVENT_NAME_SITE_CONFIG_UPDATED,
};
use sg_graphql::exec::{Actor, RootField};
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;

use crate::configuration_server::DbConfigServer;
use crate::graphql_resolvers::check_current_user_is_site_admin;
use crate::site_configuration_secrets::{redact_secrets, unredact_secrets};

/// The seams `Mutation.updateSiteConfiguration` needs: the process' raw
/// configuration store (`conf.Raw`) and its configuration server (`Write` /
/// `NeedServerRestart`).
#[derive(Clone)]
pub struct SiteConfigurationDeps {
    pub store: Arc<ConfigStore>,
    pub server: Arc<DbConfigServer>,
}

/// The baseline's error texts (`site.go:305-331`).
pub const NOT_ALLOWED_WITH_SITE_CONFIG_FILE: &str =
    "updating site configuration not allowed when using SITE_CONFIG_FILE";
pub const BLANK_SITE_CONFIGURATION: &str = "blank site configuration is invalid (you can clear the site configuration by entering an empty JSON object: {})";

/// One `updateSiteConfiguration` root field's outcome: the GraphQL `Boolean!`
/// return value or the resolver error.
pub async fn prefetch_update_site_configuration(
    pool: &PgPool,
    deps: &SiteConfigurationDeps,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, Result<bool, String>)> {
    let mut outcomes = Vec::new();
    for field in fields {
        if field.name != "updateSiteConfiguration" {
            continue;
        }
        let outcome = update_site_configuration(pool, deps, field, actor, viewer_site_admin).await;
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

async fn update_site_configuration(
    pool: &PgPool,
    deps: &SiteConfigurationDeps,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<bool, String> {
    // 🚨 SECURITY: only site admins may edit the site configuration.
    check_current_user_is_site_admin(actor, viewer_site_admin)?;

    if !can_update_site_configuration() {
        return Err(NOT_ALLOWED_WITH_SITE_CONFIG_FILE.to_owned());
    }

    let input = field
        .arguments
        .string("input")
        .ok_or_else(|| "missing input argument".to_owned())?;
    if input.trim().is_empty() {
        return Err(BLANK_SITE_CONFIGURATION.to_owned());
    }
    let last_id = field
        .arguments
        .i64("lastID")
        .ok_or_else(|| "missing lastID argument".to_owned())
        .and_then(|last_id| {
            i32::try_from(last_id).map_err(|_| "not a 32-bit integer".to_owned())
        })?;

    // `conf.Raw()` blocks until the store has been initialized; the frontend
    // starts the configuration server before serving GraphQL.
    let mut prev = deps.store.raw();

    // 🚨 SECURITY: log only the redacted previous configuration.
    let prev_redacted = redact_secrets(&prev);
    let argument = serde_json::json!({
        "prev_config": prev_redacted,
        "new_config": input,
    });

    let unredacted = unredact_secrets(input, &prev)
        .map_err(|err| format!("error unredacting secrets: {err}"))?;

    // `cloud.SiteConfig().SiteConfigAllowlistEnabled()` is always false in the
    // OSS build (`cloud.SiteConfig` returns the empty `SchemaSiteConfig`), so
    // the allowlist check is not ported (overview §4.3).
    prev.site = unredacted;

    let author_user_id = actor.uid().unwrap_or(0);
    deps.server
        .write(prev, last_id, author_user_id)
        .await
        .map_err(|err| err.to_string())?;

    if auditlog_expansion(pool, actor).await {
        // `security_event_logs` stores its own location gate; the location is
        // read from the configuration the update loop just adopted.
        let site = deps
            .store
            .last_valid()
            .map(|unified| unified.site.raw().clone())
            .unwrap_or(serde_json::Value::Null);
        if security_event_logs_in_database(&site) {
            let version = crate::version::Version::load();
            let version = String::from_utf8_lossy(version.as_bytes()).into_owned();
            let event = SecurityEvent {
                name: SECURITY_EVENT_NAME_SITE_CONFIG_UPDATED,
                url: "",
                user_id: author_user_id,
                anonymous_user_id: "",
                source: "BACKEND",
                argument,
                version: &version,
                timestamp: OffsetDateTime::now_utc(),
            };
            // The baseline's `LogSecurityEvent` logs (does not return) an
            // insert failure, so the mutation still succeeds.
            if let Err(err) = insert_security_event(pool, &event).await {
                tracing::warn!(error = %err, "Error logging security event");
            }
        }
    }

    Ok(deps.server.need_server_restart())
}

/// `canUpdateSiteConfiguration` (`site.go:354-356`).
fn can_update_site_configuration() -> bool {
    can_update_with(
        std::env::var("SITE_CONFIG_FILE")
            .unwrap_or_default()
            .as_str(),
        site_config_allow_edits(),
    )
}

fn can_update_with(site_config_file: &str, allow_edits: bool) -> bool {
    site_config_file.is_empty() || allow_edits
}

/// `siteConfigAllowEdits` (`site.go:351-352`): `strconv.ParseBool` of
/// `SITE_CONFIG_ALLOW_EDITS`, evaluated once at package init; once per process
/// here.
fn site_config_allow_edits() -> bool {
    static ALLOW: OnceLock<bool> = OnceLock::new();
    *ALLOW.get_or_init(|| {
        std::env::var("SITE_CONFIG_ALLOW_EDITS")
            .map(|value| parse_bool(&value).unwrap_or(false))
            .unwrap_or(false)
    })
}

/// `strconv.ParseBool`.
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `featureflag.FromContext(ctx).GetBoolOr("auditlog-expansion", false)`.
///
/// The baseline middleware falls back from the per-user evaluation to the
/// global flags and then to an empty map on errors; anonymous actors only have
/// the global flags.
async fn auditlog_expansion(pool: &PgPool, actor: Actor) -> bool {
    match actor {
        Actor::User { uid } => match get_user_flags(pool, uid).await {
            Ok(flags) => flags.get("auditlog-expansion").copied().unwrap_or(false),
            Err(_) => global_auditlog_expansion(pool).await,
        },
        Actor::Anonymous => global_auditlog_expansion(pool).await,
    }
}

async fn global_auditlog_expansion(pool: &PgPool) -> bool {
    match get_global_feature_flags(pool).await {
        Ok(flags) => flags.get("auditlog-expansion").copied().unwrap_or(false),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn site_config_file_gate_matches_the_baseline() {
        assert!(can_update_with("", false));
        assert!(can_update_with("", true));
        assert!(!can_update_with("/etc/site.json", false));
        assert!(can_update_with("/etc/site.json", true));
    }

    #[test]
    fn parse_bool_matches_strconv() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("False"), Some(false));
        assert_eq!(parse_bool("yes"), None);
    }

    #[test]
    fn error_texts_match_the_baseline() {
        assert_eq!(
            NOT_ALLOWED_WITH_SITE_CONFIG_FILE,
            "updating site configuration not allowed when using SITE_CONFIG_FILE"
        );
        assert_eq!(
            BLANK_SITE_CONFIGURATION,
            "blank site configuration is invalid (you can clear the site configuration by entering an empty JSON object: {})"
        );
    }
}
