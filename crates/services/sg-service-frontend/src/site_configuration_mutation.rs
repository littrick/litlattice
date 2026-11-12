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

/// `cloud.SchemaSiteConfig`, reduced to the allowlist behavior. The signed
/// `SRC_CLOUD_SITE_CONFIG` blob (SSH signature verification) is not parsed in
/// this port; an operator injects the decoded value instead.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudSiteConfig {
    /// `SourcegraphOperatorAuthProviderEnabled() && len(Paths) > 0`.
    pub allowlist_enabled: bool,
    /// `SiteConfigAllowlist.Paths`.
    pub allowed_paths: Vec<String>,
    /// `SiteConfigAllowlist.NotAllowedErrorMessage` (a Go text/template with
    /// the `{{.Paths}}` variable). `None` selects the default text.
    pub not_allowed_error_message: Option<String>,
}

impl CloudSiteConfig {
    /// `SiteConfigAllowlistOnError` for the paths [`allow_edit`] rejected.
    pub fn allowlist_error(&self, paths: &[String]) -> String {
        match &self.not_allowed_error_message {
            None => format!(
                "Editing [{}] in site configuration is not allowed on Sourcegraph Cloud. Please contact support.",
                paths.join(", ")
            ),
            // The custom template is rendered with `Paths` only.
            Some(template) => template.replace("{{.Paths}}", &format!("[{}]", paths.join(", "))),
        }
    }
}

/// One `updateSiteConfiguration` root field's outcome: the GraphQL `Boolean!`
/// return value or the resolver error.
pub async fn prefetch_update_site_configuration(
    pool: &PgPool,
    deps: &SiteConfigurationDeps,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, Result<bool, String>)> {
    // OSS: `cloud.SiteConfig()` is the empty stub, so the allowlist is off.
    prefetch_update_site_configuration_with_cloud(
        pool,
        deps,
        &CloudSiteConfig::default(),
        false,
        fields,
        actor,
        viewer_site_admin,
    )
    .await
}

/// The same prefetch with the Cloud allowlist seam and the actor's
/// `SourcegraphOperator` flag injected.
#[allow(clippy::too_many_arguments)] // Mirrors the baseline's argument set.
pub async fn prefetch_update_site_configuration_with_cloud(
    pool: &PgPool,
    deps: &SiteConfigurationDeps,
    cloud: &CloudSiteConfig,
    sourcegraph_operator: bool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, Result<bool, String>)> {
    let mut outcomes = Vec::new();
    for field in fields {
        if field.name != "updateSiteConfiguration" {
            continue;
        }
        let outcome = update_site_configuration(
            pool,
            deps,
            cloud,
            sourcegraph_operator,
            field,
            actor,
            viewer_site_admin,
        )
        .await;
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

#[allow(clippy::too_many_arguments)] // Mirrors the baseline's argument set.
async fn update_site_configuration(
    pool: &PgPool,
    deps: &SiteConfigurationDeps,
    cloud: &CloudSiteConfig,
    sourcegraph_operator: bool,
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

    // `cloud.SiteConfig().SiteConfigAllowlistEnabled()`; the OSS default is
    // disabled. The actor check mirrors `!actor.SourcegraphOperator`.
    if cloud.allowlist_enabled && !sourcegraph_operator {
        let (not_allowed, ok) = allow_edit(&prev.site, &unredacted, &cloud.allowed_paths);
        if !ok {
            return Err(cloud.allowlist_error(&not_allowed));
        }
    }

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

/// `conf.Diff`'s top-level field names (`internal/conf/diff.go`), including
/// the `experimentalFeatures::` expansion, followed by `allowEdit`
/// (`site.go:670-681`).
pub fn allow_edit(before: &str, after: &str, allowlist: &[String]) -> (Vec<String>, bool) {
    let changes = changed_site_fields(before, after);
    let mut not_allowed = Vec::new();
    for key in changes {
        for allowed in allowlist {
            if &key != allowed {
                not_allowed.push(key.clone());
            }
        }
    }
    let ok = not_allowed.is_empty();
    (not_allowed, ok)
}

/// The changed top-level keys of the parsed configs. A missing/zero scalar
/// compares equal to its Go zero value; slices/maps stay distinct from
/// `null` (Go's `reflect.DeepEqual(nil, []T{})` is false).
fn changed_site_fields(before: &str, after: &str) -> Vec<String> {
    let Ok(before) = sg_config::SiteConfiguration::parse(before) else {
        return Vec::new();
    };
    let Ok(after) = sg_config::SiteConfiguration::parse(after) else {
        return Vec::new();
    };
    let before = before.raw().as_object();
    let after = after.raw().as_object();

    let mut keys: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    if let Some(object) = before {
        keys.extend(object.keys().map(String::as_str));
    }
    if let Some(object) = after {
        keys.extend(object.keys().map(String::as_str));
    }

    let mut changed = Vec::new();
    for key in keys {
        if key == "experimentalFeatures" {
            // `getJSONFields` expands `*schema.ExperimentalFeatures` instead
            // of recording the pointer itself.
            let before_features = before
                .and_then(|object| object.get(key))
                .and_then(serde_json::Value::as_object);
            let after_features = after
                .and_then(|object| object.get(key))
                .and_then(serde_json::Value::as_object);
            let mut feature_keys: std::collections::BTreeSet<&str> =
                std::collections::BTreeSet::new();
            if let Some(object) = before_features {
                feature_keys.extend(object.keys().map(String::as_str));
            }
            if let Some(object) = after_features {
                feature_keys.extend(object.keys().map(String::as_str));
            }
            for feature in feature_keys {
                let before_value = before_features.and_then(|object| object.get(feature));
                let after_value = after_features.and_then(|object| object.get(feature));
                if normalize_site_value(before_value) != normalize_site_value(after_value) {
                    changed.push(format!("experimentalFeatures::{feature}"));
                }
            }
            continue;
        }

        let before_value = before.and_then(|object| object.get(key));
        let after_value = after.and_then(|object| object.get(key));
        if normalize_site_value(before_value) != normalize_site_value(after_value) {
            changed.push(key.to_owned());
        }
    }
    changed
}

/// Map absent/`null`/zero scalars to one value so that an omitted field and
/// its Go zero value compare equal.
fn normalize_site_value(value: Option<&serde_json::Value>) -> Option<serde_json::Value> {
    use serde_json::Value;
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) if text.is_empty() => None,
        Some(Value::Bool(false)) => None,
        Some(Value::Number(number)) if number.as_f64() == Some(0.0) => None,
        Some(value) => Some(value.clone()),
    }
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
        // Internal actors have `UID == 0`; see `config_graphql` for the
        // featureflag-middleware caveat.
        Actor::Anonymous | Actor::Internal => global_auditlog_expansion(pool).await,
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
