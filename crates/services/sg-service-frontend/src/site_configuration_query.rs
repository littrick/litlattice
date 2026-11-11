#![forbid(unsafe_code)]

//! `Site.configuration` and the `SiteConfiguration` read fields
//! (`cmd/frontend/graphqlbackend/site.go:100-138,198-231`).
//!
//! [`configuration`] ports `siteResolver.Configuration`: site-admin check,
//! the `returnSafeConfigsOnly` escape hatch for non-admins and the
//! `auditlog-expansion` security events. The returned
//! [`SiteConfigurationResolver`] then answers:
//!
//! - [`id`] — `siteConfigurationResolver.ID`: admin check +
//!   `db.Conf().SiteGetLatest(ctx).ID`;
//! - [`effective_contents`] — `siteConfigurationResolver.EffectiveContents`:
//!   `conf.ReturnSafeConfigs(conf.Raw())` on the safe branch, otherwise an
//!   admin check followed by `conf.RedactSecrets(conf.Raw())`.
//!
//! Every dependency is injected through [`SiteConfigurationBackend`]: the
//! site-admin check, the `auditlog-expansion` feature flag, the security
//! event log, `Conf().SiteGetLatest` and the `conf.Raw()` snapshot. The
//! production wiring is expected to back them with
//! `graphql_resolvers`' admin check, `sg_db::feature_flags`, the
//! `security_event_logs` write path and the configuration store.
//!
//! `conf.ReturnSafeConfigs` is reimplemented here (it only projects
//! `batchChanges.rolloutWindows`); `conf.RedactSecrets` is reused from
//! [`crate::site_configuration_secrets::redact_secrets`] (M9.8gn).

use async_trait::async_trait;
use serde_json::{Map, Value};
use sg_config::{RawUnified, SiteConfiguration};
use sg_graphql::exec::Actor;

/// `database.SecurityEventNameSiteConfigRedactedViewed`
/// (`internal/database/security_event_logs.go:83`).
pub const SECURITY_EVENT_NAME_SITE_CONFIG_REDACTED_VIEWED: &str = "SiteConfigRedactedViewed";
/// `database.SecurityEventNameSiteConfigViewed`
/// (`internal/database/security_event_logs.go:84`).
pub const SECURITY_EVENT_NAME_SITE_CONFIG_VIEWED: &str = "SiteConfigViewed";
/// The `source` argument passed to `LogSecurityEvent` (`site.go:117,127`).
pub const SECURITY_EVENT_SOURCE_BACKEND: &str = "BACKEND";
/// The flag name read by `featureflag.FromContext(ctx).GetBoolOr(...)`.
pub const AUDITLOG_EXPANSION_FLAG: &str = "auditlog-expansion";

/// One `db.SecurityEventLogs().LogSecurityEvent` call. Field names and
/// values mirror the Go `database.SecurityEvent` arguments
/// (`internal/database/security_event_logs.go:261-272`).
#[derive(Debug, Clone, PartialEq)]
pub struct SecurityEvent {
    pub name: &'static str,
    pub url: String,
    pub user_id: i32,
    pub anonymous_user_id: String,
    pub source: &'static str,
    /// `json.Marshal(arguments)`; the resolver always passes `nil` → `null`.
    pub argument: Value,
}

/// The slice of `Conf().SiteGetLatest` the resolver consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SiteConfigRow {
    pub id: i32,
}

/// All dependencies of `Site.configuration` / `siteConfigurationResolver`.
///
/// The baseline reads process-global state (`auth.CheckCurrentUserIsSiteAdmin`,
/// `featureflag.FromContext`, `conf.Raw`, `db.Conf()`); this port injects a
/// snapshot of each so the resolver is deterministic and testable.
#[async_trait]
pub trait SiteConfigurationBackend: Send + Sync {
    /// `auth.CheckCurrentUserIsSiteAdmin(ctx, db)`: `not authenticated` for
    /// anonymous actors, `must be site admin` for authenticated non-admins.
    async fn check_current_user_is_site_admin(&self, actor: Actor) -> Result<(), String>;
    /// `featureflag.FromContext(ctx).GetBoolOr("auditlog-expansion", false)`.
    async fn auditlog_expansion(&self, actor: Actor) -> bool;
    /// `db.SecurityEventLogs().LogSecurityEvent(...)`; failures are logged and
    /// ignored by the resolver.
    async fn log_security_event(&self, event: SecurityEvent) -> Result<(), String>;
    /// `db.Conf().SiteGetLatest(ctx)`.
    async fn site_get_latest(&self) -> Result<SiteConfigRow, String>;
    /// The current `conf.Raw().Site` JSONC snapshot.
    fn raw_site(&self) -> String;
}

/// `siteConfigurationResolver` (`site.go:198-201`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SiteConfigurationResolver {
    pub return_safe_configs_only: bool,
}

/// `siteResolver.Configuration` (`site.go:100-138`).
pub async fn configuration(
    backend: &dyn SiteConfigurationBackend,
    actor: Actor,
    return_safe_configs_only: Option<bool>,
) -> Result<SiteConfigurationResolver, String> {
    // `pointers.Deref(args.ReturnSafeConfigsOnly, false)`.
    let return_safe_configs_only = return_safe_configs_only.unwrap_or(false);

    // 🚨 SECURITY: the site configuration contains secrets; only admins may
    // view the unredacted form.
    if let Err(err) = backend.check_current_user_is_site_admin(actor).await {
        if return_safe_configs_only {
            // The only way a non-admin can reach the field is the safe view.
            if backend.auditlog_expansion(actor).await {
                warn_on_log_failure(
                    backend
                        .log_security_event(security_event(
                            SECURITY_EVENT_NAME_SITE_CONFIG_REDACTED_VIEWED,
                            actor,
                        ))
                        .await,
                );
            }
            return Ok(SiteConfigurationResolver {
                return_safe_configs_only,
            });
        }
        return Err(err);
    }

    if backend.auditlog_expansion(actor).await {
        warn_on_log_failure(
            backend
                .log_security_event(security_event(
                    SECURITY_EVENT_NAME_SITE_CONFIG_VIEWED,
                    actor,
                ))
                .await,
        );
    }
    Ok(SiteConfigurationResolver {
        return_safe_configs_only,
    })
}

fn security_event(name: &'static str, actor: Actor) -> SecurityEvent {
    SecurityEvent {
        name,
        url: String::new(),
        // `uint32(actor.FromContext(ctx).UID)`: the anonymous actor's UID is 0.
        user_id: actor.uid().unwrap_or(0),
        anonymous_user_id: String::new(),
        source: SECURITY_EVENT_SOURCE_BACKEND,
        argument: Value::Null,
    }
}

fn warn_on_log_failure(result: Result<(), String>) {
    if let Err(err) = result {
        // The baseline logs, it does not fail the resolver
        // (`r.logger.Warn("Error logging security event", ...)`).
        tracing::warn!(error = %err, "Error logging security event");
    }
}

/// `siteConfigurationResolver.ID` (`site.go:203-214`).
pub async fn id(
    backend: &dyn SiteConfigurationBackend,
    actor: Actor,
    _resolver: &SiteConfigurationResolver,
) -> Result<i32, String> {
    // 🚨 SECURITY: re-check every access, like the baseline.
    backend.check_current_user_is_site_admin(actor).await?;
    Ok(backend.site_get_latest().await?.id)
}

/// `siteConfigurationResolver.EffectiveContents` (`site.go:216-231`).
pub async fn effective_contents(
    backend: &dyn SiteConfigurationBackend,
    actor: Actor,
    resolver: &SiteConfigurationResolver,
) -> Result<String, String> {
    if resolver.return_safe_configs_only {
        // The safe branch deliberately skips the admin check: non-admins may
        // only ever see this projected configuration.
        return return_safe_configs(&backend.raw_site());
    }

    // 🚨 SECURITY: the full configuration contains secret tokens and
    // credentials.
    backend.check_current_user_is_site_admin(actor).await?;
    redact_site_config(&backend.raw_site())
}

/// `conf.ReturnSafeConfigs` (`internal/conf/validate.go:344-364`): only
/// `batchChanges.rolloutWindows` is safe to expose to non-admins.
///
/// Returns `parse config: ...` when the JSONC does not parse or when the
/// projected field does not match the Go typed shape.
pub fn return_safe_configs(site: &str) -> Result<String, String> {
    let config = SiteConfiguration::parse(site).map_err(|err| format!("parse config: {err}"))?;
    let mut output = Map::new();
    match config.raw().get("batchChanges.rolloutWindows") {
        // `omitempty` on the `*[]*BatchChangeRolloutWindow` pointer.
        None | Some(Value::Null) => {}
        Some(Value::Array(windows)) => {
            let windows = windows
                .iter()
                .map(safe_rollout_window)
                .collect::<Result<Vec<_>, _>>()?;
            output.insert(
                "batchChanges.rolloutWindows".to_owned(),
                Value::Array(windows),
            );
        }
        Some(_) => return Err(rollout_windows_type_error()),
    }
    // `json.Marshal` of the one-field `schema.SiteConfiguration`.
    serde_json::to_string(&Value::Object(output)).map_err(|err| format!("parse config: {err}"))
}

/// `schema.BatchChangeRolloutWindow` (`schema/schema.go:297-306`).
fn safe_rollout_window(window: &Value) -> Result<Value, String> {
    let Value::Object(object) = window else {
        if window.is_null() {
            // `[]*BatchChangeRolloutWindow` permits a `null` element.
            return Ok(Value::Null);
        }
        return Err(rollout_windows_type_error());
    };

    let mut output = Map::new();

    // `Days []string json:"days,omitempty"`.
    match object.get("days") {
        None | Some(Value::Null) => {}
        Some(Value::Array(days)) => {
            let mut values = Vec::with_capacity(days.len());
            for day in days {
                match day {
                    Value::String(day) => values.push(Value::String(day.clone())),
                    _ => return Err(window_field_type_error("days", "[]string")),
                }
            }
            // omitempty drops the empty slice.
            if !values.is_empty() {
                output.insert("days".to_owned(), Value::Array(values));
            }
        }
        Some(_) => return Err(window_field_type_error("days", "[]string")),
    }

    // `End string json:"end,omitempty"` / `Start string json:"start,omitempty"`.
    for field in ["end", "start"] {
        match object.get(field) {
            None | Some(Value::Null) => {}
            Some(Value::String(value)) => {
                if !value.is_empty() {
                    output.insert(field.to_owned(), Value::String(value.clone()));
                }
            }
            Some(_) => return Err(window_field_type_error(field, "string")),
        }
    }

    // `Rate any json:"rate"`: no `omitempty`, so the key is always emitted
    // (`null` when the input omits it).
    output.insert(
        "rate".to_owned(),
        object.get("rate").cloned().unwrap_or(Value::Null),
    );

    // The map serializes keys in lexicographic order: days, end, rate, start,
    // which is also the Go struct field order.
    Ok(Value::Object(output))
}

fn rollout_windows_type_error() -> String {
    "parse config: json: cannot unmarshal value into Go struct field \
     SiteConfiguration.batchChanges.rolloutWindows of type \
     []*schema.BatchChangeRolloutWindow"
        .to_owned()
}

fn window_field_type_error(field: &str, go_type: &str) -> String {
    format!(
        "parse config: json: cannot unmarshal value into Go struct field \
         BatchChangeRolloutWindow.{field} of type {go_type}"
    )
}

/// `conf.RedactSecrets` (`internal/conf/validate.go:367-372,386-478`).
///
/// The M9.8gn [`redact_secrets`](crate::site_configuration_secrets::redact_secrets)
/// implementation edits the raw JSONC in place and formats it; it swallows
/// parse failures as the empty string. The resolver needs the error, so this
/// wrapper parses first (the baseline's `ParseConfig`) and reports
/// `parse config: ...`; the successful path is byte-identical to M9.8gn.
pub fn redact_site_config(site: &str) -> Result<String, String> {
    SiteConfiguration::parse(site).map_err(|err| format!("parse config: {err}"))?;
    let raw = RawUnified {
        site: site.to_owned(),
        ..RawUnified::default()
    };
    Ok(crate::site_configuration_secrets::redact_secrets(&raw))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;

    struct FakeBackend {
        viewer_site_admin: bool,
        auditlog_expansion: bool,
        raw_site: String,
        site_get_latest: Result<i32, String>,
        events: Mutex<Vec<SecurityEvent>>,
        fail_log: bool,
    }

    impl FakeBackend {
        fn admin() -> Self {
            Self {
                viewer_site_admin: true,
                auditlog_expansion: false,
                raw_site: "{}".to_owned(),
                site_get_latest: Ok(7),
                events: Mutex::new(Vec::new()),
                fail_log: false,
            }
        }

        fn non_admin() -> Self {
            Self {
                viewer_site_admin: false,
                ..Self::admin()
            }
        }

        fn events(&self) -> Vec<SecurityEvent> {
            self.events.lock().expect("events").clone()
        }
    }

    #[async_trait]
    impl SiteConfigurationBackend for FakeBackend {
        async fn check_current_user_is_site_admin(&self, actor: Actor) -> Result<(), String> {
            match actor {
                Actor::Internal => Ok(()),
                Actor::Anonymous => Err("not authenticated".to_owned()),
                Actor::User { .. } if !self.viewer_site_admin => {
                    Err("must be site admin".to_owned())
                }
                Actor::User { .. } => Ok(()),
            }
        }

        async fn auditlog_expansion(&self, _actor: Actor) -> bool {
            self.auditlog_expansion
        }

        async fn log_security_event(&self, event: SecurityEvent) -> Result<(), String> {
            self.events.lock().expect("events").push(event);
            if self.fail_log {
                Err("insert failed".to_owned())
            } else {
                Ok(())
            }
        }

        async fn site_get_latest(&self) -> Result<SiteConfigRow, String> {
            self.site_get_latest.clone().map(|id| SiteConfigRow { id })
        }

        fn raw_site(&self) -> String {
            self.raw_site.clone()
        }
    }

    fn user() -> Actor {
        Actor::User { uid: 42 }
    }

    #[test]
    fn return_safe_configs_projects_only_rollout_windows() {
        assert_eq!(return_safe_configs("").expect("safe"), "{}");
        assert_eq!(return_safe_configs("{}").expect("safe"), "{}");
        assert_eq!(
            return_safe_configs(
                r#"{"auth.providers":[{"type":"github","clientID":"c","clientSecret":"s"}],
                    "batchChanges.rolloutWindows":[{"rate":"unlimited"}]}"#
            )
            .expect("safe"),
            r#"{"batchChanges.rolloutWindows":[{"rate":"unlimited"}]}"#
        );
        assert_eq!(
            return_safe_configs(r#"{"batchChanges.rolloutWindows":[]}"#).expect("safe"),
            r#"{"batchChanges.rolloutWindows":[]}"#
        );
        assert_eq!(
            return_safe_configs(r#"{"batchChanges.rolloutWindows":null}"#).expect("safe"),
            "{}"
        );
    }

    #[test]
    fn return_safe_configs_normalizes_rollout_window_fields() {
        assert_eq!(
            return_safe_configs(
                r#"{"batchChanges.rolloutWindows":[
                    {"days":["Monday","Tuesday"],"end":"18:00","rate":5,"start":"09:00","unknown":1}
                ]}"#
            )
            .expect("safe"),
            r#"{"batchChanges.rolloutWindows":[{"days":["Monday","Tuesday"],"end":"18:00","rate":5,"start":"09:00"}]}"#
        );
        // Empty optional values are omitted; `rate` is always present.
        assert_eq!(
            return_safe_configs(
                r#"{"batchChanges.rolloutWindows":[{"days":[],"end":"","start":"","rate":null}]}"#
            )
            .expect("safe"),
            r#"{"batchChanges.rolloutWindows":[{"rate":null}]}"#
        );
        assert_eq!(
            return_safe_configs(r#"{"batchChanges.rolloutWindows":[null,{"start":"10:00"}]}"#)
                .expect("safe"),
            r#"{"batchChanges.rolloutWindows":[null,{"rate":null,"start":"10:00"}]}"#
        );
    }

    #[test]
    fn return_safe_configs_reports_parse_errors() {
        assert!(return_safe_configs("{")
            .expect_err("invalid")
            .starts_with("parse config"));
        assert!(
            return_safe_configs(r#"{"batchChanges.rolloutWindows":{"rate":"x"}}"#)
                .expect_err("not an array")
                .starts_with("parse config")
        );
        assert!(
            return_safe_configs(r#"{"batchChanges.rolloutWindows":[{"days":"x"}]}"#)
                .expect_err("days type")
                .starts_with("parse config")
        );
    }

    #[test]
    fn redact_site_config_reports_parse_errors_and_reuses_m9gn_redaction() {
        assert!(redact_site_config("{")
            .expect_err("invalid")
            .starts_with("parse config"));

        let redacted = redact_site_config(
            r#"{"executors.accessToken":"secret","externalURL":"https://example.com"}"#,
        )
        .expect("redacted");
        let value: Value = serde_json::from_str(&redacted).expect("json");
        assert_eq!(value["executors.accessToken"], "REDACTED");
        assert_eq!(value["externalURL"], "https://example.com");
    }

    #[tokio::test]
    async fn admin_logs_view_event_and_gets_the_latest_id() {
        let mut backend = FakeBackend::admin();
        backend.auditlog_expansion = true;
        let resolver = configuration(&backend, user(), Some(false))
            .await
            .expect("configuration");
        assert_eq!(
            backend.events(),
            vec![SecurityEvent {
                name: SECURITY_EVENT_NAME_SITE_CONFIG_VIEWED,
                url: String::new(),
                user_id: 42,
                anonymous_user_id: String::new(),
                source: "BACKEND",
                argument: Value::Null,
            }]
        );
        assert_eq!(id(&backend, user(), &resolver).await.expect("id"), 7);
    }

    #[tokio::test]
    async fn admin_without_auditlog_logs_nothing() {
        let backend = FakeBackend::admin();
        configuration(&backend, user(), None)
            .await
            .expect("configuration");
        assert!(backend.events().is_empty());
    }

    #[tokio::test]
    async fn non_admin_safe_logs_redacted_view_and_skips_admin_only_fields() {
        let mut backend = FakeBackend::non_admin();
        backend.auditlog_expansion = true;
        let resolver = configuration(&backend, user(), Some(true))
            .await
            .expect("safe configuration");
        assert_eq!(
            backend.events(),
            vec![SecurityEvent {
                name: SECURITY_EVENT_NAME_SITE_CONFIG_REDACTED_VIEWED,
                url: String::new(),
                user_id: 42,
                anonymous_user_id: String::new(),
                source: "BACKEND",
                argument: Value::Null,
            }]
        );
        // `EffectiveContents` on the safe branch does not need admin rights.
        assert_eq!(
            effective_contents(&backend, user(), &resolver)
                .await
                .expect("safe contents"),
            "{}"
        );
        assert_eq!(
            id(&backend, user(), &resolver)
                .await
                .expect_err("admin only"),
            "must be site admin"
        );
    }

    #[tokio::test]
    async fn non_admin_unsafe_returns_the_auth_error_without_logging() {
        let mut backend = FakeBackend::non_admin();
        backend.auditlog_expansion = true;
        let err = configuration(&backend, user(), Some(false))
            .await
            .expect_err("must be denied");
        assert_eq!(err, "must be site admin");
        assert!(backend.events().is_empty());

        let err = configuration(&backend, Actor::Anonymous, None)
            .await
            .expect_err("must be denied");
        assert_eq!(err, "not authenticated");
        assert!(backend.events().is_empty());
    }

    #[tokio::test]
    async fn id_propagates_site_get_latest_errors() {
        let mut backend = FakeBackend::admin();
        backend.site_get_latest = Err("site config unavailable".to_owned());
        let resolver = configuration(&backend, user(), None)
            .await
            .expect("configuration");
        assert_eq!(
            id(&backend, user(), &resolver).await.expect_err("db error"),
            "site config unavailable"
        );
    }

    #[tokio::test]
    async fn effective_contents_redacts_on_the_admin_branch() {
        let mut backend = FakeBackend::admin();
        backend.raw_site = r#"{"executors.accessToken":"secret"}"#.to_owned();
        let resolver = configuration(&backend, user(), Some(false))
            .await
            .expect("configuration");
        let contents = effective_contents(&backend, user(), &resolver)
            .await
            .expect("contents");
        let value: Value = serde_json::from_str(&contents).expect("json");
        assert_eq!(value["executors.accessToken"], "REDACTED");
    }

    #[tokio::test]
    async fn effective_contents_unsafe_non_admin_is_denied_after_direct_construction() {
        let backend = FakeBackend::non_admin();
        let resolver = SiteConfigurationResolver {
            return_safe_configs_only: false,
        };
        assert_eq!(
            effective_contents(&backend, user(), &resolver)
                .await
                .expect_err("admin only"),
            "must be site admin"
        );
    }

    #[tokio::test]
    async fn log_failures_do_not_fail_the_resolver() {
        let mut backend = FakeBackend::admin();
        backend.auditlog_expansion = true;
        backend.fail_log = true;
        let resolver = configuration(&backend, user(), None)
            .await
            .expect("configuration still succeeds");
        assert_eq!(
            resolver,
            SiteConfigurationResolver {
                return_safe_configs_only: false
            }
        );
        assert_eq!(backend.events().len(), 1);
    }

    #[tokio::test]
    async fn effective_contents_error_path_returns_the_parse_error() {
        let mut backend = FakeBackend::admin();
        backend.raw_site = "{".to_owned();
        let resolver = configuration(&backend, user(), Some(false))
            .await
            .expect("configuration");
        assert!(effective_contents(&backend, user(), &resolver)
            .await
            .expect_err("parse error")
            .starts_with("parse config"));

        let resolver = configuration(&backend, user(), Some(true))
            .await
            .expect("configuration");
        assert!(effective_contents(&backend, user(), &resolver)
            .await
            .expect_err("parse error")
            .starts_with("parse config"));
    }

    #[test]
    fn rollout_window_json_shape_matches_go() {
        // Guard the key order: Go's struct order (`days,end,rate,start`) is
        // lexicographic, so the BTreeMap output matches byte-for-byte.
        let value: Value = serde_json::from_str(
            &return_safe_configs(
                r#"{"batchChanges.rolloutWindows":[
                    {"start":"09:00","days":["Mon"],"rate":1,"end":"17:00"}]}"#,
            )
            .expect("safe"),
        )
        .expect("json");
        assert_eq!(
            value,
            json!({"batchChanges.rolloutWindows": [
                {"days": ["Mon"], "end": "17:00", "rate": 1, "start": "09:00"}
            ]})
        );
    }
}
