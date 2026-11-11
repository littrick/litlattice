#![forbid(unsafe_code)]

//! The downstream half of `security_event_logs`
//! (`internal/database/security_event_logs.go:163-240`): everything
//! `securityEventLogsStore.InsertList` does *around* the INSERT —
//! location gating, internal-actor handling, Sourcegraph Operator attribution
//! and the `audit.Log` mirror — kept as a pure preparation step so the
//! differential harness can replay it without a database.
//!
//! Baseline `InsertList` order:
//!
//! 1. `audit.SecurityEventLocation(conf.SiteConfig())`; `none` → neither sink;
//! 2. per event: Operator `jsonc.Edit(argument, true,
//!    "sourcegraph_operator")`; internal actor + no user → skip the whole list
//!    unless `log.auditLog.internalTraffic`, otherwise anonymous UID
//!    `"internal"`;
//! 3. `database`/`all` → one multi-row INSERT ([`write_prepared_security_events`]);
//! 4. `auditlog`/`all` → one `audit.Log` per event with `Entity =
//!    "security events"` (`crate::audit::build_security_event_record`).
//!
//! Errors from `LogEvent` are logged, not returned; this module mirrors the
//! error *return* of `InsertList` (used by `LogSecurityEvent`'s caller).
//!
//! SAMS has no call in this baseline path. The cloud Session-sign-out
//! revocation stays behind the injected `SsoRemoteSignOut` seam (M9.8iv);
//! [`SamsSecurityEventForwarder`] is the analogous no-op seam for products
//! that forward security events to Sourcegraph Accounts.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{json, Value};
use sqlx::PgPool;
use time::OffsetDateTime;

use sg_config::SiteConfiguration;
use sg_db::security_event_logs::{
    insert_security_events, security_event_location, SecurityEvent, SecurityEventLocation,
};

use crate::audit::{
    build_security_event_record, is_enabled, log_filtered, AuditActor, AuditLogSetting,
    AuditRecord, ClientInfo, SecurityEventAuditFields,
};
use crate::settings_merge::marshal_go;

// ---------------------------------------------------------------------------
// Event names (`internal/database/security_event_logs.go:24-119`)
// ---------------------------------------------------------------------------

pub const SECURITY_EVENT_SIGN_OUT_ATTEMPTED: &str = "SignOutAttempted";
pub const SECURITY_EVENT_SIGN_OUT_FAILED: &str = "SignOutFailed";
pub const SECURITY_EVENT_SIGN_OUT_SUCCEEDED: &str = "SignOutSucceeded";

pub const SECURITY_EVENT_SIGN_IN_ATTEMPTED: &str = "SignInAttempted";
pub const SECURITY_EVENT_SIGN_IN_FAILED: &str = "SignInFailed";
pub const SECURITY_EVENT_SIGN_IN_SUCCEEDED: &str = "SignInSucceeded";

pub const SECURITY_EVENT_ACCESS_TOKEN_CREATED: &str = "AccessTokenCreated";
pub const SECURITY_EVENT_ACCESS_TOKEN_DELETED: &str = "AccessTokenDeleted";
pub const SECURITY_EVENT_ACCESS_TOKEN_HARD_DELETED: &str = "AccessTokenHardDeleted";
pub const SECURITY_EVENT_ACCESS_TOKEN_IMPERSONATED: &str = "AccessTokenImpersonated";
pub const SECURITY_EVENT_ACCESS_TOKEN_INVALID: &str = "AccessTokenInvalid";
pub const SECURITY_EVENT_ACCESS_TOKEN_SUBJECT_NOT_SITE_ADMIN: &str =
    "AccessTokenSubjectNotSiteAdmin";

/// `event_logs.EventLogsSourcegraphOperatorKey`.
pub const EVENT_LOGS_SOURCEGRAPH_OPERATOR_KEY: &str = "sourcegraph_operator";

/// The anonymous UID `InsertList` assigns to user-less internal events.
pub const INTERNAL_ANONYMOUS_USER_ID: &str = "internal";

// ---------------------------------------------------------------------------
// Token argument builders (`internal/database/access_tokens.go`)
// ---------------------------------------------------------------------------

/// `AccessTokenCreated`'s `arg` struct (`access_tokens.go:244-258`).
pub fn access_token_created_argument(
    subject_user_id: i32,
    creator_user_id: i32,
    scopes: &[String],
    note: &str,
    expires_at: OffsetDateTime,
) -> Value {
    json!({
        "subject_user_id": subject_user_id,
        "creator_user_id": creator_user_id,
        "scopes": scopes,
        "note": note,
        "expires_at": crate::audit::format_rfc3339_nano(expires_at),
    })
}

/// `AccessTokenDeleted`/`AccessTokenHardDeleted`'s `arg` struct
/// (`access_tokens.go:497-500,519-522`).
pub fn access_token_id_argument(id: i64) -> Value {
    json!({ "access_token_id": id })
}

/// `AccessTokenDeleted` by token value: Go marshals the `[]byte` hash as
/// standard base64 (`access_tokens.go:543-549`).
pub fn access_token_sha256_argument(hash: &[u8]) -> Value {
    json!({
        "access_token_sha256": base64::engine::general_purpose::STANDARD.encode(hash),
    })
}

// ---------------------------------------------------------------------------
// Preparation
// ---------------------------------------------------------------------------

/// `audit.SecurityEventLocation` plus the `internalTraffic` gate `InsertList`
/// reads from the same site configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityEventPolicy {
    pub location: SecurityEventLocation,
    pub internal_traffic: bool,
}

impl SecurityEventPolicy {
    pub fn from_site(site: &SiteConfiguration) -> Self {
        Self {
            location: security_event_location(site.raw()),
            internal_traffic: is_enabled(site, AuditLogSetting::InternalTraffic),
        }
    }

    /// `loc == database || all`.
    pub fn writes_database(self) -> bool {
        matches!(
            self.location,
            SecurityEventLocation::Database | SecurityEventLocation::All
        )
    }

    /// `loc == auditlog || all`.
    pub fn writes_audit_log(self) -> bool {
        matches!(
            self.location,
            SecurityEventLocation::AuditLog | SecurityEventLocation::All
        )
    }
}

/// One event as constructed by a call site before `LogSecurityEvent`'s
/// `json.Marshal(arguments)`. `argument == None` and `Some(Value::Null)` both
/// marshal to `"null"` and become `"{}"` in the row/audit field.
#[derive(Debug, Clone, PartialEq)]
pub struct SecurityEventInput {
    pub name: String,
    pub url: String,
    pub user_id: i32,
    pub anonymous_user_id: String,
    pub source: String,
    pub argument: Option<Value>,
    pub timestamp: OffsetDateTime,
}

impl SecurityEventInput {
    pub fn new(
        name: impl Into<String>,
        url: impl Into<String>,
        user_id: i32,
        anonymous_user_id: impl Into<String>,
        source: impl Into<String>,
        argument: Option<Value>,
        timestamp: OffsetDateTime,
    ) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            user_id,
            anonymous_user_id: anonymous_user_id.into(),
            source: source.into(),
            argument,
            timestamp,
        }
    }
}

/// One row as it will be inserted / audited.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedSecurityEvent {
    pub name: String,
    pub url: String,
    pub user_id: i32,
    pub anonymous_user_id: String,
    pub source: String,
    /// `argumentToJSONString()` (after the Operator edit).
    pub argument: String,
    pub version: String,
    pub timestamp: OffsetDateTime,
}

/// The outcome of `InsertList`'s preprocessing. `skipped` is the baseline's
/// early `return nil` (`none` location, or a user-less internal actor with
/// internal traffic disabled).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PreparedSecurityEventList {
    pub skipped: bool,
    pub database: bool,
    pub rows: Vec<PreparedSecurityEvent>,
    pub audits: Vec<AuditRecord>,
}

impl PreparedSecurityEventList {
    /// Emits the `auditlog` sink at the `SRC_LOG_LEVEL`-derived severity.
    pub fn emit_audits(&self, log_level: &str) {
        for record in &self.audits {
            record.emit(log_level);
        }
    }
}

fn argument_to_json_string(argument: &Option<Value>) -> String {
    let text = match argument {
        None => "null".to_owned(),
        Some(value) => marshal_go(value),
    };
    // `argumentToJSONString`: nil and the literal `null` both become `{}`.
    if text == "null" {
        "{}".to_owned()
    } else {
        text
    }
}

/// `jsonc.Edit(input, true, "sourcegraph_operator")` with
/// `internal/jsonc.DefaultFormatOptions` (`InsertSpaces: true`, `TabSize: 2`,
/// no `EOL` field set).
fn edit_sourcegraph_operator(input: &str) -> Result<String, String> {
    let options = sg_jsonx::FormatOptions {
        tab_size: 2,
        insert_spaces: true,
        eol: String::new(),
    };
    let path = sg_jsonx::property_path(&[EVENT_LOGS_SOURCEGRAPH_OPERATOR_KEY]);
    let edits = sg_jsonx::compute_property_edit(
        input,
        &path,
        sg_jsonx::EditValue::Json("true".to_owned()),
        &options,
    )?;
    sg_jsonx::apply_edits(input, &edits)
}

/// `securityEventLogsStore.InsertList`'s preprocessing. The audit-ID generator
/// is only called for events that actually produce an `audit.Log` record.
#[allow(clippy::too_many_arguments)]
pub fn prepare_security_events(
    site: &SiteConfiguration,
    policy: SecurityEventPolicy,
    actor: &AuditActor,
    sourcegraph_operator: bool,
    client: Option<&ClientInfo>,
    events: &[SecurityEventInput],
    version: &str,
    mut audit_id: impl FnMut() -> String,
) -> Result<PreparedSecurityEventList, String> {
    if policy.location == SecurityEventLocation::None {
        return Ok(PreparedSecurityEventList {
            skipped: true,
            ..PreparedSecurityEventList::default()
        });
    }

    let mut rows = Vec::with_capacity(events.len());
    for event in events {
        let argument = argument_to_json_string(&event.argument);
        let argument = if sourcegraph_operator {
            edit_sourcegraph_operator(&argument)
                .map_err(|error| format!("edit \"argument\" for Sourcegraph operator: {error}"))?
        } else {
            argument
        };

        let no_user = event.user_id == 0 && event.anonymous_user_id.is_empty();
        let mut anonymous_user_id = event.anonymous_user_id.clone();
        if actor.internal && no_user {
            // only log internal access if we are explicitly configured to do so
            if !policy.internal_traffic {
                return Ok(PreparedSecurityEventList {
                    skipped: true,
                    ..PreparedSecurityEventList::default()
                });
            }
            anonymous_user_id = INTERNAL_ANONYMOUS_USER_ID.to_owned();
        }

        rows.push(PreparedSecurityEvent {
            name: event.name.clone(),
            url: event.url.clone(),
            user_id: event.user_id,
            anonymous_user_id,
            source: event.source.clone(),
            argument,
            version: version.to_owned(),
            timestamp: event.timestamp,
        });
    }

    let mut audits = Vec::new();
    if policy.writes_audit_log() && log_filtered(site, actor, client) {
        for row in &rows {
            let fields = SecurityEventAuditFields {
                url: row.url.clone(),
                user_id: row.user_id,
                anonymous_user_id: row.anonymous_user_id.clone(),
                source: row.source.clone(),
                argument: row.argument.clone(),
                version: row.version.clone(),
                timestamp: row.timestamp,
            };
            if let Some(record) =
                build_security_event_record(site, actor, client, &row.name, &fields, &audit_id())
            {
                audits.push(record);
            }
        }
    }

    Ok(PreparedSecurityEventList {
        skipped: false,
        database: policy.writes_database(),
        rows,
        audits,
    })
}

/// The `database`/`all` sink: one multi-row INSERT of the prepared rows.
pub async fn write_prepared_security_events(
    pool: &PgPool,
    prepared: &PreparedSecurityEventList,
) -> Result<(), sqlx::Error> {
    if !prepared.database || prepared.rows.is_empty() {
        return Ok(());
    }
    let events: Vec<SecurityEvent<'_>> = prepared
        .rows
        .iter()
        .map(|row| SecurityEvent {
            name: &row.name,
            url: &row.url,
            user_id: row.user_id,
            anonymous_user_id: &row.anonymous_user_id,
            source: &row.source,
            argument: Value::String(row.argument.clone()),
            version: &row.version,
            timestamp: row.timestamp,
        })
        .collect();
    insert_security_events(pool, &events).await
}

// ---------------------------------------------------------------------------
// SAMS seam
// ---------------------------------------------------------------------------

/// Downstream Sourcegraph Accounts (`SAMS`) consumer seam. The baseline
/// `security_event_logs` path makes no SAMS call; products that do forward
/// events inject an implementation, and the default stays a no-op so tests
/// never touch the network.
#[async_trait]
pub trait SamsSecurityEventForwarder: Send + Sync {
    async fn forward(&self, event: &PreparedSecurityEvent) -> Result<(), String>;
}

/// Default no-op SAMS forwarder.
pub struct NoopSamsSecurityEventForwarder;

#[async_trait]
impl SamsSecurityEventForwarder for NoopSamsSecurityEventForwarder {
    async fn forward(&self, _event: &PreparedSecurityEvent) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_config::SiteConfiguration;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    fn at(timestamp: &str) -> OffsetDateTime {
        OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339)
            .expect("timestamp")
    }

    fn actor(uid: i32, anon: &str) -> AuditActor {
        AuditActor::new((uid > 0).then_some(uid), anon)
    }

    fn event(name: &str) -> SecurityEventInput {
        SecurityEventInput::new(
            name,
            "/sign-in",
            7,
            "",
            "BACKEND",
            None,
            at("2024-01-02T03:04:05Z"),
        )
    }

    fn prepare(
        site: &SiteConfiguration,
        actor: &AuditActor,
        operator: bool,
        events: &[SecurityEventInput],
    ) -> PreparedSecurityEventList {
        prepare_security_events(
            site,
            SecurityEventPolicy::from_site(site),
            actor,
            operator,
            None,
            events,
            "<version>",
            || "<uuid>".to_owned(),
        )
        .expect("prepare")
    }

    #[test]
    fn location_none_skips_both_sinks() {
        let site = site(r#"{"log":{"securityEventLog":{"location":"none"}}}"#);
        let prepared = prepare(&site, &actor(7, ""), false, &[event("SignInSucceeded")]);
        assert!(prepared.skipped);
        assert!(prepared.rows.is_empty());
        assert!(prepared.audits.is_empty());
    }

    #[test]
    fn location_database_and_all_select_sinks() {
        let database = prepare(
            &site(r#"{"log":{"securityEventLog":{"location":"database"}}}"#),
            &actor(7, ""),
            false,
            &[event("SignInSucceeded")],
        );
        assert!(database.database);
        assert!(database.audits.is_empty());

        let auditlog = prepare(
            &site(r#"{"log":{"securityEventLog":{"location":"auditlog"}}}"#),
            &actor(7, ""),
            false,
            &[event("SignInSucceeded")],
        );
        assert!(!auditlog.database);
        assert_eq!(auditlog.audits.len(), 1);

        let all = prepare(
            &site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#),
            &actor(7, ""),
            false,
            &[event("SignInSucceeded")],
        );
        assert!(all.database);
        assert_eq!(all.audits.len(), 1);
    }

    #[test]
    fn internal_without_user_is_skipped_or_anonymized() {
        let disabled = site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#);
        let internal = AuditActor::internal();
        let mut attempt = event("SignInAttempted");
        attempt.user_id = 0;
        let prepared = prepare(&disabled, &internal, false, &[attempt]);
        assert!(prepared.skipped);

        let enabled = site(
            r#"{"log":{"securityEventLog":{"location":"all"},"auditLog":{"internalTraffic":true}}}"#,
        );
        let mut attempt = event("SignInAttempted");
        attempt.user_id = 0;
        let prepared = prepare(&enabled, &internal, false, &[attempt]);
        assert!(!prepared.skipped);
        assert_eq!(prepared.rows[0].anonymous_user_id, "internal");
        assert_eq!(prepared.audits.len(), 1);
    }

    #[test]
    fn internal_with_user_writes_row_but_not_audit_without_internal_traffic() {
        let site = site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#);
        let prepared = prepare(
            &site,
            &AuditActor::internal(),
            false,
            &[event("SignInSucceeded")],
        );
        assert!(!prepared.skipped);
        assert!(prepared.database);
        assert_eq!(prepared.rows.len(), 1);
        assert!(prepared.audits.is_empty());
    }

    #[test]
    fn unknown_actor_and_nil_client_drop_only_the_audit_sink() {
        let mut attempt = event("SignInAttempted");
        attempt.user_id = 0;
        attempt.anonymous_user_id = String::new();
        let prepared = prepare(
            &site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#),
            &actor(0, ""),
            false,
            &[attempt.clone()],
        );
        assert!(prepared.database);
        assert_eq!(prepared.rows.len(), 1);
        assert!(prepared.audits.is_empty());

        let enabled = site(
            r#"{"log":{"securityEventLog":{"location":"all"},"auditLog":{"internalTraffic":true}}}"#,
        );
        let prepared = prepare(&enabled, &actor(0, ""), false, &[attempt]);
        assert_eq!(prepared.audits.len(), 1);
    }

    #[test]
    fn operator_argument_is_jsonc_edited() {
        let mut created = event("AccessTokenCreated");
        created.argument = Some(json!({"a": 1}));
        let prepared = prepare(
            &site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#),
            &actor(7, ""),
            true,
            &[created],
        );
        assert_eq!(
            prepared.rows[0].argument,
            "{\n  \"a\": 1,\n  \"sourcegraph_operator\": true\n}"
        );
        assert_eq!(
            prepared.audits[0].fields["event"]["argument"],
            prepared.rows[0].argument
        );
    }

    #[test]
    fn non_operator_argument_keeps_go_json_and_null_becomes_empty_object() {
        let mut nil = event("SignInSucceeded");
        nil.argument = None;
        let mut nul = event("SignInSucceeded");
        nul.argument = Some(Value::Null);
        let mut args = event("SignInSucceeded");
        args.argument = Some(json!({"b": 2, "a": "<x>&"}));
        let prepared = prepare(
            &site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#),
            &actor(7, ""),
            false,
            &[nil, nul, args],
        );
        assert_eq!(prepared.rows[0].argument, "{}");
        assert_eq!(prepared.rows[1].argument, "{}");
        assert_eq!(
            prepared.rows[2].argument,
            "{\"a\":\"\\u003cx\\u003e\\u0026\",\"b\":2}"
        );
    }

    #[test]
    fn multiple_events_share_one_preparation() {
        let prepared = prepare(
            &site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#),
            &actor(7, ""),
            false,
            &[event("SignInAttempted"), event("SignInSucceeded")],
        );
        assert_eq!(prepared.rows.len(), 2);
        assert_eq!(prepared.audits.len(), 2);
        assert_eq!(
            prepared.audits[0].message,
            "SignInAttempted (sampling immunity token: <uuid>)"
        );
    }

    #[test]
    fn token_argument_builders_match_go_marshalling() {
        assert_eq!(
            crate::settings_merge::marshal_go(&access_token_id_argument(42)),
            r#"{"access_token_id":42}"#
        );
        assert_eq!(
            crate::settings_merge::marshal_go(&access_token_sha256_argument(&[0xde, 0xad])),
            r#"{"access_token_sha256":"3q0="}"#
        );
        assert_eq!(
            crate::settings_merge::marshal_go(&access_token_created_argument(
                1,
                2,
                &["user:all".to_owned()],
                "note",
                at("2024-01-02T03:04:05Z"),
            )),
            r#"{"creator_user_id":2,"expires_at":"2024-01-02T03:04:05Z","note":"note","scopes":["user:all"],"subject_user_id":1}"#
        );
    }

    #[test]
    fn invalid_operator_argument_reports_the_baseline_wrapping() {
        let mut invalid = event("SignInSucceeded");
        invalid.argument = Some(json!([1, 2]));
        let result = prepare_security_events(
            &site(r#"{"log":{"securityEventLog":{"location":"all"}}}"#),
            SecurityEventPolicy::from_site(&site(
                r#"{"log":{"securityEventLog":{"location":"all"}}}"#,
            )),
            &actor(7, ""),
            true,
            None,
            &[invalid],
            "<version>",
            || "<uuid>".to_owned(),
        );
        assert!(result
            .expect_err("array cannot take a property")
            .starts_with("edit \"argument\" for Sourcegraph operator:"));
    }

    #[test]
    fn go_time_and_rfc3339_formatting() {
        let plain = at("2024-01-02T03:04:05Z");
        assert_eq!(
            crate::audit::format_go_time(plain),
            "2024-01-02 03:04:05 +0000 UTC"
        );
        assert_eq!(
            crate::audit::format_rfc3339_nano(plain),
            "2024-01-02T03:04:05Z"
        );
        let fractional = at("2024-01-02T03:04:05.123456789Z");
        assert_eq!(
            crate::audit::format_go_time(fractional),
            "2024-01-02 03:04:05.123456789 +0000 UTC"
        );
        assert_eq!(
            crate::audit::format_rfc3339_nano(fractional),
            "2024-01-02T03:04:05.123456789Z"
        );
        let offset = at("2024-01-02T05:04:05.120+02:00");
        assert_eq!(
            crate::audit::format_go_time(offset),
            "2024-01-02 03:04:05.12 +0000 UTC"
        );
    }
}
