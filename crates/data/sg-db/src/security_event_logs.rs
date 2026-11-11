#![forbid(unsafe_code)]

//! `internal/database/security_event_logs.go`: the write path used by the
//! `auditlog-expansion` feature. Only the persistence itself lives here; the
//! location decision is exposed as [`security_event_location`] so callers
//! with the parsed site configuration (which `sg-db` does not depend on) can
//! gate the database and audit-log sinks exactly like the baseline.

use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, QueryBuilder};

/// `audit.SecurityEventsLocation` (`internal/audit/security_events.go:5-12`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityEventLocation {
    None,
    AuditLog,
    Database,
    All,
}

/// `database.SecurityEventNameSiteConfigUpdated`.
pub const SECURITY_EVENT_NAME_SITE_CONFIG_UPDATED: &str = "SiteConfigUpdated";

/// One `security_event_logs` row (`database.SecurityEvent`).
#[derive(Debug, Clone)]
pub struct SecurityEvent<'a> {
    pub name: &'a str,
    pub url: &'a str,
    pub user_id: i32,
    pub anonymous_user_id: &'a str,
    pub source: &'a str,
    /// `json.Marshal` of the event's arguments; stored in the `jsonb`
    /// `argument` column exactly like `users.rs`'s account-deletion events.
    pub argument: Value,
    pub version: &'a str,
    pub timestamp: OffsetDateTime,
}

/// `securityEventLogsStore.InsertList`'s single multi-row INSERT
/// (`internal/database/security_event_logs.go:226-232`): the baseline joins
/// one `(name,url,...)` tuple per event with commas.
pub async fn insert_security_events(
    pool: &PgPool,
    events: &[SecurityEvent<'_>],
) -> Result<(), sqlx::Error> {
    if events.is_empty() {
        return Ok(());
    }
    let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
        "INSERT INTO security_event_logs(name, url, user_id, anonymous_user_id, \
         source, argument, version, timestamp) ",
    );
    builder.push_values(events, |mut builder, event| {
        builder
            .push_bind(event.name)
            .push_bind(event.url)
            .push_bind(event.user_id)
            .push_bind(event.anonymous_user_id)
            .push_bind(event.source)
            .push_bind(event.argument.clone())
            .push_bind(event.version)
            .push_bind(event.timestamp);
    });
    builder.build().execute(pool).await.map(|_| ())
}

/// `SecurityEventLogs().LogSecurityEvent`'s INSERT (a one-event
/// [`insert_security_events`]).
pub async fn insert_security_event(
    pool: &PgPool,
    event: &SecurityEvent<'_>,
) -> Result<(), sqlx::Error> {
    insert_security_events(pool, std::slice::from_ref(event)).await
}

/// `audit.SecurityEventLocation(conf.SiteConfig())`
/// (`internal/audit/security_events.go:14-29`): `land`/`log` nil-safe, known
/// values mapped, everything else (including a missing `location`) defaults
/// to `auditlog`.
pub fn security_event_location(site: &Value) -> SecurityEventLocation {
    let Some(security_event_log) = site
        .get("log")
        .and_then(|log| log.get("securityEventLog"))
        .filter(|value| !value.is_null())
    else {
        return SecurityEventLocation::AuditLog;
    };
    match security_event_log.get("location").and_then(Value::as_str) {
        Some("none") => SecurityEventLocation::None,
        Some("auditlog") => SecurityEventLocation::AuditLog,
        Some("database") => SecurityEventLocation::Database,
        Some("all") => SecurityEventLocation::All,
        _ => SecurityEventLocation::AuditLog,
    }
}

/// `audit.SecurityEventLocation(...) == database || all`. The caller passes
/// the parsed site config's raw JSON value.
pub fn security_event_logs_in_database(site: &Value) -> bool {
    matches!(
        security_event_location(site),
        SecurityEventLocation::Database | SecurityEventLocation::All
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn location_gate_matches_the_baseline() {
        assert!(!security_event_logs_in_database(&json!({})));
        assert!(!security_event_logs_in_database(
            &json!({"log": {"securityEventLog": null}})
        ));
        assert!(!security_event_logs_in_database(
            &json!({"log": {"securityEventLog": {}}})
        ));
        assert!(!security_event_logs_in_database(
            &json!({"log": {"securityEventLog": {"location": "none"}}})
        ));
        assert!(!security_event_logs_in_database(
            &json!({"log": {"securityEventLog": {"location": "auditlog"}}})
        ));
        assert!(security_event_logs_in_database(
            &json!({"log": {"securityEventLog": {"location": "database"}}})
        ));
        assert!(security_event_logs_in_database(
            &json!({"log": {"securityEventLog": {"location": "all"}}})
        ));
    }

    #[test]
    fn location_enum_defaults_to_auditlog() {
        assert_eq!(
            security_event_location(&json!({})),
            SecurityEventLocation::AuditLog
        );
        assert_eq!(
            security_event_location(&json!({"log": {"securityEventLog": null}})),
            SecurityEventLocation::AuditLog
        );
        assert_eq!(
            security_event_location(&json!({"log": {"securityEventLog": {}}})),
            SecurityEventLocation::AuditLog
        );
        assert_eq!(
            security_event_location(&json!({"log": {"securityEventLog": {"location": "bogus"}}})),
            SecurityEventLocation::AuditLog
        );
        assert_eq!(
            security_event_location(&json!({"log": {"securityEventLog": {"location": "none"}}})),
            SecurityEventLocation::None
        );
        assert_eq!(
            security_event_location(
                &json!({"log": {"securityEventLog": {"location": "auditlog"}}})
            ),
            SecurityEventLocation::AuditLog
        );
        assert_eq!(
            security_event_location(
                &json!({"log": {"securityEventLog": {"location": "database"}}})
            ),
            SecurityEventLocation::Database
        );
        assert_eq!(
            security_event_location(&json!({"log": {"securityEventLog": {"location": "all"}}})),
            SecurityEventLocation::All
        );
    }
}
