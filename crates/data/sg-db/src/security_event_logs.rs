#![forbid(unsafe_code)]

//! `internal/database/security_event_logs.go`: the write path used by the
//! `auditlog-expansion` feature. Only the database location is ported (see
//! `docs/overviews/m9_8gn_update_site_configuration_overview.md` §4.9): the
//! baseline writes rows when `log.securityEventLog.location` is `database` or
//! `all`, and that decision stays with the caller (it needs the parsed site
//! configuration, which `sg-db` does not depend on).

use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;

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

/// `SecurityEventLogs().LogSecurityEvent`'s INSERT
/// (`internal/database/security_event_logs.go:242-272`).
pub async fn insert_security_event(
    pool: &PgPool,
    event: &SecurityEvent<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO security_event_logs(name, url, user_id, anonymous_user_id, \
         source, argument, version, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(event.name)
    .bind(event.url)
    .bind(event.user_id)
    .bind(event.anonymous_user_id)
    .bind(event.source)
    .bind(event.argument.clone())
    .bind(event.version)
    .bind(event.timestamp)
    .execute(pool)
    .await
    .map(|_| ())
}

/// `audit.SecurityEventLocation(conf.SiteConfig()) == database || all`
/// (`internal/audit/security_events.go:14-36`, default `auditlog`). The
/// caller passes the parsed site config's raw JSON value.
pub fn security_event_logs_in_database(site: &Value) -> bool {
    let Some(log) = site.get("log") else {
        return false;
    };
    let Some(security_event_log) = log.get("securityEventLog") else {
        return false;
    };
    matches!(
        security_event_log.get("location").and_then(Value::as_str),
        Some("database" | "all")
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
}
