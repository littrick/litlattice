//! Port of `internal/audit` and the GraphQL caller `recordAuditLog`
//! (`cmd/frontend/internal/httpapi/graphql.go:275-301`).
//!
//! The baseline writes a structured zap event with an `audit` object
//! (`auditId`, `action`, `entity`, `actor.*`) plus the caller's fields
//! (`request.*`, `mutation`, `successful`). Here the same tree is built as
//! JSON and emitted as a single `audit` field on a tracing event at the level
//! selected by `SRC_LOG_LEVEL`; see the overview for the sink-shape note.

use http::HeaderMap;
use serde_json::{Map, Value};
use uuid::Uuid;

use sg_config::SiteConfiguration;

use crate::settings_merge::marshal_go;

/// `unknown` used for a missing actor ID or a nil `requestclient.Client`.
pub const UNKNOWN: &str = "unknown";

/// `audit.AuditLogSetting`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditLogSetting {
    GitserverAccess,
    InternalTraffic,
    GraphQL,
}

/// `schema.AuditLog` (`schema/schema.go:78-90`). `severityLevel` is
/// deprecated in the baseline and has no effect, so it is not modelled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuditLogConfig {
    pub gitserver_access: bool,
    pub graphql: bool,
    pub internal_traffic: bool,
}

/// `audit.getAuditCfg` (`internal/audit/audit.go:155-160`): `nil` when
/// `siteConfig.log` or `siteConfig.log.auditLog` is absent/null.
pub fn audit_log_config(site: &SiteConfiguration) -> Option<AuditLogConfig> {
    let audit = site.raw().get("log")?.get("auditLog")?;
    if audit.is_null() {
        return None;
    }
    let flag = |name: &str| audit.get(name).and_then(Value::as_bool).unwrap_or(false);
    Some(AuditLogConfig {
        gitserver_access: flag("gitserverAccess"),
        graphql: flag("graphQL"),
        internal_traffic: flag("internalTraffic"),
    })
}

/// `audit.IsEnabled` (`audit.go:123-134`): every setting defaults to false,
/// including when the `auditLog` block is missing.
pub fn is_enabled(site: &SiteConfiguration, setting: AuditLogSetting) -> bool {
    let Some(config) = audit_log_config(site) else {
        return false;
    };
    match setting {
        AuditLogSetting::GitserverAccess => config.gitserver_access,
        AuditLogSetting::InternalTraffic => config.internal_traffic,
        AuditLogSetting::GraphQL => config.graphql,
    }
}

/// `requestclient.Client` (`internal/requestclient/http.go:90-135`) reduced to
/// the four values the audit log reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientInfo {
    pub ip: String,
    pub forwarded_for: String,
    pub user_agent: String,
    pub forwarded_for_user_agent: String,
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default()
}

/// Builds the request-client view. `remote_addr` is Go's
/// `http.Request.RemoteAddr` (the `sg_http::RemoteAddr` extension); `None`
/// corresponds to a context without the requestclient middleware, i.e. a nil
/// `*requestclient.Client`.
pub fn client_info(headers: &HeaderMap, remote_addr: Option<&str>) -> Option<ClientInfo> {
    let remote_addr = remote_addr?;
    let user_agent = header_value(headers, "user-agent");
    let forwarded_agent = header_value(headers, "x-forwarded-for-user-agent");
    let forwarded_for_user_agent = if forwarded_agent.is_empty() {
        user_agent.clone()
    } else {
        forwarded_agent
    };
    // `strings.Join(Header.Values("X-Forwarded-For"), ",")`: repeated header
    // fields are concatenated with a bare comma, in arrival order.
    let forwarded_for = headers
        .get_all("x-forwarded-for")
        .iter()
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .collect::<Vec<_>>()
        .join(",");
    Some(ClientInfo {
        // `strings.Split(req.RemoteAddr, ":")[0]`; IPv6 keeps the baseline's
        // leading `[`.
        ip: remote_addr.split(':').next().unwrap_or_default().to_owned(),
        forwarded_for,
        user_agent,
        forwarded_for_user_agent,
    })
}

/// The parts of `actor.Actor` that `audit.actorId` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditActor {
    pub uid: Option<i32>,
    pub anonymous_uid: String,
    /// `actor.Actor.Internal`: only internal service actors set it. The
    /// internal HTTP listener's actor middleware is not ported, so production
    /// requests are never internal; the field keeps the baseline filter
    /// testable.
    pub internal: bool,
}

impl AuditActor {
    pub fn new(uid: Option<i32>, anonymous_uid: impl Into<String>) -> Self {
        Self {
            uid,
            anonymous_uid: anonymous_uid.into(),
            internal: false,
        }
    }

    pub fn internal() -> Self {
        Self {
            uid: None,
            anonymous_uid: String::new(),
            internal: true,
        }
    }

    /// `audit.actorId` (`audit.go:61-69`): UID first, then the anonymous UID,
    /// then `unknown`. Note that the `X-Forwarded-For` fallback used by
    /// `getUID` does **not** participate.
    pub fn id(&self) -> String {
        if let Some(uid) = self.uid {
            if uid > 0 {
                return uid.to_string();
            }
        }
        if !self.anonymous_uid.is_empty() {
            return self.anonymous_uid.clone();
        }
        UNKNOWN.to_owned()
    }
}

fn client_ip(client: Option<&ClientInfo>) -> &str {
    client.map(|client| client.ip.as_str()).unwrap_or(UNKNOWN)
}

/// `graphQLQueryParams.Variables` plus the surrounding `traceData` fields
/// consumed by `recordAuditLog`.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphqlAuditContext {
    pub actor: AuditActor,
    pub client: Option<ClientInfo>,
    pub query: String,
    /// `toJson(data.queryParams.Variables)`: already serialized because the
    /// nil map vs. `{}` distinction matters (`"null"` vs `"{}"`).
    pub variables_json: String,
    pub request_name: String,
    pub request_source: String,
    pub successful: bool,
}

impl GraphqlAuditContext {
    /// `toJson` (`graphql.go:295-301`) for a decoded `Variables` field.
    /// `None` is Go's nil map (`"null"`); `Some({})` is `"{}"`. The Go
    /// marshal-error fallback is unreachable for JSON-decoded values.
    pub fn variables_json(variables: Option<&Map<String, Value>>) -> String {
        match variables {
            None => "null".to_owned(),
            Some(variables) => marshal_go(&Value::Object(variables.clone())),
        }
    }
}

/// A built audit event: the message and field tree plus the generated ID.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditRecord {
    pub audit_id: String,
    pub message: String,
    pub fields: Value,
}

/// The level selected by `getLoggerFuncWithSeverity` (`audit.go:139-153`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditSeverity {
    Debug,
    Info,
    Warn,
    Error,
}

impl AuditSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// `getLoggerFuncWithSeverity`: an exact (lowercased) switch on
/// `SRC_LOG_LEVEL`; `dbug`, `crit`, `none` and invalid values all fall through
/// to `warn`, unlike `log.Level.Parse`.
pub fn logger_severity(log_level: &str) -> AuditSeverity {
    match log_level.to_lowercase().as_str() {
        "debug" => AuditSeverity::Debug,
        "info" => AuditSeverity::Info,
        "warn" => AuditSeverity::Warn,
        "error" => AuditSeverity::Error,
        _ => AuditSeverity::Warn,
    }
}

/// The `SRC_LOG_LEVEL` default from `internal/env/env.go:32`.
pub const DEFAULT_LOG_LEVEL: &str = "warn";

/// `env.LogLevel`: read once per process in the baseline; the Rust service
/// reads it here because it has no global env snapshot. The baseline's
/// per-process override (`env.Get` checks `SRC_LOG_LEVEL_<program>` first,
/// here `frontend`) is honoured.
pub fn log_level_from_env() -> String {
    std::env::var("SRC_LOG_LEVEL_frontend")
        .or_else(|_| std::env::var("SRC_LOG_LEVEL"))
        .unwrap_or_else(|_| DEFAULT_LOG_LEVEL.to_owned())
}

/// `recordAuditLog` + `audit.Log` gating, with a caller-provided audit ID
/// (the baseline's test-only `auditIDGenerator`).
pub fn build_graphql_record(
    site: &SiteConfiguration,
    context: &GraphqlAuditContext,
    audit_id: &str,
) -> Option<AuditRecord> {
    if !should_log(site, &context.actor, context.client.as_ref()) {
        return None;
    }

    let actor = &context.actor;
    let client = context.client.as_ref();
    let audit = serde_json::json!({
        "auditId": audit_id,
        "action": "request",
        "entity": "GraphQL",
        "actor": {
            "actorUID": actor.id(),
            "ip": client_ip(client),
            "userAgent": client.map(|c| c.user_agent.as_str()).unwrap_or(UNKNOWN),
            "forwardedForUserAgent": client
                .map(|c| c.forwarded_for_user_agent.as_str())
                .unwrap_or(UNKNOWN),
            "X-Forwarded-For": client.map(|c| c.forwarded_for.as_str()).unwrap_or(UNKNOWN),
        },
    });
    let request = serde_json::json!({
        "name": context.request_name,
        "source": context.request_source,
        "variables": context.variables_json,
        "query": context.query,
    });
    let fields = serde_json::json!({
        "audit": audit,
        "request": request,
        "mutation": context.query.contains("mutation"),
        "successful": context.successful,
    });

    Some(AuditRecord {
        audit_id: audit_id.to_owned(),
        message: format!("request (sampling immunity token: {audit_id})"),
        fields,
    })
}

/// `build_graphql_record` with a fresh UUID v4, as `audit.Log` generates.
pub fn graphql_record(
    site: &SiteConfiguration,
    context: &GraphqlAuditContext,
) -> Option<AuditRecord> {
    build_graphql_record(site, context, &Uuid::new_v4().to_string())
}

/// `recordAuditLog`'s `audit.IsEnabled(..., GraphQL)` check followed by
/// `audit.Log`'s two filters (`audit.go:21-39`).
pub fn should_log(
    site: &SiteConfiguration,
    actor: &AuditActor,
    client: Option<&ClientInfo>,
) -> bool {
    if !is_enabled(site, AuditLogSetting::GraphQL) {
        return false;
    }
    let internal_traffic = is_enabled(site, AuditLogSetting::InternalTraffic);
    // Internal actors generate a large volume of noise.
    if actor.internal && !internal_traffic {
        return false;
    }
    // Unknown actor and unknown client IP: likewise not useful.
    if actor.id() == UNKNOWN && client_ip(client) == UNKNOWN && !internal_traffic {
        return false;
    }
    true
}

impl AuditRecord {
    /// Emits the audit event at the level selected by `SRC_LOG_LEVEL`. The
    /// whole field tree is attached as one JSON string field named `audit`.
    pub fn emit(&self, log_level: &str) {
        let payload = marshal_go(&self.fields);
        match logger_severity(log_level) {
            AuditSeverity::Debug => tracing::debug!(audit = %payload, "{}", self.message),
            AuditSeverity::Info => tracing::info!(audit = %payload, "{}", self.message),
            AuditSeverity::Warn => tracing::warn!(audit = %payload, "{}", self.message),
            AuditSeverity::Error => tracing::error!(audit = %payload, "{}", self.message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    fn context() -> GraphqlAuditContext {
        GraphqlAuditContext {
            actor: AuditActor::new(Some(7), ""),
            client: Some(ClientInfo {
                ip: "192.0.2.10".to_owned(),
                forwarded_for: "203.0.113.1,203.0.113.2".to_owned(),
                user_agent: "Mozilla/5.0".to_owned(),
                forwarded_for_user_agent: "Mozilla/5.0".to_owned(),
            }),
            query: "query { repository { name } }".to_owned(),
            variables_json: "{\"param1\":\"value1\"}".to_owned(),
            request_name: "TestRequest".to_owned(),
            request_source: "code".to_owned(),
            successful: true,
        }
    }

    #[test]
    fn parses_audit_config_and_defaults_to_disabled() {
        assert!(audit_log_config(&site("{}")).is_none());
        assert!(audit_log_config(&site("{\"log\":{}}")).is_none());
        assert!(audit_log_config(&site("{\"log\":{\"auditLog\":null}}")).is_none());
        let config = audit_log_config(&site(
            "{\"log\":{\"auditLog\":{\"graphQL\":true,\"internalTraffic\":true}}}",
        ))
        .expect("config");
        assert_eq!(
            config,
            AuditLogConfig {
                gitserver_access: false,
                graphql: true,
                internal_traffic: true,
            }
        );
        assert!(!is_enabled(&site("{}"), AuditLogSetting::GraphQL));
        assert!(is_enabled(
            &site("{\"log\":{\"auditLog\":{\"graphQL\":true}}}"),
            AuditLogSetting::GraphQL
        ));
        assert!(!is_enabled(
            &site("{\"log\":{\"auditLog\":{\"graphQL\":true}}}"),
            AuditLogSetting::InternalTraffic
        ));
    }

    #[test]
    fn extracts_client_like_requestclient() {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", "Mozilla/5.0".parse().unwrap());
        headers.append("x-forwarded-for", "203.0.113.1".parse().unwrap());
        headers.append("x-forwarded-for", "203.0.113.2".parse().unwrap());
        let client = client_info(&headers, Some("192.0.2.10:1234")).expect("client");
        assert_eq!(client.ip, "192.0.2.10");
        assert_eq!(client.forwarded_for, "203.0.113.1,203.0.113.2");
        assert_eq!(client.user_agent, "Mozilla/5.0");
        assert_eq!(client.forwarded_for_user_agent, "Mozilla/5.0");

        headers.insert("x-forwarded-for-user-agent", "proxy-ua".parse().unwrap());
        let client = client_info(&headers, Some("[::1]:80")).expect("client");
        assert_eq!(client.ip, "[");
        assert_eq!(client.forwarded_for_user_agent, "proxy-ua");

        assert!(client_info(&headers, None).is_none());
    }

    #[test]
    fn actor_id_follows_baseline_precedence() {
        assert_eq!(AuditActor::new(Some(7), "anon").id(), "7");
        assert_eq!(AuditActor::new(Some(0), "anon").id(), "anon");
        assert_eq!(AuditActor::new(None, "anon").id(), "anon");
        assert_eq!(AuditActor::new(None, "").id(), UNKNOWN);
        assert_eq!(AuditActor::internal().id(), UNKNOWN);
    }

    #[test]
    fn logs_only_when_enabled_and_not_filtered() {
        let enabled = site("{\"log\":{\"auditLog\":{\"graphQL\":true}}}");
        let internal_enabled =
            site("{\"log\":{\"auditLog\":{\"graphQL\":true,\"internalTraffic\":true}}}");
        let client = context().client;
        let user = AuditActor::new(Some(7), "");

        assert!(!should_log(&site("{}"), &user, client.as_ref()));
        assert!(should_log(&enabled, &user, client.as_ref()));
        assert!(!should_log(
            &enabled,
            &AuditActor::internal(),
            client.as_ref()
        ));
        assert!(should_log(&internal_enabled, &AuditActor::internal(), None));
        // Unknown actor + nil client is filtered unless internal traffic is
        // enabled; a known client IP is enough to log.
        let unknown = AuditActor::new(None, "");
        assert!(!should_log(&enabled, &unknown, None));
        assert!(should_log(&enabled, &unknown, client.as_ref()));
        assert!(should_log(&internal_enabled, &unknown, None));
        // A known anonymous UID is not "unknown", even without a client.
        assert!(should_log(&enabled, &AuditActor::new(None, "anon"), None));
    }

    #[test]
    fn builds_fields_and_message() {
        let record = build_graphql_record(
            &site("{\"log\":{\"auditLog\":{\"graphQL\":true}}}"),
            &context(),
            "00000000-0000-4000-8000-000000000000",
        )
        .expect("record");
        assert_eq!(
            record.message,
            "request (sampling immunity token: 00000000-0000-4000-8000-000000000000)"
        );
        assert_eq!(
            record.fields,
            serde_json::json!({
                "audit": {
                    "auditId": "00000000-0000-4000-8000-000000000000",
                    "action": "request",
                    "entity": "GraphQL",
                    "actor": {
                        "actorUID": "7",
                        "ip": "192.0.2.10",
                        "userAgent": "Mozilla/5.0",
                        "forwardedForUserAgent": "Mozilla/5.0",
                        "X-Forwarded-For": "203.0.113.1,203.0.113.2",
                    },
                },
                "request": {
                    "name": "TestRequest",
                    "source": "code",
                    "variables": "{\"param1\":\"value1\"}",
                    "query": "query { repository { name } }",
                },
                "mutation": false,
                "successful": true,
            })
        );
    }

    #[test]
    fn nil_client_and_unknown_actor_use_unknown_fields() {
        let mut context = context();
        context.actor = AuditActor::new(None, "");
        context.client = None;
        let record = build_graphql_record(
            &site("{\"log\":{\"auditLog\":{\"graphQL\":true,\"internalTraffic\":true}}}"),
            &context,
            "id",
        )
        .expect("record");
        let actor = &record.fields["audit"]["actor"];
        assert_eq!(actor["actorUID"], UNKNOWN);
        assert_eq!(actor["ip"], UNKNOWN);
        assert_eq!(actor["userAgent"], UNKNOWN);
        assert_eq!(actor["forwardedForUserAgent"], UNKNOWN);
        assert_eq!(actor["X-Forwarded-For"], UNKNOWN);
    }

    #[test]
    fn variables_nil_empty_and_object() {
        assert_eq!(GraphqlAuditContext::variables_json(None), "null");
        assert_eq!(GraphqlAuditContext::variables_json(Some(&Map::new())), "{}");
        let variables: Map<String, Value> =
            serde_json::from_str("{\"s\":\"<a>&\",\"n\":1.0,\"nested\":{\"b\":2,\"a\":1}}")
                .expect("variables");
        assert_eq!(
            GraphqlAuditContext::variables_json(Some(&variables)),
            "{\"n\":1,\"nested\":{\"a\":1,\"b\":2},\"s\":\"\\u003ca\\u003e\\u0026\"}"
        );
    }

    #[test]
    fn mutation_and_success_flags() {
        let mut context = context();
        context.query = "mutation { updateUser { id } }".to_owned();
        context.successful = false;
        let record = build_graphql_record(
            &site("{\"log\":{\"auditLog\":{\"graphQL\":true}}}"),
            &context,
            "id",
        )
        .expect("record");
        assert_eq!(record.fields["mutation"], true);
        assert_eq!(record.fields["successful"], false);
    }

    #[test]
    fn generates_uuid_v4_audit_ids() {
        let record = graphql_record(
            &site("{\"log\":{\"auditLog\":{\"graphQL\":true}}}"),
            &context(),
        )
        .expect("record");
        assert_eq!(record.audit_id.len(), 36);
        let parsed = Uuid::parse_str(&record.audit_id).expect("uuid");
        assert_eq!(parsed.get_version_num(), 4);
        assert!(record
            .message
            .ends_with(&format!("(sampling immunity token: {})", record.audit_id)));
        assert_eq!(record.fields["audit"]["auditId"], record.audit_id);
    }

    #[test]
    fn severity_is_exact_switch_on_lowercased_level() {
        assert_eq!(logger_severity("DEBUG"), AuditSeverity::Debug);
        assert_eq!(logger_severity("info"), AuditSeverity::Info);
        assert_eq!(logger_severity("warn"), AuditSeverity::Warn);
        assert_eq!(logger_severity("error"), AuditSeverity::Error);
        for level in ["dbug", "eror", "crit", "none", "", "INFO ", "trace"] {
            assert_eq!(logger_severity(level), AuditSeverity::Warn, "{level}");
        }
        assert_eq!(AuditSeverity::Info.as_str(), "info");
    }
}
