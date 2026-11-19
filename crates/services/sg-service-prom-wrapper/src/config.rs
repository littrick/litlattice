#![forbid(unsafe_code)]

//! Site configuration fields consumed by prom-wrapper (`siteconfig.go` +
//! `schema/schema.go` types).

use serde::{Deserialize, Serialize};

/// A problem surfaced in `/prom-wrapper/config-subscriber` and (in Go) in the
/// site configuration problems list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Problem {
    pub message: String,
}

impl Problem {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// `schema.SMTPServerConfig`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SmtpConfig {
    pub host: String,
    pub port: i32,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub authentication: String,
    #[serde(default)]
    pub domain: String,
    #[serde(default)]
    pub no_verify_tls: bool,
    #[serde(default)]
    pub additional_headers: Vec<Header>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Header {
    pub key: String,
    pub value: String,
}

/// The email fields `siteconfig.go` subscribes to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SiteEmailConfig {
    #[serde(default)]
    pub smtp: Option<SmtpConfig>,
    #[serde(default)]
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Responder {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "type")]
    pub responder_type: String,
    #[serde(default)]
    pub username: String,
}

/// `schema.Notifier` (tagged union on `type`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Notifier {
    #[serde(rename_all = "camelCase")]
    Email { address: String },
    #[serde(rename_all = "camelCase")]
    Opsgenie {
        #[serde(default)]
        api_key: String,
        #[serde(default)]
        api_url: String,
        #[serde(default)]
        priority: String,
        #[serde(default)]
        responders: Vec<Responder>,
        #[serde(default)]
        tags: String,
    },
    #[serde(rename_all = "camelCase")]
    Pagerduty {
        #[serde(default)]
        api_url: String,
        integration_key: String,
        #[serde(default)]
        severity: String,
    },
    Slack {
        #[serde(default, rename = "icon_emoji")]
        icon_emoji: String,
        #[serde(default, rename = "icon_url")]
        icon_url: String,
        #[serde(default)]
        recipient: String,
        #[serde(default)]
        url: String,
        #[serde(default)]
        username: String,
    },
    #[serde(rename_all = "camelCase")]
    Webhook {
        #[serde(default)]
        bearer_token: String,
        #[serde(default)]
        password: String,
        url: String,
        #[serde(default)]
        username: String,
    },
}

/// `schema.ObservabilityAlerts`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    pub level: String,
    #[serde(default)]
    pub owners: Vec<String>,
    #[serde(default)]
    pub disable_send_resolved: bool,
    pub notifier: Notifier,
}

/// The subset of `SiteConfiguration` that `subscribedSiteConfig` tracks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SubscribedSiteConfig {
    #[serde(default)]
    pub alerts: Vec<Alert>,
    #[serde(default)]
    pub email: Option<SiteEmailConfig>,
    #[serde(default)]
    pub silenced_alerts: Vec<String>,
    #[serde(default)]
    pub external_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiffType {
    Alerts,
    Email,
    Silences,
}

impl DiffType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Alerts => "alerts",
            Self::Email => "email",
            Self::Silences => "silences",
        }
    }
}

impl SubscribedSiteConfig {
    /// Parse the relevant fields out of the raw site configuration JSON
    /// (`newSubscribedSiteConfig`). Missing/invalid fields become empty.
    pub fn from_site_config(raw: &serde_json::Value) -> Self {
        let alerts = raw
            .get("observability")
            .and_then(|value| value.get("alerts"))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        let email = raw
            .get("email")
            .and_then(|value| serde_json::from_value::<SiteEmailConfig>(value.clone()).ok());
        let silenced_alerts = raw
            .get("observability")
            .and_then(|value| value.get("silenceAlerts"))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        let external_url = raw
            .get("externalURL")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_owned();
        Self {
            alerts,
            email,
            silenced_alerts,
            external_url,
        }
    }

    /// `subscribedSiteConfig.Diff`: which groups changed relative to `other`.
    pub fn diff(&self, other: &Self) -> Vec<DiffType> {
        let mut diffs = Vec::new();
        if self.alerts != other.alerts {
            diffs.push(DiffType::Alerts);
        }
        if self.email != other.email {
            diffs.push(DiffType::Email);
        }
        if self.silenced_alerts != other.silenced_alerts {
            diffs.push(DiffType::Silences);
        }
        diffs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_site_config_fields() {
        let raw = serde_json::json!({
            "externalURL": "https://example.com",
            "email": {
                "address": "alerts@example.com",
                "smtp": { "host": "smtp", "port": 587, "authentication": "PLAIN" }
            },
            "observability": {
                "alerts": [{
                    "level": "critical",
                    "owners": ["team-a"],
                    "notifier": {"type": "slack", "url": "https://hooks.example/x", "recipient": "#alerts"}
                }],
                "silenceAlerts": ["level=\"warning\""]
            }
        });
        let config = SubscribedSiteConfig::from_site_config(&raw);
        assert_eq!(config.external_url, "https://example.com");
        assert_eq!(config.alerts.len(), 1);
        assert_eq!(config.alerts[0].level, "critical");
        assert!(matches!(config.alerts[0].notifier, Notifier::Slack { .. }));
        assert_eq!(config.silenced_alerts, vec!["level=\"warning\"".to_owned()]);
        assert_eq!(config.email.as_ref().unwrap().address, "alerts@example.com");
    }

    #[test]
    fn diffs_by_group() {
        let base = SubscribedSiteConfig::default();
        let mut changed = base.clone();
        changed.external_url = "https://example.com".to_owned();
        assert!(base.diff(&changed).is_empty());

        changed.silenced_alerts.push("level=\"warning\"".to_owned());
        assert_eq!(base.diff(&changed), vec![DiffType::Silences]);
    }
}
