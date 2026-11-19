#![forbid(unsafe_code)]

//! Alertmanager configuration model (`github.com/prometheus/alertmanager/config`).
//!
//! This models the subset of the Alertmanager configuration that the wrapper
//! reads and writes. Field names and `omitempty`/always-emitted behavior mirror
//! the YAML tags on the corresponding Go `config` package types.
//!
//! Unknown fields are preserved:
//! - [`AmConfig::extra`] keeps unknown top-level sections (for example
//!   `inhibit_rules`, `time_intervals`).
//! - [`Route::extra`] keeps route fields we do not model.
//! - Every notifier config keeps unknown fields through
//!   [`NotifierConfig::extra`].

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Unknown YAML fields preserved during a load/render round-trip.
pub type Extra = BTreeMap<String, serde_yaml::Value>;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AmConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global: Option<GlobalConfig>,
    #[serde(default)]
    pub route: Route,
    #[serde(default)]
    pub receivers: Vec<Receiver>,
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default)]
    pub templates: Vec<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `config.GlobalConfig` subset (SMTP plus preserved extras).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolve_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_hello: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_smarthost: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_auth_username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_auth_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_auth_password_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_auth_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_auth_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_require_tls: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `config.Route`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Route {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub receiver: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub r#match: BTreeMap<String, String>,
    #[serde(
        default,
        rename = "match_re",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub match_re: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matchers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mute_time_intervals: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_time_intervals: Vec<String>,
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default, rename = "continue")]
    pub continue_: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<Route>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_wait: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_interval: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat_interval: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `config.Receiver`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Receiver {
    #[serde(default)]
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub email_configs: Vec<EmailConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pagerduty_configs: Vec<PagerDutyConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slack_configs: Vec<SlackConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub webhook_configs: Vec<WebhookConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub opsgenie_configs: Vec<OpsGenieConfig>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `config.NotifierConfig`, inlined into every notifier config.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NotifierConfig {
    /// Go's YAML tag has no `omitempty`, so this is always emitted.
    #[serde(default)]
    pub send_resolved: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `config.EmailConfig`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EmailConfig {
    #[serde(flatten)]
    pub notifier: NotifierConfig,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hello: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub smarthost: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_username: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_password: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_password_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_secret: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_identity: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub html: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_tls: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_config: Option<serde_yaml::Value>,
}

/// `config.PagerdutyConfig`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PagerDutyConfig {
    #[serde(flatten)]
    pub notifier: NotifierConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_config: Option<HttpClientConfig>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service_key_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub routing_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub routing_key_file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub details: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<PagerDutyImage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<PagerDutyLink>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub severity: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub class: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub component: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PagerDutyImage {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub src: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub alt: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub href: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PagerDutyLink {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub href: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
}

/// `config.SlackConfig`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SlackConfig {
    #[serde(flatten)]
    pub notifier: NotifierConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_config: Option<HttpClientConfig>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub api_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub api_url_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub channel: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub color: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title_link: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pretext: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<serde_yaml::Value>,
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default)]
    pub short_fields: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub footer: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fallback: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub callback_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub icon_emoji: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub icon_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thumb_url: String,
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default)]
    pub link_names: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mrkdwn_in: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<SlackAction>,
}

/// `config.SlackAction`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SlackAction {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub r#type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub style: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm: Option<serde_yaml::Value>,
}

/// `config.OpsGenieConfig`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpsGenieConfig {
    #[serde(flatten)]
    pub notifier: NotifierConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_config: Option<HttpClientConfig>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub api_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub api_key_file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub details: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub entity: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responders: Vec<OpsGenieConfigResponder>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actions: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tags: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub priority: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub update_alerts: bool,
}

/// `config.OpsGenieConfigResponder`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpsGenieConfigResponder {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
    #[serde(default, rename = "type", skip_serializing_if = "String::is_empty")]
    pub responder_type: String,
}

/// `commoncfg.Secret.MarshalYAML`: mask non-empty secrets.
fn serialize_secret<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if value.is_empty() {
        serializer.serialize_none()
    } else {
        serializer.serialize_str("<secret>")
    }
}

/// `config.WebhookConfig`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WebhookConfig {
    #[serde(flatten)]
    pub notifier: NotifierConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_config: Option<HttpClientConfig>,
    /// The Go fields have no `omitempty`, so they are always emitted.
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub url_file: String,
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default)]
    pub max_alerts: u64,
}

/// `commoncfg.HTTPClientConfig` subset.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HttpClientConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basic_auth: Option<BasicAuth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<Authorization>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth2: Option<serde_yaml::Value>,
    /// `commoncfg.Secret`: Go marshals a non-empty value as `<secret>` and
    /// omits an empty one, so the on-disk file never carries the real token.
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        serialize_with = "serialize_secret"
    )]
    pub bearer_token: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bearer_token_file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_config: Option<serde_yaml::Value>,
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default)]
    pub follow_redirects: bool,
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default)]
    pub enable_http2: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub proxy_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub no_proxy: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub proxy_from_environment: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_connect_header: Option<serde_yaml::Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `commoncfg.BasicAuth`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BasicAuth {
    /// The Go field has no `omitempty`, so it is always emitted.
    #[serde(default)]
    pub username: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username_file: String,
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        serialize_with = "serialize_secret"
    )]
    pub password: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password_file: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `commoncfg.Authorization`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Authorization {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub r#type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub credentials: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub credentials_file: String,
    #[serde(flatten)]
    pub extra: Extra,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// `amconfig.LoadFile`: read and parse an Alertmanager configuration file.
///
/// A missing file or invalid YAML yields an error. A file containing only
/// `global: {}` parses successfully (all fields are defaulted).
pub fn load_file(path: &Path) -> Result<AmConfig, String> {
    let data = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    serde_yaml::from_str(&data).map_err(|err| format!("failed to parse {}: {err}", path.display()))
}

/// `yaml.Marshal` + the structural checks `amconfig.Load` performs for the
/// subset of the configuration that the wrapper produces.
///
/// Unknown fields are preserved by the model and re-serialized, never dropped.
pub fn render_yaml(config: &AmConfig) -> Result<String, String> {
    let rendered =
        serde_yaml::to_string(config).map_err(|err| format!("failed to marshal: {err}"))?;

    // Round-trip the rendered output so that anything we write is guaranteed to
    // be readable by this model, then validate the parsed result (this mirrors
    // Go's `amconfig.Load(string(data))` after `yaml.Marshal`).
    let parsed: AmConfig =
        serde_yaml::from_str(&rendered).map_err(|err| format!("failed to load: {err}"))?;
    validate(&parsed)?;
    Ok(rendered)
}

fn validate(config: &AmConfig) -> Result<(), String> {
    let mut names = std::collections::HashSet::new();
    for receiver in &config.receivers {
        if receiver.name.is_empty() {
            return Err("missing name in receiver".to_owned());
        }
        if !names.insert(receiver.name.as_str()) {
            return Err(format!(
                "notification config name {:?} is not unique",
                receiver.name
            ));
        }
        for email in &receiver.email_configs {
            if email.to.is_empty() {
                return Err("missing to address in email config".to_owned());
            }
        }
        for pagerduty in &receiver.pagerduty_configs {
            if pagerduty.routing_key.is_empty() && pagerduty.service_key.is_empty() {
                return Err("missing service or routing key in PagerDuty config".to_owned());
            }
        }
        for webhook in &receiver.webhook_configs {
            if webhook.url.is_empty() && webhook.url_file.is_empty() {
                return Err("one of url or url_file must be configured".to_owned());
            }
        }
    }

    // The root route is the fallback node for all alerts.
    if config.route.receiver.is_empty() {
        return Err("root route must specify a default receiver".to_owned());
    }
    if config.route.continue_ {
        return Err("cannot have continue in root route".to_owned());
    }
    if !config.route.r#match.is_empty()
        || !config.route.match_re.is_empty()
        || !config.route.matchers.is_empty()
    {
        return Err("root route must not have any matchers".to_owned());
    }
    if !config.route.mute_time_intervals.is_empty() {
        return Err("root route must not have any mute time intervals".to_owned());
    }
    if !config.route.active_time_intervals.is_empty() {
        return Err("root route must not have any active time intervals".to_owned());
    }

    check_route_receivers(&config.route, &names)?;
    Ok(())
}

fn check_route_receivers(
    route: &Route,
    names: &std::collections::HashSet<&str>,
) -> Result<(), String> {
    for child in &route.routes {
        check_route_receivers(child, names)?;
    }
    // An empty receiver inherits from the parent route, which Alertmanager
    // allows for non-root routes.
    if !route.receiver.is_empty() && !names.contains(route.receiver.as_str()) {
        return Err(format!(
            "undefined receiver {:?} used in route",
            route.receiver
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parses_global_only() {
        let config: AmConfig = serde_yaml::from_str("global: {}\n").expect("parse global only");
        assert!(config.global.is_some());
        assert_eq!(config.route, Route::default());
        assert!(config.receivers.is_empty());

        // A defaulted root route has no receiver, matching Go's
        // "root route must specify a default receiver" rejection.
        let rendered = render_yaml(&config);
        assert!(rendered.is_err());
    }

    #[test]
    fn load_file_missing_and_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.yml");
        assert!(load_file(&missing).is_err());

        let invalid = dir.path().join("invalid.yml");
        let mut file = std::fs::File::create(&invalid).unwrap();
        file.write_all(b"route: [not a mapping\n").unwrap();
        assert!(load_file(&invalid).is_err());
    }

    #[test]
    fn round_trips_all_receiver_types() {
        let yaml = r##"
global:
  smtp_smarthost: smtp.example.com:587
  smtp_from: alerts@example.com
  smtp_auth_username: alerts
  smtp_auth_password: hunter2
  smtp_require_tls: true
route:
  receiver: email
  group_by: [alertname]
  continue: false
  routes:
    - receiver: slack
      match:
        level: warning
      match_re:
        owner: ^(team-a)$
      continue: true
receivers:
  - name: email
    email_configs:
      - send_resolved: false
        to: oncall@example.com
        headers:
          subject: "[{{ .CommonLabels.level }}]"
        html: "<body>firing</body>"
        text: firing
  - name: slack
    slack_configs:
      - send_resolved: true
        api_url: https://hooks.slack.com/services/x
        channel: "#alerts"
        username: Sourcegraph Alerts
        title: title
        title_link: https://sourcegraph.com/docs/admin/observability/alerts
        text: text
        color: "#FF0000"
        actions:
          - type: button
            text: Dashboard
            url: https://example.com
  - name: pagerduty
    pagerduty_configs:
      - send_resolved: true
        routing_key: integration-key
        severity: critical
        description: description
        links:
          - href: https://example.com
            text: Next steps
  - name: opsgenie
    opsgenie_configs:
      - send_resolved: true
        api_key: key
        api_url: https://api.opsgenie.com/
        message: message
        description: description
        priority: P1
        tags: "a=b,"
        responders:
          - type: team
            name: team-a
  - name: webhook
    webhook_configs:
      - send_resolved: true
        url: https://example.com/hook
        http_config:
          basic_auth:
            username: user
            password: pass
          bearer_token: token
          follow_redirects: false
          enable_http2: false
templates:
  - /etc/alertmanager/templates/*.tmpl
inhibit_rules:
  - source_matchers: ["severity=critical"]
    target_matchers: ["severity=warning"]
    equal: [alertname]
"##;
        let config: AmConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert_eq!(config.receivers.len(), 5);

        let rendered = render_yaml(&config).expect("render config");
        let reparsed: AmConfig = serde_yaml::from_str(&rendered).expect("reparse rendered");

        // The render is structurally equivalent, and unknown top-level fields
        // are preserved rather than dropped.
        assert_eq!(reparsed.receivers.len(), 5);
        assert_eq!(reparsed.global, config.global);
        assert!(rendered.contains("inhibit_rules"));
        assert!(rendered.contains("templates:"));
        assert_eq!(reparsed.extra.len(), 1);

        // Notifier details survive the round-trip.
        let slack = &reparsed.receivers[1].slack_configs[0];
        assert_eq!(slack.api_url, "https://hooks.slack.com/services/x");
        assert_eq!(slack.actions.len(), 1);
        let webhook = &reparsed.receivers[4].webhook_configs[0];
        let http = webhook.http_config.as_ref().expect("http config");
        assert_eq!(http.basic_auth.as_ref().unwrap().username, "user");
        // `commoncfg.Secret` marshals as the literal `<secret>` in the file
        // (Go does the same), so re-parsing sees the placeholder.
        assert_eq!(http.basic_auth.as_ref().unwrap().password, "<secret>");
        assert_eq!(http.bearer_token, "<secret>");
        let route = &reparsed.route.routes[0];
        assert!(route.continue_);
        assert_eq!(route.match_re.get("owner").unwrap(), "^(team-a)$");
    }

    #[test]
    fn rejects_invalid_route() {
        let mut config = AmConfig {
            route: Route {
                receiver: "missing".to_owned(),
                ..Default::default()
            },
            receivers: vec![Receiver {
                name: "email".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = render_yaml(&config).unwrap_err();
        assert!(err.contains("undefined receiver"), "{err}");

        // Duplicate receiver names are rejected.
        config.route.receiver = "email".to_owned();
        config.receivers.push(Receiver {
            name: "email".to_owned(),
            ..Default::default()
        });
        let err = render_yaml(&config).unwrap_err();
        assert!(err.contains("is not unique"), "{err}");
    }
}
