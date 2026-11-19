#![forbid(unsafe_code)]

//! `observability.alerts` -> Alertmanager receivers/routes (`receivers.go`).
//!
//! Ported from `newRoutesAndReceivers` and `newRootRoute` in the Go wrapper.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::amconfig::{
    AmConfig, BasicAuth, EmailConfig, HttpClientConfig, NotifierConfig, OpsGenieConfig,
    OpsGenieConfigResponder, PagerDutyConfig, PagerDutyLink, Receiver, Route, SlackAction,
    SlackConfig, WebhookConfig,
};
use crate::config::{Alert, Notifier, Problem};

// Values from the Go constants (`alertmanagerWarningReceiver`, etc.).
pub const WARNING_RECEIVER: &str = "src-warning-receiver";
pub const CRITICAL_RECEIVER: &str = "src-critical-receiver";
pub const NOOP_RECEIVER: &str = "src-noop-receiver";

const COLOR_WARNING: &str = "#FFFF00"; // yellow
const COLOR_CRITICAL: &str = "#FF0000"; // red
const COLOR_GOOD: &str = "#00FF00"; // green

/// The packaged prom-wrapper is built without version ldflags, so the Go
/// `alertsReferenceURL` resolves to the unversioned documentation URL.
const ALERTS_REFERENCE_URL: &str = "https://sourcegraph.com/docs/admin/observability/alerts";

/// Labels we group alerts by; also used by the static templates below.
const COMMON_LABELS: [&str; 6] = [
    "alertname",
    "level",
    "service_name",
    "name",
    "owner",
    "description",
];

// Static alertmanager templates. Templating reference:
// https://prometheus.io/docs/alerting/latest/notifications
const OBSERVABLE_DOC_ANCHOR_TEMPLATE: &str =
    r#"{{ .CommonLabels.service_name }}-{{ .CommonLabels.name | reReplaceAll "_" "-" }}"#;

const FIRING_TITLE_TEMPLATE: &str =
    r#"[{{ .CommonLabels.level | toUpper }}] {{ .CommonLabels.description }}"#;
const RESOLVED_TITLE_TEMPLATE: &str = r#"[RESOLVED] {{ .CommonLabels.description }}"#;

const TAGS_TEMPLATE_DEFAULT: &str =
    r#"{{ range $key, $value := .CommonLabels }}{{$key}}={{$value}},{{end}}"#;

const FIRING_BODY_TEMPLATE: &str = r#"{{ .CommonLabels.level | title }} alert '{{ .CommonLabels.name }}' is firing for service '{{ .CommonLabels.service_name }}' ({{ .CommonLabels.owner }})."#;
const RESOLVED_BODY_TEMPLATE: &str = r#"{{ .CommonLabels.level | title }} alert '{{ .CommonLabels.name }}' for service '{{ .CommonLabels.service_name }}' has resolved."#;

/// `alertsReferenceURL` from the Go wrapper: link to the alerts reference page.
fn alerts_reference_url() -> &'static str {
    ALERTS_REFERENCE_URL
}

fn opsgenie_api_key() -> String {
    static KEY: OnceLock<String> = OnceLock::new();
    KEY.get_or_init(|| std::env::var("OPSGENIE_API_KEY").unwrap_or_default())
        .clone()
}

fn status_template(firing: &str, resolved: &str) -> String {
    format!("{{{{ if eq .Status \"firing\" }}}}{firing}{{{{ else }}}}{resolved}{{{{ end }}}}")
}

fn dashboard_url_template(external_url: &str) -> String {
    let mut template = String::new();
    template.push_str(external_url.strip_suffix('/').unwrap_or(external_url));
    template.push_str("/-/debug/grafana/d/");
    template.push_str("{{ .CommonLabels.service_name }}/{{ .CommonLabels.service_name }}");
    template.push_str("?viewPanel={{ .CommonLabels.grafana_panel_id }}");
    template.push_str(
        "{{ $start := (index .Alerts 0).StartsAt.Unix }}{{ $end := (index .Alerts 0).EndsAt.Unix }}",
    );
    template.push_str("{{ if gt $end 0 }}&from={{ $start }}000&end={{ $end }}000");
    template.push_str("{{ else }}&time={{ $start }}000&time.window=3600000{{ end }}");
    template
}

/// `changeReceivers`: replace receivers and the root route, leaving global SMTP
/// configuration untouched.
pub fn apply_receivers(
    config: &mut AmConfig,
    alerts: &[Alert],
    external_url: &str,
    problems: &mut Vec<Problem>,
) {
    let (receivers, routes) = new_routes_and_receivers(alerts, external_url, problems);
    config.receivers = receivers;
    config.route = new_root_route(routes);
}

/// `newRootRoute`: the base route Alertmanager requires to wrap all routes.
pub fn new_root_route(routes: Vec<Route>) -> Route {
    Route {
        group_by: COMMON_LABELS
            .iter()
            .map(|label| (*label).to_owned())
            .collect(),
        // Each group matches exactly one alert, so fire immediately.
        group_wait: Some("1s".to_owned()),
        group_interval: Some("1m".to_owned()),
        repeat_interval: Some("1w".to_owned()),
        routes,
        receiver: NOOP_RECEIVER.to_owned(),
        ..Default::default()
    }
}

fn new_routes_and_receivers(
    new_alerts: &[Alert],
    external_url: &str,
    problems: &mut Vec<Problem>,
) -> (Vec<Receiver>, Vec<Route>) {
    let mut warning_receiver = Receiver {
        name: WARNING_RECEIVER.to_owned(),
        ..Default::default()
    };
    let mut critical_receiver = Receiver {
        name: CRITICAL_RECEIVER.to_owned(),
        ..Default::default()
    };
    // Stub receiver for routes that do not have a configured receiver.
    let mut additional_receivers: BTreeMap<String, Receiver> = BTreeMap::new();
    additional_receivers.insert(
        NOOP_RECEIVER.to_owned(),
        Receiver {
            name: NOOP_RECEIVER.to_owned(),
            ..Default::default()
        },
    );

    // Default routes fall through to the per-level receivers.
    let default_routes = vec![
        Route {
            receiver: WARNING_RECEIVER.to_owned(),
            r#match: BTreeMap::from([("level".to_owned(), "warning".to_owned())]),
            ..Default::default()
        },
        Route {
            receiver: CRITICAL_RECEIVER.to_owned(),
            r#match: BTreeMap::from([("level".to_owned(), "critical".to_owned())]),
            ..Default::default()
        },
    ];
    let mut additional_routes: Vec<Route> = Vec::new();

    // Parameterized Alertmanager templates.
    let alerts_reference_url_template = format!(
        "{}#{}",
        alerts_reference_url(),
        OBSERVABLE_DOC_ANCHOR_TEMPLATE
    );
    let dashboard_url_template = dashboard_url_template(external_url);
    let notification_title_template =
        status_template(FIRING_TITLE_TEMPLATE, RESOLVED_TITLE_TEMPLATE);
    let firing_body_template_with_links = format!(
        "{FIRING_BODY_TEMPLATE}\n\nFor next steps, please refer to our documentation: {alerts_reference_url_template}\nFor more details, please refer to the service dashboard: {dashboard_url_template}"
    );
    let notification_body_template_without_links =
        status_template(FIRING_BODY_TEMPLATE, RESOLVED_BODY_TEMPLATE);
    let notification_body_template_with_links =
        status_template(&firing_body_template_with_links, RESOLVED_BODY_TEMPLATE);

    for (i, alert) in new_alerts.iter().enumerate() {
        // Go routes critical alerts to the critical receiver and everything
        // else to the warning receiver.
        let is_critical = alert.level == "critical";
        let active_color = if is_critical {
            COLOR_CRITICAL
        } else {
            COLOR_WARNING
        };
        let color_template = status_template(active_color, COLOR_GOOD);

        enum ReceiverKey {
            Warning,
            Critical,
            Named(String),
        }

        // Generate receiver and route for alerts with `owners`.
        let key = if !alert.owners.is_empty() {
            let owners = alert.owners.join("|");
            let owner_regexp = format!("^({owners})$");
            if !is_valid_regex(&owner_regexp) {
                problems.push(site_problem(format!(
                    "failed to apply alert {i}: invalid regular expression `{owner_regexp}`"
                )));
                continue;
            }

            let receiver_name = format!("src-{}-on-{}", alert.level, owners);
            if !additional_receivers.contains_key(&receiver_name) {
                additional_receivers.insert(
                    receiver_name.clone(),
                    Receiver {
                        name: receiver_name.clone(),
                        ..Default::default()
                    },
                );
                additional_routes.push(Route {
                    receiver: receiver_name.clone(),
                    r#match: BTreeMap::from([("level".to_owned(), alert.level.clone())]),
                    match_re: BTreeMap::from([("owner".to_owned(), owner_regexp)]),
                    // Generated routes are siblings: `continue: true` lets the
                    // alert fall through to other routes (and the per-level
                    // receivers).
                    continue_: true,
                    ..Default::default()
                });
            }
            ReceiverKey::Named(receiver_name)
        } else if is_critical {
            ReceiverKey::Critical
        } else {
            ReceiverKey::Warning
        };

        let receiver = match &key {
            ReceiverKey::Warning => &mut warning_receiver,
            ReceiverKey::Critical => &mut critical_receiver,
            ReceiverKey::Named(name) => additional_receivers
                .get_mut(name)
                .expect("owner receiver was inserted above"),
        };

        let notifier = NotifierConfig {
            send_resolved: !alert.disable_send_resolved,
            ..Default::default()
        };
        let notifier_kind = &alert.notifier;

        match notifier_kind {
            Notifier::Email { address } => {
                receiver.email_configs.push(EmailConfig {
                    notifier,
                    to: address.clone(),
                    headers: BTreeMap::from([(
                        "subject".to_owned(),
                        notification_title_template.clone(),
                    )]),
                    html: format!("<body>{notification_body_template_with_links}</body>"),
                    text: notification_body_template_with_links.clone(),
                    ..Default::default()
                });
            }

            Notifier::Opsgenie {
                api_key,
                api_url,
                priority,
                responders,
                tags,
            } => {
                let api_url = if api_url.is_empty() {
                    None
                } else {
                    match parse_url(i, api_url, problems) {
                        Some(url) => Some(url),
                        None => continue,
                    }
                };

                let api_key = if api_key.is_empty() {
                    opsgenie_api_key()
                } else {
                    api_key.clone()
                };

                let priority = if priority.is_empty() {
                    match alert.level.as_str() {
                        "critical" => "P1",
                        "warning" => "P2",
                        "info" => "P3",
                        _ => "P4",
                    }
                    .to_owned()
                } else {
                    priority.clone()
                };

                let tags = if tags.is_empty() {
                    TAGS_TEMPLATE_DEFAULT.to_owned()
                } else {
                    tags.clone()
                };

                let responders = responders
                    .iter()
                    .map(|responder| OpsGenieConfigResponder {
                        id: responder.id.clone(),
                        name: responder.name.clone(),
                        username: responder.username.clone(),
                        responder_type: responder.responder_type.clone(),
                    })
                    .collect();

                receiver.opsgenie_configs.push(OpsGenieConfig {
                    notifier,
                    api_key,
                    api_url,
                    message: notification_title_template.clone(),
                    description: notification_body_template_without_links.clone(),
                    priority,
                    tags,
                    responders,
                    source: dashboard_url_template.clone(),
                    details: BTreeMap::from([(
                        "Next steps".to_owned(),
                        alerts_reference_url_template.clone(),
                    )]),
                    ..Default::default()
                });
            }

            Notifier::Pagerduty {
                api_url,
                integration_key,
                severity,
            } => {
                let api_url = if api_url.is_empty() {
                    None
                } else {
                    match parse_url(i, api_url, problems) {
                        Some(url) => Some(url),
                        None => continue,
                    }
                };

                receiver.pagerduty_configs.push(PagerDutyConfig {
                    notifier,
                    routing_key: integration_key.clone(),
                    severity: severity.clone(),
                    url: api_url,
                    description: notification_title_template.clone(),
                    links: vec![
                        PagerDutyLink {
                            text: "Next steps".to_owned(),
                            href: alerts_reference_url_template.clone(),
                        },
                        PagerDutyLink {
                            text: "Dashboard".to_owned(),
                            href: dashboard_url_template.clone(),
                        },
                    ],
                    ..Default::default()
                });
            }

            Notifier::Slack {
                icon_emoji,
                icon_url,
                recipient,
                url,
                username,
            } => {
                let api_url = match parse_url(i, url, problems) {
                    Some(url) => url,
                    None => continue,
                };
                let username = if username.is_empty() {
                    "Sourcegraph Alerts".to_owned()
                } else {
                    username.clone()
                };

                receiver.slack_configs.push(SlackConfig {
                    notifier,
                    api_url,
                    username,
                    channel: recipient.clone(),
                    icon_emoji: icon_emoji.clone(),
                    icon_url: icon_url.clone(),
                    title: notification_title_template.clone(),
                    title_link: alerts_reference_url_template.clone(),
                    text: notification_body_template_without_links.clone(),
                    actions: vec![
                        SlackAction {
                            r#type: "button".to_owned(),
                            text: "Next steps".to_owned(),
                            url: alerts_reference_url_template.clone(),
                            ..Default::default()
                        },
                        SlackAction {
                            r#type: "button".to_owned(),
                            text: "Dashboard".to_owned(),
                            url: dashboard_url_template.clone(),
                            ..Default::default()
                        },
                    ],
                    color: color_template,
                    ..Default::default()
                });
            }

            Notifier::Webhook {
                bearer_token,
                password,
                url,
                username,
            } => {
                let url = match parse_url(i, url, problems) {
                    Some(url) => url,
                    None => continue,
                };

                receiver.webhook_configs.push(WebhookConfig {
                    notifier,
                    url,
                    http_config: Some(HttpClientConfig {
                        basic_auth: Some(BasicAuth {
                            username: username.clone(),
                            password: password.clone(),
                            ..Default::default()
                        }),
                        bearer_token: bearer_token.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            }
        }
    }

    // Sort for stability: BTreeMap yields receivers ordered by name.
    let mut receivers: Vec<Receiver> = additional_receivers.into_values().collect();
    receivers.push(warning_receiver);
    receivers.push(critical_receiver);

    let mut routes = additional_routes;
    routes.extend(default_routes);
    (receivers, routes)
}

fn site_problem(message: String) -> Problem {
    Problem::new(format!("`observability.alerts`: {message}"))
}

/// Parse a notifier URL, recording a `Problem` on failure (`url.Parse` in Go).
fn parse_url(alert_index: usize, raw: &str, problems: &mut Vec<Problem>) -> Option<String> {
    if raw.is_empty() {
        problems.push(site_problem(format!(
            "failed to apply notifier {alert_index}: empty URL"
        )));
        return None;
    }
    match url::Url::parse(raw) {
        Ok(url) => Some(url.to_string()),
        Err(err) => {
            problems.push(site_problem(format!(
                "failed to apply notifier {alert_index}: {err}"
            )));
            None
        }
    }
}

/// Lightweight regexp syntax check for the owner pattern.
///
/// We cannot pull in the `regex` crate from this slice (Cargo.toml is out of
/// scope), so this validates the constructs that can appear in the patterns the
/// wrapper generates (`^(owner|owner)$`) and catches clearly malformed input
/// such as unbalanced groups/brackets and dangling escapes.
fn is_valid_regex(pattern: &str) -> bool {
    let mut parens: i32 = 0;
    let mut brackets: i32 = 0;
    let mut braces: i32 = 0;
    let mut escaped = false;
    // Whether the previous token can be followed by a repetition operator.
    let mut repeatable = false;

    for c in pattern.chars() {
        if escaped {
            escaped = false;
            repeatable = true;
            continue;
        }
        match c {
            '\\' => {
                escaped = true;
                repeatable = false;
            }
            '(' => {
                parens += 1;
                repeatable = false;
            }
            ')' => {
                parens -= 1;
                if parens < 0 {
                    return false;
                }
                repeatable = true;
            }
            '[' => {
                brackets += 1;
                repeatable = false;
            }
            ']' => {
                brackets -= 1;
                if brackets < 0 {
                    return false;
                }
                repeatable = true;
            }
            '{' => {
                braces += 1;
                repeatable = true;
            }
            '}' => {
                braces -= 1;
                if braces < 0 {
                    return false;
                }
                repeatable = true;
            }
            '*' | '+' | '?' => {
                if !repeatable {
                    return false;
                }
                repeatable = true;
            }
            '|' | '^' | '$' => repeatable = false,
            _ => repeatable = true,
        }
    }

    !escaped && parens == 0 && brackets == 0 && braces == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amconfig::AmConfig;
    use crate::config::{Alert, Notifier, Problem, Responder};

    fn email() -> Notifier {
        Notifier::Email {
            address: "oncall@example.com".to_owned(),
        }
    }

    fn opsgenie() -> Notifier {
        Notifier::Opsgenie {
            api_key: String::new(),
            api_url: String::new(),
            priority: String::new(),
            responders: vec![Responder {
                id: "id".to_owned(),
                name: "team-a".to_owned(),
                responder_type: "team".to_owned(),
                username: String::new(),
            }],
            tags: String::new(),
        }
    }

    fn pagerduty() -> Notifier {
        Notifier::Pagerduty {
            api_url: String::new(),
            integration_key: "integration-key".to_owned(),
            severity: "critical".to_owned(),
        }
    }

    fn slack() -> Notifier {
        Notifier::Slack {
            icon_emoji: String::new(),
            icon_url: String::new(),
            recipient: "#alerts".to_owned(),
            url: "https://hooks.slack.com/services/x".to_owned(),
            username: String::new(),
        }
    }

    fn webhook() -> Notifier {
        Notifier::Webhook {
            bearer_token: "bearer".to_owned(),
            password: "password".to_owned(),
            url: "https://example.com/hook".to_owned(),
            username: "user".to_owned(),
        }
    }

    fn alert(level: &str, notifier: Notifier) -> Alert {
        Alert {
            level: level.to_owned(),
            owners: Vec::new(),
            disable_send_resolved: false,
            notifier,
        }
    }

    #[test]
    fn notifier_configs_match_go_defaults() {
        let alerts = vec![
            alert("critical", email()),
            alert("warning", opsgenie()),
            alert("critical", pagerduty()),
            alert("warning", slack()),
            alert("info", webhook()),
        ];
        let mut config = AmConfig::default();
        let mut problems = Vec::new();
        apply_receivers(
            &mut config,
            &alerts,
            "https://sourcegraph.example.com/",
            &mut problems,
        );
        assert!(problems.is_empty(), "{problems:?}");

        assert_eq!(config.receivers.len(), 3);
        let noop = &config.receivers[0];
        assert_eq!(noop.name, NOOP_RECEIVER);
        assert!(noop.email_configs.is_empty());

        let warning = &config.receivers[1];
        assert_eq!(warning.name, WARNING_RECEIVER);
        assert_eq!(warning.opsgenie_configs.len(), 1);
        assert_eq!(warning.slack_configs.len(), 1);
        assert_eq!(warning.webhook_configs.len(), 1);

        let opsgenie = &warning.opsgenie_configs[0];
        assert_eq!(opsgenie.priority, "P2");
        assert_eq!(opsgenie.tags, TAGS_TEMPLATE_DEFAULT);
        assert_eq!(
            opsgenie.api_key,
            std::env::var("OPSGENIE_API_KEY").unwrap_or_default()
        );
        assert_eq!(opsgenie.responders[0].responder_type, "team");
        assert!(opsgenie.notifier.send_resolved);

        let slack = &warning.slack_configs[0];
        assert_eq!(slack.username, "Sourcegraph Alerts");
        assert_eq!(slack.channel, "#alerts");
        assert_eq!(slack.actions.len(), 2);
        assert_eq!(slack.actions[0].text, "Next steps");
        assert_eq!(slack.actions[0].r#type, "button");
        assert!(slack.color.contains("firing"));

        let webhook = &warning.webhook_configs[0];
        let http = webhook.http_config.as_ref().expect("webhook http config");
        assert_eq!(http.basic_auth.as_ref().unwrap().username, "user");
        assert_eq!(http.basic_auth.as_ref().unwrap().password, "password");
        assert_eq!(http.bearer_token, "bearer");
        assert!(!http.follow_redirects);
        assert!(!http.enable_http2);

        let critical = &config.receivers[2];
        assert_eq!(critical.name, CRITICAL_RECEIVER);
        assert_eq!(critical.email_configs.len(), 1);
        assert_eq!(critical.pagerduty_configs.len(), 1);

        let email = &critical.email_configs[0];
        assert_eq!(email.to, "oncall@example.com");
        assert_eq!(
            email.headers.get("subject").unwrap(),
            &status_template(FIRING_TITLE_TEMPLATE, RESOLVED_TITLE_TEMPLATE)
        );
        assert!(email.text.contains("is firing for service"));

        let pagerduty = &critical.pagerduty_configs[0];
        assert_eq!(pagerduty.routing_key, "integration-key");
        assert_eq!(pagerduty.severity, "critical");
        assert_eq!(pagerduty.links.len(), 2);
        assert_eq!(pagerduty.links[1].text, "Dashboard");

        // Root route mirrors newRootRoute.
        assert_eq!(config.route.receiver, NOOP_RECEIVER);
        assert_eq!(config.route.group_by, COMMON_LABELS);
        assert_eq!(config.route.group_wait.as_deref(), Some("1s"));
        assert_eq!(config.route.group_interval.as_deref(), Some("1m"));
        assert_eq!(config.route.repeat_interval.as_deref(), Some("1w"));
        assert_eq!(config.route.routes.len(), 2);
        assert_eq!(config.route.routes[0].receiver, WARNING_RECEIVER);
        assert_eq!(
            config.route.routes[1].r#match.get("level").unwrap(),
            "critical"
        );
    }

    #[test]
    fn owners_add_route_and_receiver_with_continue() {
        let mut owners_alert = alert("critical", slack());
        owners_alert.owners = vec!["team-a".to_owned(), "team-b".to_owned()];
        owners_alert.disable_send_resolved = true;

        let mut config = AmConfig::default();
        let mut problems = Vec::new();
        apply_receivers(&mut config, &[owners_alert], "", &mut problems);
        assert!(problems.is_empty(), "{problems:?}");

        let receiver_name = "src-critical-on-team-a|team-b";
        let receiver = config
            .receivers
            .iter()
            .find(|receiver| receiver.name == receiver_name)
            .expect("owner receiver");
        assert_eq!(receiver.slack_configs.len(), 1);
        assert!(!receiver.slack_configs[0].notifier.send_resolved);

        let route = config
            .route
            .routes
            .iter()
            .find(|route| route.receiver == receiver_name)
            .expect("owner route");
        assert_eq!(route.r#match.get("level").unwrap(), "critical");
        assert_eq!(route.match_re.get("owner").unwrap(), "^(team-a|team-b)$");
        assert!(route.continue_);

        // The default per-level routes are still present.
        assert_eq!(config.route.routes.len(), 3);
    }

    #[test]
    fn invalid_url_yields_problem_and_skips_alert() {
        let mut bad_slack = alert(
            "warning",
            Notifier::Slack {
                icon_emoji: String::new(),
                icon_url: String::new(),
                recipient: "#alerts".to_owned(),
                url: "://bad".to_owned(),
                username: String::new(),
            },
        );
        bad_slack.owners = vec!["team-a".to_owned()];

        let mut config = AmConfig::default();
        let mut problems: Vec<Problem> = Vec::new();
        apply_receivers(&mut config, &[bad_slack], "", &mut problems);

        assert_eq!(problems.len(), 1);
        assert!(
            problems[0]
                .message
                .starts_with("`observability.alerts`: failed to apply notifier 0:"),
            "{}",
            problems[0].message
        );

        // The owner receiver/route was created before the notifier failure, so
        // it remains but has no channel configuration.
        let receiver = config
            .receivers
            .iter()
            .find(|receiver| receiver.name == "src-warning-on-team-a")
            .expect("owner receiver");
        assert!(receiver.slack_configs.is_empty());
    }

    #[test]
    fn invalid_owner_regex_yields_problem() {
        let mut bad = alert("warning", slack());
        bad.owners = vec!["(".to_owned()];

        let mut config = AmConfig::default();
        let mut problems = Vec::new();
        apply_receivers(&mut config, &[bad], "", &mut problems);

        assert_eq!(problems.len(), 1);
        assert!(
            problems[0]
                .message
                .starts_with("`observability.alerts`: failed to apply alert 0:"),
            "{}",
            problems[0].message
        );
    }
}
