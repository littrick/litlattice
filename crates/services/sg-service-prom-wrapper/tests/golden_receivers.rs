//! Golden comparison against the Go baseline's receiver/route output.
//!
//! `go_receivers_golden.yaml` was produced by a throwaway Go test calling the
//! baseline `newRoutesAndReceivers` + `newRootRoute` with the same fixture.

use sg_service_prom_wrapper::amconfig::AmConfig;
use sg_service_prom_wrapper::config::{Alert, Notifier, Problem, Responder};
use sg_service_prom_wrapper::receivers::apply_receivers;

const GOLDEN: &str = include_str!("fixtures/go_receivers_golden.yaml");

fn fixture_alerts() -> Vec<Alert> {
    vec![
        Alert {
            level: "critical".to_owned(),
            owners: vec![],
            disable_send_resolved: false,
            notifier: Notifier::Slack {
                icon_emoji: String::new(),
                icon_url: String::new(),
                recipient: "#alerts".to_owned(),
                url: "https://hooks.slack.com/services/T/B/X".to_owned(),
                username: String::new(),
            },
        },
        Alert {
            level: "warning".to_owned(),
            owners: vec!["team-a".to_owned(), "team-b".to_owned()],
            disable_send_resolved: false,
            notifier: Notifier::Email {
                address: "oncall@example.com".to_owned(),
            },
        },
        Alert {
            level: "warning".to_owned(),
            owners: vec![],
            disable_send_resolved: true,
            notifier: Notifier::Pagerduty {
                api_url: String::new(),
                integration_key: "pd-key".to_owned(),
                severity: "warning".to_owned(),
            },
        },
        Alert {
            level: "critical".to_owned(),
            owners: vec![],
            disable_send_resolved: false,
            notifier: Notifier::Webhook {
                bearer_token: "tok".to_owned(),
                password: "p".to_owned(),
                url: "https://example.com/hook".to_owned(),
                username: "u".to_owned(),
            },
        },
        Alert {
            level: "critical".to_owned(),
            owners: vec![],
            disable_send_resolved: false,
            notifier: Notifier::Opsgenie {
                api_key: "og-key".to_owned(),
                api_url: String::new(),
                priority: String::new(),
                responders: vec![Responder {
                    id: String::new(),
                    name: "sre".to_owned(),
                    responder_type: "team".to_owned(),
                    username: String::new(),
                }],
                tags: String::new(),
            },
        },
    ]
}

#[test]
fn receivers_and_route_match_go_golden() {
    let mut config = AmConfig::default();
    let mut problems: Vec<Problem> = Vec::new();
    apply_receivers(
        &mut config,
        &fixture_alerts(),
        "https://sourcegraph.example.com",
        &mut problems,
    );
    assert!(problems.is_empty(), "unexpected problems: {problems:?}");

    let actual = serde_yaml::to_value(&config).expect("serialize rust config");
    let expected: serde_yaml::Value = serde_yaml::from_str(GOLDEN).expect("parse go golden");

    assert_eq!(
        actual.get("route"),
        expected.get("route"),
        "route differs from Go golden"
    );
    assert_eq!(
        actual.get("receivers"),
        expected.get("receivers"),
        "receivers differ from Go golden"
    );
}
