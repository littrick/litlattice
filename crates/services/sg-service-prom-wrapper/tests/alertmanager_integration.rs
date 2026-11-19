//! Integration against a real Alertmanager binary.
//!
//! Run with `ALERTMANAGER_TEST_URL=http://127.0.0.1:19093 cargo test
//! -p sg-service-prom-wrapper --test alertmanager_integration`; without the
//! variable the test is skipped.

use std::time::Duration;

use sg_service_prom_wrapper::alertmanager::AlertmanagerClient;
use sg_service_prom_wrapper::amconfig::{load_file, GlobalConfig};
use sg_service_prom_wrapper::config::{Alert, Notifier, Problem, Responder};
use sg_service_prom_wrapper::receivers::apply_receivers;
use sg_service_prom_wrapper::silence::{matchers_from_silence, silence_from_matchers};

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
            owners: vec!["team-a".to_owned()],
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

#[tokio::test]
async fn applies_generated_config_and_manages_silences() {
    let Ok(base_url) = std::env::var("ALERTMANAGER_TEST_URL") else {
        eprintln!("skipping: ALERTMANAGER_TEST_URL is not set");
        return;
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("alertmanager.yml");
    std::fs::write(
        &config_path,
        "route:\n  receiver: src-noop-receiver\nreceivers:\n  - name: src-noop-receiver\n",
    )
    .expect("seed config");

    let client = AlertmanagerClient::new(base_url, &config_path);
    client
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("alertmanager ready");

    let mut config = load_file(&config_path).expect("load config");
    config.global = Some(GlobalConfig {
        smtp_from: Some("alerts@example.com".to_owned()),
        smtp_hello: Some("example.com".to_owned()),
        smtp_smarthost: Some("smtp.example.com:587".to_owned()),
        smtp_auth_username: Some("user".to_owned()),
        smtp_auth_password: Some("pass".to_owned()),
        smtp_require_tls: Some(true),
        ..Default::default()
    });
    let mut problems: Vec<Problem> = Vec::new();
    apply_receivers(
        &mut config,
        &fixture_alerts(),
        "https://sourcegraph.example.com",
        &mut problems,
    );
    assert!(problems.is_empty(), "problems: {problems:?}");

    client
        .apply(&config)
        .await
        .expect("alertmanager accepts config");

    // The rewritten file must still load through our model.
    let reloaded = load_file(&config_path).expect("reload config");
    assert!(reloaded.receivers.len() >= 3);

    let silence = "test_alert_name";
    client
        .post_silence(None, &matchers_from_silence(silence))
        .await
        .expect("create silence");
    let silences = client.get_silences().await.expect("list silences");
    let created = silences
        .iter()
        .find(|candidate| candidate.created_by == "src-prom-wrapper")
        .expect("created silence visible");
    assert_eq!(
        silence_from_matchers(
            &created
                .matchers
                .iter()
                .map(|matcher| sg_service_prom_wrapper::silence::Matcher {
                    name: matcher.name.clone(),
                    value: matcher.value.clone(),
                    is_regex: matcher.is_regex,
                    is_equal: matcher.is_equal,
                })
                .collect::<Vec<_>>()
        ),
        silence
    );

    client
        .delete_silence(&created.id)
        .await
        .expect("delete silence");
}
