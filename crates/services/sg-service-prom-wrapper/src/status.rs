#![forbid(unsafe_code)]

//! `/prom-wrapper/alerts-status` (`status.go`).

use std::collections::BTreeSet;
use std::sync::Arc;

use http::{Request, Response, StatusCode};
use hyper::body::Incoming;
use serde::Serialize;
use tonic::body::BoxBody;

use crate::alertmanager::{json_response, text_response, AlertmanagerClient};

/// Summary returned by `/prom-wrapper/alerts-status`
/// (`srcprometheus.AlertsStatus`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct AlertsSummary {
    pub warning: usize,
    pub silenced: usize,
    pub critical: usize,
    /// Go's JSON tag is `services_critical`.
    #[serde(rename = "services_critical")]
    pub services_critical: usize,
}

pub struct AlertsStatusReporter {
    pub client: Arc<AlertmanagerClient>,
}

impl AlertsStatusReporter {
    pub fn new(client: Arc<AlertmanagerClient>) -> Self {
        Self { client }
    }

    pub async fn handle(&self, _request: Request<Incoming>) -> Response<BoxBody> {
        self.handle_response().await
    }

    /// `Handler` body. Split out so tests do not need to construct an
    /// `Incoming` body.
    pub(crate) async fn handle_response(&self) -> Response<BoxBody> {
        if alertmanager_disabled() {
            return text_response(StatusCode::SERVICE_UNAVAILABLE, "alertmanager is disabled");
        }

        match self.client.alert_status().await {
            Ok(value) => json_response(StatusCode::OK, &summarize_alerts(&value)),
            Err(error) => text_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        }
    }
}

/// `DISABLE_ALERTMANAGER=true` (the baseline checks a process-wide variable
/// initialized from this env var).
fn alertmanager_disabled() -> bool {
    std::env::var("DISABLE_ALERTMANAGER").is_ok_and(|value| value == "true")
}

/// Count active, non-inhibited alerts returned by
/// `GET /alertmanager/api/v2/alerts?active=true&inhibited=false`.
pub fn summarize_alerts(alerts: &serde_json::Value) -> AlertsSummary {
    let mut summary = AlertsSummary::default();
    let mut critical_services = BTreeSet::new();

    let Some(alerts) = alerts.as_array() else {
        return summary;
    };

    for alert in alerts {
        let silenced = alert
            .get("status")
            .and_then(|status| status.get("silencedBy"))
            .and_then(|silenced_by| silenced_by.as_array())
            .is_some_and(|silenced_by| !silenced_by.is_empty());
        if silenced {
            summary.silenced += 1;
            continue;
        }

        let labels = alert.get("labels");
        let level = labels
            .and_then(|labels| labels.get("level"))
            .and_then(|level| level.as_str())
            .unwrap_or_default();
        match level {
            "warning" => summary.warning += 1,
            "critical" => {
                summary.critical += 1;
                if let Some(service) = labels
                    .and_then(|labels| labels.get("service_name"))
                    .and_then(|service| service.as_str())
                {
                    critical_services.insert(service);
                }
            }
            _ => {}
        }
    }

    summary.services_critical = critical_services.len();
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    fn alerts() -> serde_json::Value {
        serde_json::json!([
            {
                "labels": {"level": "critical", "service_name": "frontend"},
                "status": {"silencedBy": []}
            },
            {
                "labels": {"level": "critical", "service_name": "frontend"},
                "status": {"silencedBy": []}
            },
            {
                "labels": {"level": "critical", "service_name": "gitserver"},
                "status": {"silencedBy": ["silence-1"]}
            },
            {
                "labels": {"level": "warning", "service_name": "zoekt"},
                "status": {"silencedBy": []}
            },
            {
                "labels": {"level": "warning", "service_name": "zoekt"},
                "status": {"silencedBy": ["silence-2"]}
            },
            {
                "labels": {"level": "info", "service_name": "syntect"},
                "status": {"silencedBy": []}
            }
        ])
    }

    #[test]
    fn summarizes_alerts() {
        let summary = summarize_alerts(&alerts());
        assert_eq!(
            summary,
            AlertsSummary {
                warning: 1,
                silenced: 2,
                critical: 2,
                services_critical: 1,
            }
        );
    }

    #[test]
    fn serializes_with_go_field_names() {
        let summary = summarize_alerts(&alerts());
        assert_eq!(
            serde_json::to_value(summary).expect("json"),
            serde_json::json!({
                "warning": 1,
                "silenced": 2,
                "critical": 2,
                "services_critical": 1,
            })
        );
    }

    #[test]
    fn non_array_payload_is_empty() {
        assert_eq!(
            summarize_alerts(&serde_json::Value::Null),
            AlertsSummary::default()
        );
    }

    #[tokio::test]
    async fn handle_returns_summary() {
        use crate::alertmanager::test_support::MockServer;

        let server = MockServer::start(|_| (StatusCode::OK, alerts().to_string())).await;
        let reporter = AlertsStatusReporter::new(Arc::new(AlertmanagerClient::new(
            server.base_url.clone(),
            "/tmp/alertmanager.yml",
        )));
        let response = reporter.handle_response().await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("json"),
            serde_json::json!({
                "warning": 1,
                "silenced": 2,
                "critical": 2,
                "services_critical": 1,
            })
        );
    }

    #[tokio::test]
    async fn handle_reports_alertmanager_errors() {
        use crate::alertmanager::test_support::MockServer;

        let server =
            MockServer::start(|_| (StatusCode::INTERNAL_SERVER_ERROR, "boom".to_owned())).await;
        let reporter = AlertsStatusReporter::new(Arc::new(AlertmanagerClient::new(
            server.base_url.clone(),
            "/tmp/alertmanager.yml",
        )));
        let response = reporter.handle_response().await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("boom"));
    }
}
