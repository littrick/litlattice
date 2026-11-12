//! M9.19c differential harness: `GitHubWebhook.ServeHTTP`'s
//! `externalServiceID` lookup, configured-secret validation and event dispatch
//! (`cmd/frontend/internal/webhooks/github_webhooks.go`).
//!
//! Replays `test_ws/p9-go/data/github_webhook_seam/corpus.json` through
//! [`github_webhook_response`] with an injected [`GitHubWebhookDeps`], writing
//! the same JSON document as the Go oracle
//! (`cmd/frontend/internal/webhooks/zz_m919c_github_webhook_oracle_test.go`,
//! UNTRACKED, gated by `SG_M919C_WEBHOOKS=1`).
//!
//! Gated on `M919C_WEBHOOKS_CORPUS`/`M919C_WEBHOOKS_OUT`;
//! `run_github_webhook_seam_diff.sh` sets them.

use std::collections::HashMap;

use bytes::Bytes;
use http::{header, Request};
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use serde_json::json;
use sg_service_frontend::external_api::{
    github_webhook_response, GitHubExternalService, GitHubWebhookConfig, GitHubWebhookDeps,
    WebhookDispatchError,
};

#[derive(Deserialize)]
struct Corpus {
    services: Vec<Service>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Service {
    id: i64,
    #[serde(default = "github_kind")]
    kind: String,
    url: String,
    #[serde(default)]
    webhooks: Vec<String>,
    #[serde(default)]
    get_error: bool,
    #[serde(default)]
    config_error: bool,
}

fn github_kind() -> String {
    "github".to_owned()
}

impl Service {
    fn external_service(&self) -> GitHubExternalService {
        GitHubExternalService {
            id: self.id,
            url: self.url.clone(),
            webhooks: self
                .webhooks
                .iter()
                .map(|secret| GitHubWebhookConfig {
                    secret: secret.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    event: String,
    #[serde(default)]
    signature: Option<String>,
    #[serde(default)]
    content_type: String,
}

struct CorpusDeps {
    by_id: HashMap<i64, Service>,
}

impl GitHubWebhookDeps for CorpusDeps {
    fn get_external_service(&self, id: i64) -> Result<Option<GitHubExternalService>, String> {
        let Some(service) = self.by_id.get(&id) else {
            return Ok(None);
        };
        if service.get_error {
            return Err("harness database error".to_owned());
        }
        if service.config_error {
            return Err("invalid configuration".to_owned());
        }
        if service.kind != "github" {
            return Err(format!(
                "invalid configuration, received github webhook for non-github external service: {id}"
            ));
        }
        Ok(Some(service.external_service()))
    }

    fn list_github_external_services(&self) -> Result<Vec<GitHubExternalService>, String> {
        let mut services = Vec::new();
        let mut ordered: Vec<&Service> = self.by_id.values().collect();
        ordered.sort_by_key(|service| service.id);
        for service in ordered {
            if service.kind != "github" {
                continue;
            }
            if service.config_error {
                return Err("invalid configuration".to_owned());
            }
            services.push(service.external_service());
        }
        Ok(services)
    }

    fn dispatch(
        &self,
        event: &str,
        _code_host_url: &str,
        _body: &[u8],
    ) -> Result<(), WebhookDispatchError> {
        match event {
            "push" => Err(WebhookDispatchError::NotFound(
                "no such event target".to_owned(),
            )),
            "pull_request" => Err(WebhookDispatchError::Internal("boom".to_owned())),
            _ => Ok(()),
        }
    }
}

#[tokio::test]
async fn github_webhook_seam_diff() {
    let (Ok(corpus_path), Ok(out_path)) = (
        std::env::var("M919C_WEBHOOKS_CORPUS"),
        std::env::var("M919C_WEBHOOKS_OUT"),
    ) else {
        eprintln!("M919C_WEBHOOKS_CORPUS/M919C_WEBHOOKS_OUT not set; skipping");
        return;
    };
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");
    let by_id = corpus
        .services
        .into_iter()
        .map(|service| (service.id, service))
        .collect();
    let deps = CorpusDeps { by_id };

    let mut results = Vec::new();
    for case in &corpus.cases {
        let mut target = "/.api/github-webhooks".to_owned();
        if !case.query.is_empty() {
            target.push('?');
            target.push_str(&case.query);
        }
        let mut builder = Request::builder()
            .method("POST")
            .uri(target)
            .header(header::CONTENT_LENGTH, case.body.len().to_string());
        if let Some(signature) = &case.signature {
            builder = builder.header("x-hub-signature", signature);
        }
        if !case.event.is_empty() {
            builder = builder.header("x-github-event", &case.event);
        }
        if !case.content_type.is_empty() {
            builder = builder.header(header::CONTENT_TYPE, &case.content_type);
        }
        let mut request = builder
            .body(Full::new(Bytes::from(case.body.clone())))
            .expect("request");
        let response = github_webhook_response(&mut request, &deps).await;
        let status = response.status().as_u16();
        let header_value = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_owned()
        };
        let content_type = header_value("content-type");
        let xcto = header_value("x-content-type-options");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        results.push(json!({
            "name": case.name,
            "status": status,
            "content_type": content_type,
            "xcto": xcto,
            "body": String::from_utf8_lossy(&body),
        }));
    }

    std::fs::write(&out_path, format!("{}\n", json!(results))).expect("write output");
}
