#![forbid(unsafe_code)]

//! `outbound-webhook-sender` (`cmd/worker/internal/outboundwebhooks`).
//!
//! M9.8m ports the sender's core: the `outbound_webhook_jobs` queue operations
//! (see [`store::WebhookJobQueue`]), the `outbound_webhooks`/`outbound_webhook_logs`
//! stores and the [`handler::WebhookSender`]. The worker loop, stalled
//! resetter and hourly janitor that `Routines` wires up in the baseline are
//! not started yet, so the job still fails startup loudly like an unported job.
//!
//! Baseline facts reproduced here:
//!
//! * `keyring.Default().OutboundWebhookKey` is never populated by `NewRing`
//!   (see `sg-keyring`), so the sender runs with `key = None` and webhook
//!   URLs, secrets, payloads and log messages stay plaintext;
//! * the worker store uses `OrderByExpression: id`, `MaxNumResets: 5`,
//!   `StalledMaxAge: 10s` and no `RetryAfter`, so only `queued` rows are
//!   dequeued and an errored row is immediately `failed` (`MaxNumRetries` 0);
//! * `outbound.CheckURL` rejects loopback (unless the test denylist is active)
//!   and `169.254.169.254`.

use async_trait::async_trait;

use crate::framework::BackgroundRoutine;
use crate::jobs::{Job, JobDeps};

pub mod handler;
pub mod store;
pub mod urlcheck;

/// `outboundwebhooks.sender`.
pub struct OutboundWebhookSender;

impl OutboundWebhookSender {
    pub fn new() -> Self {
        Self
    }
}

impl Default for OutboundWebhookSender {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for OutboundWebhookSender {
    fn description(&self) -> String {
        "Outbound webhook sender".to_owned()
    }

    async fn routines(
        &self,
        _deps: &JobDeps,
        _setup_ctx: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Err(
            "job \"outbound-webhook-sender\" is not implemented by the Rust worker yet \
             (the sender stores and handler land in M9.8m; the worker loop follows)"
                .to_owned(),
        )
    }
}

#[async_trait]
impl handler::WebhookClient for sg_http::HttpClient {
    async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<handler::WebhookResponse, String> {
        let response = sg_http::HttpClient::post(self, url, headers, body).await?;
        Ok(handler::WebhookResponse {
            status: response.status,
            headers: response.headers,
            body: response.body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_description_matches_the_baseline() {
        let job = OutboundWebhookSender::new();
        assert_eq!(job.description(), "Outbound webhook sender");
        assert!(job.configs().is_empty());
    }
}
