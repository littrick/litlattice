#![forbid(unsafe_code)]

//! `outbound-webhook-sender` (`cmd/worker/internal/outboundwebhooks`).
//!
//! The sender's core (queue operations, webhook/log stores, `CheckURL` and the
//! fan-out handler) landed in M9.8m; M9.8n wires the three routines the
//! baseline's `Routines` builds:
//!
//! * `outbound_webhook_job_worker` — `workerutil.Worker` over
//!   [`store::WebhookJobQueue`] with `Interval: 1s`, `NumHandlers: 1`,
//!   `HeartbeatInterval: 10s`;
//! * `outbound_webhook_job_resetter` — `dbworker.Resetter` every 5 minutes;
//! * `outbound-webhooks.janitor` — `PeriodicGoroutine` every hour, deleting
//!   terminal jobs finished before `webhook.logging.retention` (default 72h).
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
//!   and `169.254.169.254`;
//! * the baseline worker derives its host name from `hostname.Get()`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::framework::dbworker::default_hostname;
use crate::framework::dbworker::{
    DbBackedRoutine, DbRecord, DbResetter, DbWorkerHandler, DbWorkerStore, ResetterOptions,
    ResetterRoutine, WorkerOptions, WorkerRoutine,
};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::jobs::webhooks::calculate_retention;
use crate::jobs::{Job, JobDeps};

use handler::WebhookSender;
use store::{WebhookJob, WebhookJobQueue, WebhookLogStore, WebhookStore};
use urlcheck::DenyList;

pub mod handler;
pub mod store;
pub mod urlcheck;

/// `workerutil.WorkerOptions.Name`.
pub const WORKER_NAME: &str = "outbound_webhook_job_worker";
/// `dbworker.ResetterOptions.Name`.
pub const RESETTER_NAME: &str = "outbound_webhook_job_resetter";
/// `janitorFrequency`.
pub const JANITOR_INTERVAL: Duration = Duration::from_secs(3600);
pub const JANITOR_NAME: &str = "outbound-webhooks.janitor";
pub const JANITOR_DESCRIPTION: &str = "cleans up stale outbound webhook jobs";

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
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let queue = Arc::new(WebhookJobQueue::new(deps.frontend.clone(), None));
        let sender = Arc::new(WebhookSender {
            client: sg_http::HttpClient::new(),
            webhooks: WebhookStore::new(deps.frontend.clone()),
            logs: WebhookLogStore::new(deps.frontend.clone(), None),
            key: None,
            deny_list: DenyList::default_denylist(),
        });

        let worker = WorkerRoutine::new(
            Arc::clone(&queue),
            sender,
            WorkerOptions {
                name: WORKER_NAME.to_owned(),
                description: String::new(),
                interval: Duration::from_secs(1),
                heartbeat_interval: Duration::from_secs(10),
                num_handlers: 1,
                worker_hostname: default_hostname(),
            },
        );

        let resetter = ResetterRoutine::new(
            Arc::clone(&queue) as Arc<dyn DbResetter>,
            ResetterOptions {
                name: RESETTER_NAME.to_owned(),
                interval: Duration::from_secs(5 * 60),
            },
        );

        let janitor = PeriodicRoutine::new(Arc::new(WebhookJanitorHandler {
            queue,
            live_config: Arc::clone(&deps.live_config),
        }))
        .with_name(JANITOR_NAME)
        .with_description(JANITOR_DESCRIPTION)
        .with_interval(JANITOR_INTERVAL);

        Ok(vec![
            BackgroundRoutine::DbBacked(Arc::new(worker) as Arc<dyn DbBackedRoutine>),
            BackgroundRoutine::DbBacked(Arc::new(resetter) as Arc<dyn DbBackedRoutine>),
            janitor.into(),
        ])
    }
}

/// `cmd/worker/internal/outboundwebhooks.makeJanitor`'s handler.
pub struct WebhookJanitorHandler {
    queue: Arc<WebhookJobQueue>,
    live_config: Arc<sg_config::LiveConfig>,
}

#[async_trait]
impl Handler for WebhookJanitorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let retention = calculate_retention(&self.live_config.current().site);
        let before = sg_db::OffsetDateTime::now_utc()
            - time::Duration::try_from(retention)
                .map_err(|_| HandlerError::new("retention outside the timestamp range"))?;

        self.queue.delete_before(before).await.map_err(|err| {
            tracing::error!(error = %err, "outbound webhook janitor error");
            HandlerError::new(err)
        })
    }
}

impl DbRecord for WebhookJob {
    fn record_id(&self) -> i64 {
        self.id
    }
}

#[async_trait]
impl DbWorkerStore for WebhookJobQueue {
    type Record = WebhookJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        WebhookJobQueue::dequeue(self, worker_hostname, sg_db::OffsetDateTime::now_utc()).await
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let mut parsed = Vec::with_capacity(ids.len());
        for id in ids {
            parsed.push(
                id.parse::<i64>()
                    .map_err(|err| format!("invalid record id {id:?}: {err}"))?,
            );
        }
        WebhookJobQueue::heartbeat(self, &parsed, None, sg_db::OffsetDateTime::now_utc()).await
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        WebhookJobQueue::mark_complete(self, record.id, None).await
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        WebhookJobQueue::mark_errored(self, record.id, failure_message, None).await
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        WebhookJobQueue::mark_failed(self, record.id, failure_message, None).await
    }
}

#[async_trait]
impl DbResetter for WebhookJobQueue {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        WebhookJobQueue::reset_stalled(
            self,
            sg_db::OffsetDateTime::now_utc(),
            Duration::from_secs(10),
            5,
        )
        .await
    }
}

#[async_trait]
impl DbWorkerHandler<WebhookJobQueue> for WebhookSender<sg_http::HttpClient> {
    async fn handle(&self, ctx: CancellationToken, record: &WebhookJob) -> Result<(), String> {
        WebhookSender::handle_cancellable(self, record, ctx).await
    }
}

#[async_trait]
impl handler::WebhookClient for sg_http::HttpClient {
    async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
        cancel: &CancellationToken,
    ) -> Result<handler::WebhookResponse, String> {
        let response = tokio::select! {
            result = sg_http::HttpClient::post(self, url, headers, body) => result?,
            _ = cancel.cancelled() => return Err("context canceled".to_owned()),
        };
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

    #[test]
    fn routine_names_match_the_baseline() {
        assert_eq!(WORKER_NAME, "outbound_webhook_job_worker");
        assert_eq!(RESETTER_NAME, "outbound_webhook_job_resetter");
        assert_eq!(JANITOR_NAME, "outbound-webhooks.janitor");
        assert_eq!(JANITOR_DESCRIPTION, "cleans up stale outbound webhook jobs");
        assert_eq!(JANITOR_INTERVAL, Duration::from_secs(3600));
    }
}
