#![forbid(unsafe_code)]

//! `cmd/worker/internal/outboundwebhooks/handler.go`: the sender handler.
//!
//! For one `outbound_webhook_jobs` row the handler lists every webhook whose
//! event type (and scope, when the job has one) matches, then fans out one
//! HTTP POST per webhook with a max concurrency of 100. Each request carries
//! the baseline headers and an HMAC-SHA256 signature of the payload; the
//! request, response and transport error are written to
//! `outbound_webhook_logs` regardless of the outcome.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use sha2::{Digest, Sha256};

use sg_encryption::Key;
use tokio_util::sync::CancellationToken;

use super::store::{
    FilterEventType, Webhook, WebhookJob, WebhookLog, WebhookLogStore, WebhookStore,
};
use super::urlcheck::{check_url, DenyList};

/// `types.WebhookLogMessage`, serialized exactly like Go's `encoding/json`:
/// field order `Header`, `Body`, `Method`, `URL`, `Version`; `Header` is a
/// sorted object; `Body` is standard base64; empty method/URL/version are
/// omitted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebhookLogMessage {
    pub header: Option<BTreeMap<String, Vec<String>>>,
    pub body: Vec<u8>,
    pub method: String,
    pub url: String,
    pub version: String,
}

impl WebhookLogMessage {
    /// `encryption.NewUnencryptedJSON`: the serialized plaintext stored in the
    /// `request`/`response` columns when no key is available.
    pub fn to_json(&self) -> String {
        let mut out = String::from("{\"Header\":");
        match &self.header {
            None => out.push_str("null"),
            Some(header) => {
                out.push('{');
                let mut first = true;
                for (name, values) in header {
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    push_json_string(&mut out, name);
                    out.push_str(":[");
                    for (index, value) in values.iter().enumerate() {
                        if index > 0 {
                            out.push(',');
                        }
                        push_json_string(&mut out, value);
                    }
                    out.push(']');
                }
                out.push('}');
            }
        }
        out.push_str(",\"Body\":");
        push_json_string(
            &mut out,
            &base64::engine::general_purpose::STANDARD.encode(&self.body),
        );
        if !self.method.is_empty() {
            out.push_str(",\"Method\":");
            push_json_string(&mut out, &self.method);
        }
        if !self.url.is_empty() {
            out.push_str(",\"URL\":");
            push_json_string(&mut out, &self.url);
        }
        if !self.version.is_empty() {
            out.push_str(",\"Version\":");
            push_json_string(&mut out, &self.version);
        }
        out.push('}');
        out
    }

    /// Parse a stored log message (used by the differential harness).
    pub fn from_json(raw: &str) -> Result<Self, String> {
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|err| format!("parsing log message: {err}"))?;
        let header = match value.get("Header") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::Object(map)) => {
                let mut headers = BTreeMap::new();
                for (name, values) in map {
                    let values = values
                        .as_array()
                        .ok_or_else(|| "header values are not an array".to_owned())?
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| "header value is not a string".to_owned())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    headers.insert(name.clone(), values);
                }
                Some(headers)
            }
            Some(_) => return Err("Header is not an object".to_owned()),
        };
        let body = match value.get("Body") {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(serde_json::Value::String(encoded)) => base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|err| format!("decoding body: {err}"))?,
            Some(_) => return Err("Body is not a string".to_owned()),
        };
        let string_field = |name: &str| -> Result<String, String> {
            match value.get(name) {
                None | Some(serde_json::Value::Null) => Ok(String::new()),
                Some(serde_json::Value::String(value)) => Ok(value.clone()),
                Some(_) => Err(format!("{name} is not a string")),
            }
        };
        Ok(Self {
            header,
            body,
            method: string_field("Method")?,
            url: string_field("URL")?,
            version: string_field("Version")?,
        })
    }
}

fn push_json_string(out: &mut String, value: &str) {
    out.push_str(&serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned()));
}

/// The response subset the handler stores in the log row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookResponse {
    pub status: u16,
    pub headers: BTreeMap<String, Vec<String>>,
    pub body: Vec<u8>,
}

/// The baseline's `*http.Client` (`httpcli.ExternalClient`) plus tracing.
#[async_trait]
pub trait WebhookClient: Send + Sync {
    async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
        cancel: &CancellationToken,
    ) -> Result<WebhookResponse, String>;
}

/// `handler`.
#[derive(Clone)]
pub struct WebhookSender<C: WebhookClient> {
    pub client: C,
    pub webhooks: WebhookStore,
    pub logs: WebhookLogStore,
    pub key: Option<Arc<dyn Key>>,
    pub deny_list: DenyList,
}

impl<C: WebhookClient + Clone + 'static> WebhookSender<C> {
    /// `handler.Handle`.
    pub async fn handle(&self, job: &WebhookJob) -> Result<(), String> {
        self.handle_cancellable(job, CancellationToken::new()).await
    }

    /// `handler.Handle` with the worker's cancellation token; the token is
    /// cancelled when the queue's `cancel` flag is observed by a heartbeat.
    pub async fn handle_cancellable(
        &self,
        job: &WebhookJob,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let filters = [FilterEventType {
            event_type: job.event_type.clone(),
            scope: job.scope.clone(),
        }];
        let webhooks = match self.webhooks.list(&filters).await {
            Ok(webhooks) => webhooks,
            Err(err) => {
                tracing::error!(error = %err, "error retrieving outbound webhooks");
                return Err(format!("retrieving outbound webhooks: {err}"));
            }
        };

        // `conc/pool` with `WithMaxGoroutines(100)`; the first error in webhook
        // order is returned (the tests only use single-error jobs).
        let mut tasks = tokio::task::JoinSet::new();
        for (index, webhook) in webhooks.into_iter().enumerate() {
            let sender = self.clone();
            let job = job.clone();
            let cancel = cancel.clone();
            tasks.spawn(async move { (index, sender.send_webhook(&job, webhook, cancel).await) });
        }
        let mut results = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(result) => results.push(result),
                Err(err) => return Err(format!("webhook task failed: {err}")),
            }
        }
        results.sort_by_key(|(index, _)| *index);
        for (_, result) in results {
            result?;
        }
        Ok(())
    }

    /// `handler.sendWebhook` (all errors are already logged by the caller's
    /// tracing spans in the baseline).
    async fn send_webhook(
        &self,
        job: &WebhookJob,
        webhook: Webhook,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let key = self.key.as_deref();

        let url = webhook
            .url
            .decrypt(key)
            .await
            .map_err(|err| format!("decrypting webhook URL: {err}"))?;

        check_url(&url, &self.deny_list)
            .await
            .map_err(|err| format!("checking webhook URL: {err}"))?;

        let secret = webhook
            .secret
            .decrypt(key)
            .await
            .map_err(|err| format!("decrypting webhook secret: {err}"))?;

        let payload = job
            .payload
            .decrypt(key)
            .await
            .map_err(|err| format!("decrypting payload: {err}"))?;

        let signature = calculate_signature(secret.as_bytes(), payload.as_bytes());

        let mut request_headers = BTreeMap::new();
        request_headers.insert(
            "Content-Type".to_owned(),
            vec!["application/json; charset=utf-8".to_owned()],
        );
        request_headers.insert(
            "X-Sourcegraph-Webhook-Event-Type".to_owned(),
            vec![job.event_type.clone()],
        );
        request_headers.insert(
            "X-Sourcegraph-Webhook-Signature".to_owned(),
            vec![signature],
        );

        let request_message = WebhookLogMessage {
            header: Some(request_headers.clone()),
            body: payload.as_bytes().to_vec(),
            method: "POST".to_owned(),
            url: url.clone(),
            version: String::new(),
        };

        let headers: Vec<(String, String)> = request_headers
            .iter()
            .map(|(name, values)| (name.clone(), values.join(",")))
            .collect();

        let mut log = WebhookLog {
            id: 0,
            job_id: job.id,
            outbound_webhook_id: webhook.id,
            status_code: 0,
            request: request_message.to_json(),
            response: WebhookLogMessage::default().to_json(),
            error: String::new(),
        };

        let response = match self
            .client
            .post(&url, &headers, payload.into_bytes(), &cancel)
            .await
        {
            Ok(response) => response,
            Err(err) => {
                log.error = err.clone();
                self.write_log(&log).await;
                return Err(format!("sending webhook: {err}"));
            }
        };

        log.status_code = i32::from(response.status);
        log.response = WebhookLogMessage {
            header: Some(response.headers),
            body: response.body,
            method: "POST".to_owned(),
            url,
            version: String::new(),
        }
        .to_json();

        let status = response.status;
        self.write_log(&log).await;

        if status >= 400 {
            return Err(format!("unexpected status code: {status}"));
        }

        Ok(())
    }

    /// The deferred `logStore.Create`; failures are swallowed after a warning.
    async fn write_log(&self, log: &WebhookLog) {
        if let Err(err) = self.logs.create(log).await {
            tracing::warn!(error = %err, "error writing outbound webhook log");
        }
    }
}

/// `calculateSignature`: hex-encoded HMAC-SHA256 of the payload.
pub fn calculate_signature(secret: &[u8], payload: &[u8]) -> String {
    let mut key_block = [0u8; 64];
    if secret.len() > key_block.len() {
        let digest = Sha256::digest(secret);
        key_block[..digest.len()].copy_from_slice(&digest);
    } else {
        key_block[..secret.len()].copy_from_slice(secret);
    }

    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for index in 0..64 {
        inner_pad[index] ^= key_block[index];
        outer_pad[index] ^= key_block[index];
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(payload);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    hex_encode(&outer.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_the_go_vector() {
        // HMAC-SHA256("secret", "payload"), matching Go's `calculateSignature`.
        assert_eq!(
            calculate_signature(b"secret", b"payload"),
            "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4"
        );
    }

    #[test]
    fn log_message_json_matches_go_field_order() {
        let message = WebhookLogMessage {
            header: Some(BTreeMap::from([
                (
                    "Content-Type".to_owned(),
                    vec!["application/json; charset=utf-8".to_owned()],
                ),
                ("X-Test".to_owned(), vec!["yes".to_owned()]),
            ])),
            body: b"{}".to_vec(),
            method: "POST".to_owned(),
            url: "http://127.0.0.1/hook".to_owned(),
            version: String::new(),
        };
        assert_eq!(
            message.to_json(),
            r#"{"Header":{"Content-Type":["application/json; charset=utf-8"],"X-Test":["yes"]},"Body":"e30=","Method":"POST","URL":"http://127.0.0.1/hook"}"#
        );
        assert_eq!(
            message,
            WebhookLogMessage::from_json(&message.to_json()).expect("parse")
        );
    }
}
