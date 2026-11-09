#![forbid(unsafe_code)]

//! `actionRunner` (`internal/codemonitors/background/workers.go`): the
//! handler the action dbworker runs for one `cm_action_jobs` row.
//!
//! [`ActionRunner::handle`] dispatches on the action type exactly like the
//! baseline switch:
//!
//! * e-mail → `GetActionJobMetadata`, `GetEmailAction`, `ListRecipients`,
//!   `NewTemplateDataForNewSearchResults`, then one send per recipient
//!   (org recipients are skipped; a recipient with neither namespace is
//!   `nil recipient`);
//! * webhook → `GetActionJobMetadata`, `GetWebhookAction`,
//!   `sendWebhookNotification` (the payload layer ported in M9.8cl);
//! * Slack webhook → the same with `GetSlackWebhookAction` and
//!   `sendSlackNotification`.
//!
//! Error wrapping matches the baseline: `Email: …`, `Webhook: …`,
//! `SlackWebhook: …`, and
//! `job must be one of type email, webhook, or slack webhook`.
//!
//! The e-mail **transport** is injected through [`EmailSender`]: the
//! baseline's `SendEmailForNewSearchResult` is a package variable, and its
//! real implementation (`txemail` + the HTML/text templates) is not ported
//! yet. The runner therefore performs the same database reads and template
//! data construction as the baseline and hands the result to the sender.
//!
//! Also not ported: the baseline wraps each handler body in
//! `CodeMonitorStore.Transact`; the reads here are already a consistent set
//! of single statements, and no writes happen in the handler.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use url::Url;

use sg_db::code_monitors::{self, ActionJob};

use super::code_monitor_actions::{
    new_template_data_for_new_search_results, send_slack_notification, send_webhook_notification,
    ActionArgs, CommitMatch, EmailActionInput, MatchedRange, MatchedString,
    TemplateDataNewSearchResults, UTM_SOURCE_EMAIL,
};
use super::code_monitor_workers::ActionJobStore;
use crate::framework::dbworker::DbWorkerHandler;

/// `handleWebhook`'s `UTMSource`.
pub const UTM_SOURCE_WEBHOOK: &str = "code-monitor-webhook";
/// `handleSlackWebhook`'s `UTMSource`.
pub const UTM_SOURCE_SLACK_WEBHOOK: &str = "code-monitor-slack-webhook";

/// The baseline's `SendEmailForNewSearchResult`. The real transport is not
/// ported; the default runner requires one to be injected.
#[async_trait]
pub trait EmailSender: Send + Sync + 'static {
    async fn send(&self, user_id: i32, data: &TemplateDataNewSearchResults) -> Result<(), String>;
}

/// `actionRunner`.
pub struct ActionRunner {
    pool: PgPool,
    /// `url.Parse(conf.Get().ExternalURL)`.
    external_url: Url,
    client: reqwest::Client,
    email_sender: Arc<dyn EmailSender>,
}

impl ActionRunner {
    pub fn new(
        pool: PgPool,
        external_url: Url,
        client: reqwest::Client,
        email_sender: Arc<dyn EmailSender>,
    ) -> Self {
        Self {
            pool,
            external_url,
            client,
            email_sender,
        }
    }

    /// `(*actionRunner).Handle`.
    pub async fn handle(&self, job: &ActionJob) -> Result<(), String> {
        // The baseline's switch checks Email, then Webhook, then
        // SlackWebhook; the schema's `cm_action_jobs_only_one_action_type`
        // check makes the order unobservable for valid rows.
        if let Some(email) = job.email {
            return self
                .handle_email(job.id, email)
                .await
                .map_err(|err| format!("Email: {err}"));
        }
        if let Some(webhook) = job.webhook {
            return self
                .handle_webhook(job.id, webhook)
                .await
                .map_err(|err| format!("Webhook: {err}"));
        }
        if let Some(slack_webhook) = job.slack_webhook {
            return self
                .handle_slack_webhook(job.id, slack_webhook)
                .await
                .map_err(|err| format!("SlackWebhook: {err}"));
        }
        Err("job must be one of type email, webhook, or slack webhook".to_owned())
    }

    async fn metadata(&self, job_id: i32) -> Result<(Metadata, Vec<CommitMatch>), String> {
        let metadata = code_monitors::get_action_job_metadata(&self.pool, job_id)
            .await
            .map_err(|err| format!("GetActionJobMetadata: {err}"))?;
        let results = commit_matches_from_json(&metadata.results)
            .map_err(|err| format!("GetActionJobMetadata: {err}"))?;
        Ok((
            Metadata {
                description: metadata.description,
                query: metadata.query.unwrap_or_default(),
                monitor_id: metadata.monitor_id,
                owner_name: metadata.owner_name,
            },
            results,
        ))
    }

    /// `(*actionRunner).handleEmail`.
    async fn handle_email(&self, job_id: i32, email_id: i64) -> Result<(), String> {
        let (metadata, results) = self.metadata(job_id).await?;

        let email = code_monitors::get_email_action(&self.pool, email_id)
            .await
            .map_err(|err| format!("GetEmailAction: {err}"))?;

        let recipients = code_monitors::list_recipients(&self.pool, email_id)
            .await
            .map_err(|err| format!("ListRecipients: {err}"))?;

        let args = ActionArgs {
            monitor_description: &metadata.description,
            external_url: &self.external_url,
            monitor_id: metadata.monitor_id,
            utm_source: UTM_SOURCE_EMAIL,
            monitor_owner_name: &metadata.owner_name,
            query: &metadata.query,
            results: &results,
            include_results: email.include_results,
        };
        let data = new_template_data_for_new_search_results(
            &args,
            &EmailActionInput {
                monitor: email.monitor,
                priority: email.priority,
            },
        );

        for recipient in recipients {
            if recipient.namespace_org_id.is_some() {
                // TODO (baseline): send emails to org members.
                continue;
            }
            let Some(user_id) = recipient.namespace_user_id else {
                return Err("nil recipient".to_owned());
            };
            self.email_sender.send(user_id, &data).await?;
        }
        Ok(())
    }

    /// `(*actionRunner).handleWebhook`.
    async fn handle_webhook(&self, job_id: i32, webhook_id: i64) -> Result<(), String> {
        let (metadata, results) = self.metadata(job_id).await?;

        let webhook = code_monitors::get_webhook_action(&self.pool, webhook_id)
            .await
            .map_err(|err| format!("GetWebhookAction: {err}"))?;

        let args = ActionArgs {
            monitor_description: &metadata.description,
            external_url: &self.external_url,
            monitor_id: webhook.monitor,
            utm_source: UTM_SOURCE_WEBHOOK,
            monitor_owner_name: &metadata.owner_name,
            query: &metadata.query,
            results: &results,
            include_results: webhook.include_results,
        };
        send_webhook_notification(&self.client, &webhook.url, &args).await
    }

    /// `(*actionRunner).handleSlackWebhook`.
    async fn handle_slack_webhook(&self, job_id: i32, slack_webhook_id: i64) -> Result<(), String> {
        let (metadata, results) = self.metadata(job_id).await?;

        let webhook = code_monitors::get_slack_webhook_action(&self.pool, slack_webhook_id)
            .await
            .map_err(|err| format!("GetSlackWebhookAction: {err}"))?;

        let args = ActionArgs {
            monitor_description: &metadata.description,
            external_url: &self.external_url,
            monitor_id: webhook.monitor,
            utm_source: UTM_SOURCE_SLACK_WEBHOOK,
            monitor_owner_name: &metadata.owner_name,
            query: &metadata.query,
            results: &results,
            include_results: webhook.include_results,
        };
        send_slack_notification(&self.client, &webhook.url, &args).await
    }
}

/// The fields of `database.ActionJobMetadata` the runner uses.
struct Metadata {
    description: String,
    query: String,
    monitor_id: i64,
    owner_name: String,
}

/// `watch` the action worker dequeues with and hands to [`ActionRunner`].
#[async_trait]
impl DbWorkerHandler<ActionJobStore> for ActionRunner {
    async fn handle(&self, _ctx: CancellationToken, record: &ActionJob) -> Result<(), String> {
        ActionRunner::handle(self, record).await
    }
}

/// `json.Unmarshal(search_results, &[]*result.CommitMatch)` restricted to the
/// fields the action payloads read (`Repo.Name`, `Commit.ID`,
/// `MessagePreview`/`DiffPreview` and their matched ranges).
///
/// The stored form is [`result.CommitMatch`]'s custom `MarshalJSON`
/// (`stableCommitMatchJSON`): camelCase keys, no `Diff` field. Its
/// `UnmarshalJSON` also re-parses `diffPreview` into structured hunks, which
/// is not needed here — every stored value was produced by the baseline's
/// marshaler.
#[derive(Debug, Deserialize)]
struct StoredCommitMatch {
    #[serde(rename = "repoName")]
    repo_name: String,
    #[serde(rename = "commitID")]
    commit_id: String,
    #[serde(rename = "messagePreview", default)]
    message_preview: Option<StoredMatchedString>,
    #[serde(rename = "diffPreview", default)]
    diff_preview: Option<StoredMatchedString>,
}

#[derive(Debug, Deserialize)]
struct StoredMatchedString {
    content: String,
    #[serde(rename = "matchedRanges", default)]
    matched_ranges: Vec<StoredRange>,
}

#[derive(Debug, Deserialize)]
struct StoredRange {
    start: StoredLocation,
    end: StoredLocation,
}

/// `result.Location` marshals as `[offset, line, column]` (its
/// `MarshalJSON` shrinks the field names), so the range endpoints are
/// three-element arrays.
type StoredLocation = [usize; 3];

/// Decode the `cm_trigger_jobs.search_results` JSON the baseline scans into
/// `[]*result.CommitMatch`.
pub fn commit_matches_from_json(value: &serde_json::Value) -> Result<Vec<CommitMatch>, String> {
    if value.is_null() {
        // `json.Unmarshal(nil, ...)` reports exactly this.
        return Err("unexpected end of JSON input".to_owned());
    }
    let stored: Vec<StoredCommitMatch> =
        serde_json::from_value(value.clone()).map_err(|err| err.to_string())?;
    Ok(stored
        .into_iter()
        .map(|stored| CommitMatch {
            repo_name: stored.repo_name,
            commit_id: stored.commit_id,
            message_preview: stored.message_preview.map(to_matched_string),
            diff_preview: stored.diff_preview.map(to_matched_string),
        })
        .collect())
}

fn to_matched_string(stored: StoredMatchedString) -> MatchedString {
    MatchedString {
        content: stored.content,
        matched_ranges: stored
            .matched_ranges
            .into_iter()
            .map(|range| MatchedRange {
                start_offset: range.start[0],
                end_offset: range.end[0],
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_stored_search_results_shape() {
        let value = serde_json::json!([
            {
                "repoID": 1,
                "repoName": "alpha",
                "repoStars": 0,
                "commitID": "abcdef1234567890",
                "author": {"name": "a", "email": "a@b", "date": "2020-01-01T00:00:00Z"},
                "message": "m",
                "messagePreview": {"content": "alpha", "matchedRanges": [{"start": [0, 0, 0], "end": [5, 0, 5]}]}
            }
        ]);
        let matches = commit_matches_from_json(&value).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].repo_name, "alpha");
        assert_eq!(matches[0].commit_id, "abcdef1234567890");
        let preview = matches[0].message_preview.as_ref().unwrap();
        assert_eq!(preview.content, "alpha");
        assert_eq!(
            preview.matched_ranges,
            vec![MatchedRange {
                start_offset: 0,
                end_offset: 5
            }]
        );
        assert!(matches[0].diff_preview.is_none());
    }

    #[test]
    fn null_results_report_the_go_unmarshal_error() {
        assert_eq!(
            commit_matches_from_json(&serde_json::Value::Null),
            Err("unexpected end of JSON input".to_owned())
        );
    }
}
