//! `internal/codemonitors/background/email.go`: the real
//! `SendEmailForNewSearchResult` transport for code monitor e-mail actions.
//!
//! [`send_email`] ports the baseline's `sendEmail`:
//!
//! 1. `db.UserEmails().GetPrimaryEmail(ctx, userID)`, mapping the not-found
//!    error type (not any error) to
//!    `unable to send email to user ID %d with unknown email address`;
//! 2. the verified check
//!    (`unable to send email to user ID %d's unverified primary email address`);
//! 3. `txemail.Send(ctx, "code-monitor", …)` with the monitor's template and
//!    data, wrapping any failure as
//!    `send mail to email=%q userID=%d: %w`.
//!
//! [`TxEmailSender`] implements the [`EmailSender`] seam of
//! [`super::code_monitor_action_runner`] on top of it, reading the SMTP
//! settings (`email.address`, `email.senderName`, `email.smtp`) from the live
//! site configuration at send time, the way `txemail.Send` calls `conf.Get()`.
//! The blocking `sg_txemail::send` runs on [`tokio::task::spawn_blocking`].
//!
//! The subject and the HTML/plain-text bodies are the baseline's embedded
//! `email_template.html.tmpl` / `email_template.txt.tmpl`, copied verbatim
//! (see the files' sha256 in `docs/modules/06_worker.md` §112).

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_txemail::send::Header;
use sg_txemail::{Message, SendConfig, SmtpConfig, Templates};
use sqlx::PgPool;

use super::code_monitor_action_runner::EmailSender;
use super::code_monitor_actions::TemplateDataNewSearchResults;

/// `newSearchResultsEmailTemplates.Subject` (the baseline's raw string; note
/// the `{{ if }}` action deliberately keeps the spaces around `Test:`).
pub const NEW_SEARCH_RESULTS_SUBJECT: &str = "{{ if .IsTest }}Test: {{ end }}{{.Priority}}Sourcegraph code monitor {{.Description}} detected {{.TotalCount}} new {{.ResultPluralized}}";

/// `//go:embed email_template.html.tmpl`.
pub const NEW_SEARCH_RESULTS_HTML: &str =
    include_str!("code_monitor_email/search_results.html.tmpl");
/// `//go:embed email_template.txt.tmpl`.
pub const NEW_SEARCH_RESULTS_TEXT: &str =
    include_str!("code_monitor_email/search_results.txt.tmpl");

/// `newSearchResultsEmailTemplates` (`txemail.MustValidate` happens inside
/// [`sg_txemail::send`], which parses the templates on every render like the
/// Go baseline's `Send`).
pub fn new_search_results_templates() -> Templates {
    Templates {
        subject: NEW_SEARCH_RESULTS_SUBJECT.to_owned(),
        text: NEW_SEARCH_RESULTS_TEXT.to_owned(),
        html: NEW_SEARCH_RESULTS_HTML.to_owned(),
    }
}

/// `TemplateDataNewSearchResults` as the template sees it.
///
/// The Go templates address exported fields (`.Priority`,
/// `.TruncatedResults`, …), so the JSON keys are the Go field names. `serde_json`
/// maps sort their keys, which makes the output stable for the differential
/// harness; the Go side declares its mirror struct in the same order.
pub fn template_data_json(data: &TemplateDataNewSearchResults) -> serde_json::Value {
    let results: Vec<serde_json::Value> = data
        .truncated_results
        .iter()
        .map(|result| {
            serde_json::json!({
                "CommitID": result.commit_id,
                "CommitURL": result.commit_url,
                "Content": result.content,
                "RepoName": result.repo_name,
                "ResultType": result.result_type,
            })
        })
        .collect();
    serde_json::json!({
        "CodeMonitorURL": data.code_monitor_url,
        "Description": data.description,
        "DisplayMoreLink": data.display_more_link,
        "IncludeResults": data.include_results,
        "IsTest": data.is_test,
        "Priority": data.priority,
        "ResultPluralized": data.result_pluralized,
        "SearchURL": data.search_url,
        "TotalCount": data.total_count,
        "TruncatedCount": data.truncated_count,
        "TruncatedResultPluralized": data.truncated_result_pluralized,
        "TruncatedResults": results,
    })
}

/// `conf.Get()`'s e-mail settings as an [`sg_txemail::SendConfig`].
///
/// Site configuration stores these under flat keys (`"email.address"`,
/// `"email.smtp"`, …), so the raw JSON object is read directly; `sg-config`
/// has no typed binding for them yet. Missing values keep the baseline's zero
/// values, which [`sg_txemail::send`] turns into the same configuration
/// errors `txemail.Send` produces.
pub fn send_config_from_site(site: &SiteConfiguration) -> SendConfig {
    let raw = site.raw();
    SendConfig {
        email_address: string_field(raw, "email.address"),
        sender_name: string_field(raw, "email.senderName"),
        smtp: raw
            .get("email.smtp")
            .filter(|smtp| smtp.is_object())
            .map(|smtp| SmtpConfig {
                host: string_field(smtp, "host"),
                port: u16_field(smtp, "port"),
                username: string_field(smtp, "username"),
                password: string_field(smtp, "password"),
                authentication: string_field(smtp, "authentication"),
                domain: string_field(smtp, "domain"),
                no_verify_tls: smtp
                    .get("noVerifyTLS")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                additional_headers: smtp
                    .get("additionalHeaders")
                    .and_then(serde_json::Value::as_array)
                    .map(|headers| {
                        headers
                            .iter()
                            .filter(|header| header.is_object())
                            .map(|header| Header {
                                key: string_field(header, "key"),
                                value: string_field(header, "value"),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
    }
}

fn string_field(object: &serde_json::Value, key: &str) -> String {
    object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn u16_field(object: &serde_json::Value, key: &str) -> u16 {
    object
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(0)
}

/// `sendEmail(ctx, db, userID, template, data)`.
///
/// `Err` carries the baseline's `errors.Errorf` text; the caller
/// ([`TxEmailSender`]) adds no further wrapping, matching
/// `SendEmailForNewSearchResult`.
pub async fn send_email(
    pool: &PgPool,
    site: &SiteConfiguration,
    user_id: i32,
    template: Templates,
    data: &TemplateDataNewSearchResults,
) -> Result<(), String> {
    let (email, verified) = match sg_db::user_emails::get_primary_email(pool, user_id).await {
        Ok(Some(value)) => value,
        Ok(None) => {
            return Err(format!(
                "unable to send email to user ID {user_id} with unknown email address"
            ))
        }
        Err(err) => return Err(format!("get primary email for userID={user_id}: {err}")),
    };
    if !verified {
        return Err(format!(
            "unable to send email to user ID {user_id}'s unverified primary email address"
        ));
    }

    let config = send_config_from_site(site);
    let message = Message {
        to: vec![email.clone()],
        reply_to: None,
        message_id: None,
        references: Vec::new(),
        template,
        data: template_data_json(data),
    };

    let outcome = tokio::task::spawn_blocking(move || sg_txemail::send(&config, &message)).await;
    let error = match outcome {
        Ok(Ok(())) => return Ok(()),
        // A panic in the blocking task is not a delivery error the baseline
        // can produce; it is still reported through the same wrapper so the
        // job fails with context.
        Ok(Err(err)) => err.to_string(),
        Err(err) => err.to_string(),
    };
    Err(format!(
        "send mail to email={email:?} userID={user_id}: {error}"
    ))
}

/// The real [`EmailSender`]: `SendEmailForNewSearchResult` without the
/// baseline's package-level mock.
pub struct TxEmailSender {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

impl TxEmailSender {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self { pool, live_config }
    }
}

#[async_trait]
impl EmailSender for TxEmailSender {
    async fn send(&self, user_id: i32, data: &TemplateDataNewSearchResults) -> Result<(), String> {
        // `conf.Get()` is evaluated per send in `txemail.Send`.
        let site = self.live_config.current().site.clone();
        send_email(
            &self.pool,
            &site,
            user_id,
            new_search_results_templates(),
            data,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_txemail::render;

    fn sample_data(is_test: bool) -> TemplateDataNewSearchResults {
        TemplateDataNewSearchResults {
            priority: "[Critical] ".to_owned(),
            code_monitor_url: "https://sourcegraph.example.com/code-monitoring/Q29kZU1vbml0b3I6MQ==?utm_source=code-monitoring-email".to_owned(),
            search_url: "https://sourcegraph.example.com/search?q=type%3Acommit&utm_source=code-monitoring-email".to_owned(),
            description: "my monitor".to_owned(),
            include_results: true,
            truncated_results: vec![super::super::code_monitor_actions::DisplayResult {
                result_type: "Message".to_owned(),
                commit_url: "https://sourcegraph.example.com/github.com/acme/alpha/-/commit/a1b2c3d".to_owned(),
                repo_name: "github.com/acme/alpha".to_owned(),
                commit_id: "a1b2c3d".to_owned(),
                content: "crash here".to_owned(),
            }],
            total_count: 7,
            truncated_count: 1,
            result_pluralized: "results".to_owned(),
            truncated_result_pluralized: "result".to_owned(),
            display_more_link: true,
            is_test,
        }
    }

    #[test]
    fn renders_the_new_search_results_templates() {
        let data = serde_json::to_value(template_data_json(&sample_data(false))).unwrap();
        let message = Message {
            to: vec!["dest@example.test".to_owned()],
            reply_to: None,
            message_id: None,
            references: Vec::new(),
            template: new_search_results_templates(),
            data,
        };
        let rendered = render("noreply@example.com", "Sourcegraph", &message).expect("render");
        assert_eq!(
            rendered.subject,
            "[Critical] Sourcegraph code monitor my monitor detected 7 new results"
        );
        assert!(rendered
            .text
            .contains("Your Sourcegraph code monitor, my monitor, detected 7 new results."));
        assert!(rendered.text.contains(
            "- Message match: https://sourcegraph.example.com/github.com/acme/alpha/-/commit/a1b2c3d from github.com/acme/alpha@a1b2c3d"
        ));
        assert!(rendered.text.contains(
            "...and 1 more result: https://sourcegraph.example.com/search?q=type%3Acommit&utm_source=code-monitoring-email"
        ));
        assert!(rendered.html.contains(
            "Your Sourcegraph code monitor, <b>my monitor</b>, detected <b>7</b> new results."
        ));
        assert!(rendered
            .html
            .contains("https://sourcegraph.example.com/code-monitoring/"));
    }

    #[test]
    fn renders_the_test_preview_variant() {
        let data = serde_json::to_value(template_data_json(&sample_data(true))).unwrap();
        let message = Message {
            to: vec!["dest@example.test".to_owned()],
            reply_to: None,
            message_id: None,
            references: Vec::new(),
            template: new_search_results_templates(),
            data,
        };
        let rendered = render("noreply@example.com", "Sourcegraph", &message).expect("render");
        assert!(rendered
            .subject
            .starts_with("Test: [Critical] Sourcegraph code monitor"));
        assert!(rendered.text.contains("This email is a preview."));
        assert!(rendered.html.contains("This email is a preview."));
    }

    #[test]
    fn reads_flat_site_configuration_keys() {
        let site = SiteConfiguration::parse(
            r#"{
                "email.address": "noreply@example.com",
                "email.senderName": "Robot",
                "email.smtp": {
                    "host": "smtp.example.com",
                    "port": 2525,
                    "username": "u",
                    "password": "p",
                    "authentication": "PLAIN",
                    "domain": "helo.example.test",
                    "noVerifyTLS": true,
                    "additionalHeaders": [{"key": "x-a", "value": "1"}]
                }
            }"#,
        )
        .expect("parse");
        let config = send_config_from_site(&site);
        assert_eq!(config.email_address, "noreply@example.com");
        assert_eq!(config.sender_name, "Robot");
        let smtp = config.smtp.expect("smtp");
        assert_eq!(smtp.host, "smtp.example.com");
        assert_eq!(smtp.port, 2525);
        assert_eq!(smtp.authentication, "PLAIN");
        assert!(smtp.no_verify_tls);
        assert_eq!(
            smtp.additional_headers,
            vec![Header {
                key: "x-a".to_owned(),
                value: "1".to_owned(),
            }]
        );
    }
}
