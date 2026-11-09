#![forbid(unsafe_code)]

//! `sg-txemail`: transactional e-mail template parsing, rendering and delivery.
//!
//! Port of `internal/txemail` (Go baseline):
//!
//! * [`Templates`] / [`Message`] mirror `txtypes.Templates` and
//!   `txtypes.Message`.
//! * [`parse_template`] mirrors `txemail.ParseTemplate`: it trims each template
//!   with Go's `strings.TrimSpace`, parses the subject and the plain-text body
//!   with `text/template` semantics and the HTML body with `html/template`
//!   contextual auto-escaping.
//! * [`render`] mirrors the unexported `txemail.render`: it assembles the
//!   `From`/`To`/`Reply-To`/`Message-ID`/`References` headers and executes the
//!   three templates.
//!
//! The template engine is a subset of Go's `text/template` sufficient for every
//! template shipped in the Go baseline: field pipelines (`.Field`, `$.Field`,
//! `.`/`$`), `if`/`else if`/`else`/`end`, `range` over slices, `{{/* … */}}`
//! comments and the `{{-`/`-}}` whitespace trim markers. Anything outside that
//! subset is rejected with [`TemplateError::Unsupported`] instead of silently
//! rendering differently; see `docs/modules/06_worker.md` §108 for the list.
//!
//! [`Email`] plus [`Email::bytes`] port `jordan-wright/email`'s `Bytes()` for
//! the no-attachment case that `txemail` uses (see [`mime`]).
//!
//! [`send`] and [`prepare_email`] port `txemail.Send`: configuration checks,
//! Mandrill/additional headers, `Email.Bytes()`, and delivery through the
//! [`smtp`] client (EHLO/HELO, STARTTLS, AUTH PLAIN/CRAM-MD5, MAIL/RCPT/DATA).
//!
//! Not ported: the `html2text` fallback for templates without a `Text` body and
//! MIME attachments. [`parse_template`] returns
//! [`TxEmailError::Html2TextNotPorted`] instead of emitting a divergent body.

mod address;
mod escape;
mod mail;
mod mime;
pub mod send;
pub mod smtp;
mod template;

use std::collections::BTreeMap;

pub use address::mail_address;
pub use mime::{Email, MimeError, MimeRuntime};
pub use send::{prepare_email, send, Header, SendConfig, SendError, SmtpConfig};
pub use smtp::Client as SmtpClient;
pub use template::{TemplateError, TemplateMode};

/// `txtypes.Templates`: subject plus HTML and plain-text bodies.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Templates {
    /// `text/template` source for the subject. Required.
    pub subject: String,
    /// `html/template` source for the HTML body. Required.
    pub html: String,
    /// `text/template` source for the plain-text body. When empty the Go
    /// baseline derives it from `html` with `html2text`.
    pub text: String,
}

/// `txtypes.Message`: one transactional e-mail.
#[derive(Clone, Debug)]
pub struct Message {
    pub to: Vec<String>,
    pub reply_to: Option<String>,
    pub message_id: Option<String>,
    pub references: Vec<String>,
    pub template: Templates,
    /// Template data, modelled as JSON. The Go baseline passes a Go value; the
    /// template engine only observes exported field names / map keys, which
    /// JSON preserves.
    pub data: serde_json::Value,
}

/// The fields `txemail.render` fills on `jordan-wright/email.Email`, before
/// `Bytes()` adds the random `Date`, `Message-Id` and MIME boundaries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedMessage {
    pub to: Vec<String>,
    pub from: String,
    pub reply_to: Option<Vec<String>>,
    /// `email.Email.Headers`: `Message-ID` and `References` when requested.
    pub headers: BTreeMap<String, Vec<String>>,
    pub subject: String,
    pub text: String,
    pub html: String,
}

impl RenderedMessage {
    /// Converts the rendered fields into the `jordan-wright/email` message that
    /// `txemail.Send` serializes with `Bytes()`.
    pub fn into_email(self) -> Email {
        Email {
            reply_to: self.reply_to.unwrap_or_default(),
            from: self.from,
            to: self.to,
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: self.subject,
            text: self.text.into_bytes(),
            html: self.html.into_bytes(),
            headers: self.headers,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TxEmailError {
    #[error("template: {0}")]
    Template(#[from] TemplateError),
    // Go's ParseTemplate derives the text body with html2text.HTML2Text when
    // `Text` is empty. Porting that converter is a separate slice; returning an
    // explicit error keeps the gap visible instead of rendering a different
    // plain-text body.
    #[error("text body is empty and the html2text fallback is not ported")]
    Html2TextNotPorted,
}

/// Parsed form of [`Templates`].
#[derive(Clone, Debug)]
pub struct ParsedTemplates {
    subject: template::Template,
    text: template::Template,
    html: template::Template,
}

impl ParsedTemplates {
    /// Executes the subject, plain-text and HTML templates in that order, like
    /// `renderTemplate`. Returns `(subject, text, html)`.
    pub fn render(
        &self,
        data: &serde_json::Value,
    ) -> Result<(String, String, String), TemplateError> {
        let subject = self.subject.execute(data)?;
        let text = self.text.execute(data)?;
        let html = self.html.execute(data)?;
        Ok((subject, text, html))
    }
}

/// Port of `txemail.ParseTemplate`.
pub fn parse_template(input: &Templates) -> Result<ParsedTemplates, TxEmailError> {
    if input.text.is_empty() {
        return Err(TxEmailError::Html2TextNotPorted);
    }
    let subject = template::Template::parse(trim_space(&input.subject), TemplateMode::Text)?;
    let text = template::Template::parse(trim_space(&input.text), TemplateMode::Text)?;
    let html = template::Template::parse(trim_space(&input.html), TemplateMode::Html)?;
    Ok(ParsedTemplates {
        subject,
        text,
        html,
    })
}

/// Port of the unexported `txemail.render`.
pub fn render(
    from_address: &str,
    from_name: &str,
    message: &Message,
) -> Result<RenderedMessage, TxEmailError> {
    let mut headers = BTreeMap::new();
    if let Some(message_id) = &message.message_id {
        headers.insert("Message-ID".to_owned(), vec![message_id.clone()]);
    }
    if !message.references.is_empty() {
        // jordan-wright/email does not support lists, so the baseline builds
        // the References header itself, wrapping every entry in angle brackets.
        let references = message
            .references
            .iter()
            .map(|reference| format!("<{reference}>"))
            .collect::<Vec<_>>()
            .join(" ");
        headers.insert("References".to_owned(), vec![references]);
    }

    let parsed = parse_template(&message.template)?;
    let (subject, text, html) = parsed.render(&message.data)?;

    Ok(RenderedMessage {
        to: message.to.clone(),
        from: address::mail_address(from_name, from_address),
        reply_to: message
            .reply_to
            .as_ref()
            .map(|reply_to| vec![reply_to.clone()]),
        headers,
        subject,
        text,
        html,
    })
}

/// Go's `strings.TrimSpace`: Unicode white space at both ends.
fn trim_space(input: &str) -> &str {
    input.trim_matches(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(subject: &str, text: &str, html: &str, data: serde_json::Value) -> Message {
        Message {
            to: vec!["dest@example.com".to_owned()],
            reply_to: None,
            message_id: None,
            references: Vec::new(),
            template: Templates {
                subject: subject.to_owned(),
                text: text.to_owned(),
                html: html.to_owned(),
            },
            data,
        }
    }

    #[test]
    fn renders_a_field_pipeline_in_both_modes() {
        let rendered = render(
            "noreply@example.com",
            "Sourcegraph",
            &message(
                "Hi {{.Name}}",
                "Hi {{.Name}}",
                "Hi <b>{{.Name}}</b>",
                serde_json::json!({"Name": "A <B> & C+ \"D\""}),
            ),
        )
        .expect("render");
        assert_eq!(rendered.subject, "Hi A <B> & C+ \"D\"");
        assert_eq!(rendered.text, "Hi A <B> & C+ \"D\"");
        assert_eq!(
            rendered.html,
            "Hi <b>A &lt;B&gt; &amp; C&#43; &#34;D&#34;</b>"
        );
        assert_eq!(rendered.from, "\"Sourcegraph\" <noreply@example.com>");
    }

    #[test]
    fn trims_space_like_the_baseline() {
        let rendered = render(
            "a@b.c",
            "n",
            &message(
                "  subject  ",
                "  x  ",
                "  <p>y</p>  ",
                serde_json::json!({}),
            ),
        )
        .expect("render");
        assert_eq!(rendered.subject, "subject");
        assert_eq!(rendered.text, "x");
        assert_eq!(rendered.html, "<p>y</p>");
    }

    #[test]
    fn reports_the_html2text_gap() {
        let error = parse_template(&Templates {
            subject: "s".to_owned(),
            html: "<p>x</p>".to_owned(),
            text: String::new(),
        })
        .expect_err("fallback not ported");
        assert!(matches!(error, TxEmailError::Html2TextNotPorted));
    }
}
