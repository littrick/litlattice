//! Port of `txemail.Send` (`internal/txemail/txemail.go`) and its config.
//!
//! The Go baseline reads `email.address`, `email.senderName` and `email.smtp`
//! from site configuration, renders the message, applies Mandrill and
//! additional headers, then drives `net/smtp`. [`SendConfig`] carries the same
//! values; a future `sg-config` binding can populate it without changing this
//! crate.
//!
//! Deviations, all documented in `docs/modules/06_worker.md` §110:
//!
//! * `MockSend` / `DisableSilently` are Go test hooks. Rust callers inject an
//!   `EmailSender` implementation (see `sg-service-worker`) instead; [`send`]
//!   performs the real delivery only.
//! * The `src_email_send` Prometheus counter is metrics infrastructure that
//!   the Rust process exposes through a different stack; it is not emitted
//!   here (the counter has no functional effect on delivery).
//! * The 30s context deadline only bounds dialing, exactly like Go: after the
//!   dial returns, `DialContext`'s deadline does not close the connection.

use std::error::Error as StdError;
use std::fmt;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use crate::smtp::{self, Auth, Client};
use crate::{render, Email, Message};

/// `30 * time.Second` in `txemail.Send`.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// `schema.Header` / `email.smtp.additionalHeaders`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: String,
}

/// `schema.SMTPServerConfig` / `email.smtp`.
#[derive(Clone, Debug, Default)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    /// `"none"`, `"PLAIN"` or `"CRAM-MD5"`.
    pub authentication: String,
    /// `email.smtp.domain`: the name sent in `EHLO` / `HELO`.
    pub domain: String,
    /// `email.smtp.noVerifyTLS`: `tls.Config.InsecureSkipVerify`.
    pub no_verify_tls: bool,
    pub additional_headers: Vec<Header>,
}

/// The site-configuration values `txemail.Send` reads.
#[derive(Clone, Debug, Default)]
pub struct SendConfig {
    /// `email.address` (required).
    pub email_address: String,
    /// `email.senderName`; the baseline falls back to `"Sourcegraph"` when
    /// empty (`conf.EmailSenderName`).
    pub sender_name: String,
    /// `email.smtp` (required).
    pub smtp: Option<SmtpConfig>,
}

/// Errors from [`send`]. `Display` reproduces `txemail.Send`'s own error
/// strings, e.g. `send MAIL: 550 5.1.1 ...`.
#[derive(Debug)]
pub enum SendError {
    /// An error the baseline formats itself, without a wrapped cause.
    Message(String),
    /// `errors.Wrap(err, context)`: `"<context>: <cause>"`.
    Wrapped {
        context: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Message(message) => f.write_str(message),
            SendError::Wrapped { context, source } => write!(f, "{context}: {source}"),
        }
    }
}

impl StdError for SendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            SendError::Message(_) => None,
            SendError::Wrapped { source, .. } => Some(source.as_ref()),
        }
    }
}

fn wrap(context: &'static str, source: impl StdError + Send + Sync + 'static) -> SendError {
    SendError::Wrapped {
        context,
        source: Box::new(source),
    }
}

/// Builds the [`Email`] that [`send`] serializes: `txemail.render`, the
/// Mandrill header overrides and `email.smtp.additionalHeaders`, in the
/// baseline's order.
pub fn prepare_email(config: &SendConfig, message: &Message) -> Result<Email, SendError> {
    let sender_name = if config.sender_name.is_empty() {
        "Sourcegraph"
    } else {
        config.sender_name.as_str()
    };
    let rendered = render(&config.email_address, sender_name, message)
        .map_err(|error| wrap("render", error))?;
    let mut email = rendered.into_email();

    let Some(smtp) = &config.smtp else {
        return Ok(email);
    };

    // Disable Mandrill features, because they make the emails look sketchy.
    if smtp.host == "smtp.mandrillapp.com" {
        email
            .headers
            .insert("X-MC-Track".to_owned(), vec!["noclicks".to_owned()]);
        email
            .headers
            .insert("X-MC-AutoText".to_owned(), vec!["false".to_owned()]);
        email
            .headers
            .insert("X-MC-AutoHTML".to_owned(), vec!["false".to_owned()]);
        email
            .headers
            .insert("X-MC-ViewContentLink".to_owned(), vec!["false".to_owned()]);
    }

    // `textproto.MIMEHeader.Add` canonicalizes the key and appends the value.
    for header in &smtp.additional_headers {
        email
            .headers
            .entry(canonical_mime_header_key(&header.key))
            .or_default()
            .push(header.value.clone());
    }
    Ok(email)
}

/// Port of `txemail.Send`: validates the configuration, renders and serializes
/// the message, then delivers it through the ported `net/smtp` client.
pub fn send(config: &SendConfig, message: &Message) -> Result<(), SendError> {
    if config.email_address.is_empty() {
        return Err(SendError::Message(
            "no \"From\" email address configured (in email.address)".to_owned(),
        ));
    }
    let Some(smtp_config) = &config.smtp else {
        return Err(SendError::Message(
            "no SMTP server configured (in email.smtp)".to_owned(),
        ));
    };

    let email = prepare_email(config, message)?;
    let raw = email.bytes().map_err(|error| wrap("get bytes", error))?;

    let address = join_host_port(&smtp_config.host, smtp_config.port);
    let stream = dial(&smtp_config.host, smtp_config.port).map_err(|error| {
        SendError::Message(format!(
            "establishing TCP connection to {address:?}: {error}"
        ))
    })?;

    let mut client =
        Client::new(stream, &smtp_config.host).map_err(|error| wrap("new SMTP client", error))?;

    // Some services (e.g. Google SMTP relay) require echoing a desired
    // hostname; the baseline's dependency always says "localhost" otherwise.
    let helo_hostname = if smtp_config.domain.is_empty() {
        "localhost"
    } else {
        smtp_config.domain.as_str()
    };
    client
        .hello(helo_hostname)
        .map_err(|error| wrap("send HELO", error))?;

    if client.extension("STARTTLS").0 {
        client
            .start_tls(smtp_config.no_verify_tls)
            .map_err(|error| wrap("send STARTTLS", error))?;
    }

    let auth = match smtp_config.authentication.as_str() {
        "none" => None,
        "PLAIN" => Some(Auth::plain(
            "",
            &smtp_config.username,
            &smtp_config.password,
            &smtp_config.host,
        )),
        "CRAM-MD5" => Some(Auth::cram_md5(&smtp_config.username, &smtp_config.password)),
        other => {
            return Err(SendError::Message(format!(
                "invalid SMTP authentication type {other:?}"
            )))
        }
    };
    if let Some(auth) = auth {
        if client.extension("AUTH").0 {
            client.auth(&auth).map_err(|error| wrap("auth", error))?;
        }
    }

    client
        .mail(&config.email_address)
        .map_err(|error| wrap("send MAIL", error))?;
    for recipient in &email.to {
        client
            .rcpt(recipient)
            .map_err(|error| wrap("send RCPT", error))?;
    }
    let mut writer = client.data().map_err(|error| wrap("send DATA", error))?;
    writer
        .write_all(&raw)
        .map_err(|error| wrap("write", error))?;
    writer.finish().map_err(|error| wrap("close", error))?;

    client.quit().map_err(|error| wrap("send QUIT", error))?;
    Ok(())
}

/// `net.JoinHostPort`.
fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// `net.Dialer.DialContext` with the 30s deadline: resolve, then try each
/// address until one connects or the deadline passes.
fn dial(host: &str, port: u16) -> io::Result<TcpStream> {
    let addresses: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no addresses resolved",
        ));
    }
    let deadline = Instant::now() + DIAL_TIMEOUT;
    let mut last_error = None;
    for address in addresses {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match TcpStream::connect_timeout(&address, remaining) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "dial deadline exceeded")))
}

/// `textproto.CanonicalMIMEHeaderKey` (`net/textproto/reader.go`).
fn canonical_mime_header_key(key: &str) -> String {
    let bytes = key.as_bytes();
    let mut no_canonicalization = false;
    for &byte in bytes {
        if valid_header_field_byte(byte) {
            continue;
        }
        // A space is accepted in a malformed header but suppresses
        // canonicalization; anything else is not a header key at all.
        if byte == b' ' {
            no_canonicalization = true;
            continue;
        }
        return key.to_owned();
    }
    if no_canonicalization {
        return key.to_owned();
    }

    let mut canonical = bytes.to_vec();
    let mut upper = true;
    for byte in &mut canonical {
        if upper && byte.is_ascii_lowercase() {
            *byte = byte.to_ascii_uppercase();
        } else if !upper && byte.is_ascii_uppercase() {
            *byte = byte.to_ascii_lowercase();
        }
        upper = *byte == b'-';
    }
    String::from_utf8(canonical).expect("header keys are ASCII")
}

/// `textproto.validHeaderFieldByte`.
fn valid_header_field_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'0'..=b'9'
            | b'a'..=b'z'
            | b'A'..=b'Z'
            | b'!'
            | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
    )
}

// Keep the smtp module reachable for callers that construct a client directly
// (and for the differential harness).
pub use smtp::{Error as SmtpError, ServerInfo};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, Templates};
    use serde_json::json;

    fn message() -> Message {
        Message {
            to: vec!["dest@example.com".to_owned()],
            reply_to: None,
            message_id: None,
            references: Vec::new(),
            template: Templates {
                subject: "Subject".to_owned(),
                text: "Text\n".to_owned(),
                html: "<p>HTML</p>\n".to_owned(),
            },
            data: json!({}),
        }
    }

    #[test]
    fn canonicalizes_header_keys_like_textproto() {
        assert_eq!(canonical_mime_header_key("x-custom"), "X-Custom");
        // textproto lower-cases every letter after the first of each dash
        // token, so a caller-supplied "X-MC-Track" becomes "X-Mc-Track" (the
        // Mandrill constants are assigned as map keys and keep their casing).
        assert_eq!(canonical_mime_header_key("x-mc-track"), "X-Mc-Track");
        assert_eq!(canonical_mime_header_key("X-MC-Track"), "X-Mc-Track");
        assert_eq!(canonical_mime_header_key("content-id"), "Content-Id");
        // Spaces suppress canonicalization; other invalid bytes are returned
        // unchanged.
        assert_eq!(canonical_mime_header_key("bad key"), "bad key");
        assert_eq!(canonical_mime_header_key("bad:key"), "bad:key");
    }

    #[test]
    fn joins_host_and_port_like_net() {
        assert_eq!(join_host_port("example.com", 587), "example.com:587");
        assert_eq!(join_host_port("::1", 25), "[::1]:25");
    }

    #[test]
    fn fallback_sender_name_is_sourcegraph() {
        let config = SendConfig {
            email_address: "noreply@example.com".to_owned(),
            sender_name: String::new(),
            smtp: None,
        };
        let email = prepare_email(&config, &message()).expect("prepare");
        assert_eq!(email.from, "\"Sourcegraph\" <noreply@example.com>");
    }

    #[test]
    fn mandrill_headers_match_the_baseline() {
        let config = SendConfig {
            email_address: "noreply@example.com".to_owned(),
            sender_name: "Sourcegraph".to_owned(),
            smtp: Some(SmtpConfig {
                host: "smtp.mandrillapp.com".to_owned(),
                port: 587,
                ..SmtpConfig::default()
            }),
        };
        let email = prepare_email(&config, &message()).expect("prepare");
        assert_eq!(
            email.headers.get("X-MC-Track"),
            Some(&vec!["noclicks".to_owned()])
        );
        assert_eq!(
            email.headers.get("X-MC-AutoText"),
            Some(&vec!["false".to_owned()])
        );
        assert_eq!(
            email.headers.get("X-MC-AutoHTML"),
            Some(&vec!["false".to_owned()])
        );
        assert_eq!(
            email.headers.get("X-MC-ViewContentLink"),
            Some(&vec!["false".to_owned()])
        );
    }

    #[test]
    fn additional_headers_append_and_canonicalize() {
        let config = SendConfig {
            email_address: "noreply@example.com".to_owned(),
            sender_name: "Sourcegraph".to_owned(),
            smtp: Some(SmtpConfig {
                host: "127.0.0.1".to_owned(),
                port: 2525,
                additional_headers: vec![
                    Header {
                        key: "x-custom".to_owned(),
                        value: "one".to_owned(),
                    },
                    Header {
                        key: "X-Custom".to_owned(),
                        value: "two".to_owned(),
                    },
                ],
                ..SmtpConfig::default()
            }),
        };
        let email = prepare_email(&config, &message()).expect("prepare");
        assert_eq!(
            email.headers.get("X-Custom"),
            Some(&vec!["one".to_owned(), "two".to_owned()])
        );
    }

    #[test]
    fn rejects_missing_configuration() {
        let error = send(&SendConfig::default(), &message()).expect_err("missing address");
        assert_eq!(
            error.to_string(),
            "no \"From\" email address configured (in email.address)"
        );

        let config = SendConfig {
            email_address: "noreply@example.com".to_owned(),
            sender_name: String::new(),
            smtp: None,
        };
        let error = send(&config, &message()).expect_err("missing smtp");
        assert_eq!(
            error.to_string(),
            "no SMTP server configured (in email.smtp)"
        );
    }
}
