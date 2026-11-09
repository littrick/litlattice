//! Port of `jordan-wright/email`'s `Email.Bytes()` for the subset used by
//! `internal/txemail`: a plain-text body, an HTML body, optional `Cc`/`Bcc`,
//! and no attachments.
//!
//! The serialization mirrors the dependency exactly:
//!
//! * [`Email::msg_headers`] merges the typed fields and the custom header map
//!   like `msgHeaders` (including its `Message-Id` vs `Message-ID` behaviour).
//! * [`write_headers`] mirrors `headerToBytes`: `Content-Type` is emitted
//!   verbatim, address headers are parsed and re-formatted, everything else is
//!   RFC 2047 Q-encoded with the `UTF-8` charset.
//! * Two non-empty bodies become `multipart/alternative` with quoted-printable
//!   parts; a single non-empty body becomes a single quoted-printable part.
//! * [`QuotedPrintableWriter`] is a byte-for-byte port of Go's
//!   `mime/quotedprintable.Writer`.
//!
//! Not ported: attachments (`multipart/mixed` / `multipart/related` parts and
//! base64 wrapping) — `txemail` never attaches files — and the SMTP transport
//! (`Email.Send`, `txemail.Send`), which is a separate slice.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use time::OffsetDateTime;

use crate::mail;

/// Errors raised while assembling a MIME message.
#[derive(Debug, thiserror::Error)]
pub enum MimeError {
    #[error("MIME assembly is only ported for messages without attachments")]
    AttachmentsNotPorted,
    #[error("reading randomness: {0}")]
    Random(String),
}

/// The message fields `jordan-wright/email.Email` exposes and `txemail.render`
/// fills.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Email {
    pub reply_to: Vec<String>,
    pub from: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    /// Plain-text body; `txemail` always renders one.
    pub text: Vec<u8>,
    /// HTML body.
    pub html: Vec<u8>,
    /// `email.Email.Headers`, with keys kept as the caller supplied them.
    pub headers: BTreeMap<String, Vec<String>>,
}

/// Values `Bytes()` takes from the environment: the `Date` header, the
/// generated `Message-Id`, and the multipart boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MimeRuntime {
    date: String,
    message_id: String,
    boundary: String,
}

impl MimeRuntime {
    /// Captures the current time, a random message id and a random boundary,
    /// like the Go dependency does.
    pub fn from_environment() -> Result<Self, MimeError> {
        let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
        let message_id = generate_message_id()?;
        let boundary = random_bytes(30)?
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Ok(Self {
            date: rfc1123z(now),
            message_id,
            boundary,
        })
    }

    /// Deterministic values for tests; `Date` must already be formatted.
    pub fn with_values(
        date: impl Into<String>,
        message_id: impl Into<String>,
        boundary: impl Into<String>,
    ) -> Self {
        Self {
            date: date.into(),
            message_id: message_id.into(),
            boundary: boundary.into(),
        }
    }
}

/// `time.RFC1123Z`: `Mon, 02 Jan 2006 15:04:05 -0700`.
fn rfc1123z(time: OffsetDateTime) -> String {
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let weekday = WEEKDAYS[time.weekday().number_days_from_sunday() as usize];
    let month = MONTHS[time.month() as usize - 1];
    let minutes = time.offset().whole_minutes();
    let sign = if minutes < 0 { '-' } else { '+' };
    let minutes = minutes.abs();
    format!(
        "{weekday}, {:02} {month} {:04} {:02}:{:02}:{:02} {sign}{:02}{:02}",
        time.day(),
        time.year(),
        time.hour(),
        time.minute(),
        time.second(),
        minutes / 60,
        minutes % 60,
    )
}

/// `generateMessageID`: `<nanos.pid.random@hostname>`.
fn generate_message_id() -> Result<String, MimeError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id();
    let random =
        u64::from_le_bytes(random_bytes(8)?.try_into().expect("8 bytes")) & i64::MAX as u64;
    let hostname = hostname();
    Ok(format!("<{nanos}.{pid}.{random}@{hostname}>"))
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "localhost.localdomain".to_owned())
}

fn random_bytes(len: usize) -> Result<Vec<u8>, MimeError> {
    use std::io::Read;
    let mut bytes = vec![0u8; len];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|error| MimeError::Random(error.to_string()))?;
    Ok(bytes)
}

impl Email {
    /// `msgHeaders`: merge the typed fields and custom headers. The baseline
    /// looks for `Message-Id` (not `Message-ID`) in `Headers`; the exact-key
    /// copy at the end then adds a caller-supplied `Message-ID` as well, so a
    /// message can carry both headers. That behaviour is reproduced here.
    fn msg_headers(&self, runtime: &MimeRuntime) -> BTreeMap<String, Vec<String>> {
        let mut result = BTreeMap::new();
        for header in [
            "Reply-To",
            "To",
            "Cc",
            "From",
            "Subject",
            "Date",
            "Message-Id",
            "MIME-Version",
        ] {
            if let Some(values) = self.headers.get(header) {
                result.insert(header.to_owned(), values.clone());
            }
        }
        if !result.contains_key("Reply-To") && !self.reply_to.is_empty() {
            result.insert("Reply-To".to_owned(), vec![self.reply_to.join(", ")]);
        }
        if !result.contains_key("To") && !self.to.is_empty() {
            result.insert("To".to_owned(), vec![self.to.join(", ")]);
        }
        if !result.contains_key("Cc") && !self.cc.is_empty() {
            result.insert("Cc".to_owned(), vec![self.cc.join(", ")]);
        }
        if !result.contains_key("Subject") && !self.subject.is_empty() {
            result.insert("Subject".to_owned(), vec![self.subject.clone()]);
        }
        if !result.contains_key("Message-Id") {
            result.insert("Message-Id".to_owned(), vec![runtime.message_id.clone()]);
        }
        if !result.contains_key("From") {
            result.insert("From".to_owned(), vec![self.from.clone()]);
        }
        if !result.contains_key("Date") {
            result.insert("Date".to_owned(), vec![runtime.date.clone()]);
        }
        // textproto.MIMEHeader.Set canonicalizes the key, so the default is
        // stored as "Mime-Version" even though the baseline looks for
        // "MIME-Version" before setting it.
        if !result.contains_key("MIME-Version") {
            result.insert("Mime-Version".to_owned(), vec!["1.0".to_owned()]);
        }
        for (field, values) in &self.headers {
            if !result.contains_key(field) {
                result.insert(field.clone(), values.clone());
            }
        }
        result
    }

    /// `Email.Bytes()` for the no-attachment case.
    pub fn bytes(&self) -> Result<Vec<u8>, MimeError> {
        self.bytes_with(&MimeRuntime::from_environment()?)
    }

    /// `Email.Bytes()` with explicit `Date`/`Message-Id`/boundary values.
    pub fn bytes_with(&self, runtime: &MimeRuntime) -> Result<Vec<u8>, MimeError> {
        let mut headers = self.msg_headers(runtime);

        let is_alternative = !self.text.is_empty() && !self.html.is_empty();
        if is_alternative {
            headers.insert(
                "Content-Type".to_owned(),
                vec![format!(
                    "multipart/alternative;\r\n boundary={}",
                    runtime.boundary
                )],
            );
        } else if !self.html.is_empty() {
            headers.insert(
                "Content-Type".to_owned(),
                vec!["text/html; charset=UTF-8".to_owned()],
            );
            headers.insert(
                "Content-Transfer-Encoding".to_owned(),
                vec!["quoted-printable".to_owned()],
            );
        } else {
            headers.insert(
                "Content-Type".to_owned(),
                vec!["text/plain; charset=UTF-8".to_owned()],
            );
            headers.insert(
                "Content-Transfer-Encoding".to_owned(),
                vec!["quoted-printable".to_owned()],
            );
        }

        let mut out = Vec::new();
        write_headers(&mut out, &headers);
        out.extend_from_slice(b"\r\n");

        if !self.text.is_empty() || !self.html.is_empty() {
            if is_alternative {
                let mut first_part = true;
                if !self.text.is_empty() {
                    write_multipart_part(
                        &mut out,
                        &runtime.boundary,
                        &mut first_part,
                        "text/plain",
                        &self.text,
                    );
                }
                if !self.html.is_empty() {
                    write_multipart_part(
                        &mut out,
                        &runtime.boundary,
                        &mut first_part,
                        "text/html",
                        &self.html,
                    );
                }
                out.extend_from_slice(b"\r\n--");
                out.extend_from_slice(runtime.boundary.as_bytes());
                out.extend_from_slice(b"--\r\n");
            } else if !self.html.is_empty() {
                QuotedPrintableWriter::new(&self.html).finish_into(&mut out);
            } else {
                QuotedPrintableWriter::new(&self.text).finish_into(&mut out);
            }
        }
        Ok(out)
    }
}

/// `headerToBytes`.
fn write_headers(out: &mut Vec<u8>, headers: &BTreeMap<String, Vec<String>>) {
    for (field, values) in headers {
        for value in values {
            out.extend_from_slice(field.as_bytes());
            out.extend_from_slice(b": ");
            match field.as_str() {
                "Content-Type" | "Content-Disposition" => {
                    out.extend_from_slice(value.as_bytes());
                }
                "From" | "To" | "Cc" | "Bcc" => {
                    out.extend_from_slice(mail::format_address_list(value).as_bytes());
                }
                _ => {
                    out.extend_from_slice(mail::encode_header_value(value).as_bytes());
                }
            }
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// One `writeMessage` call with `multipart=true`: a quoted-printable body part
/// inside the `multipart/alternative` writer.
fn write_multipart_part(
    out: &mut Vec<u8>,
    boundary: &str,
    first_part: &mut bool,
    media_type: &str,
    body: &[u8],
) {
    if *first_part {
        *first_part = false;
        out.extend_from_slice(b"--");
    } else {
        out.extend_from_slice(b"\r\n--");
    }
    out.extend_from_slice(boundary.as_bytes());
    out.extend_from_slice(b"\r\n");
    // multipart.Writer.CreatePart sorts the header keys.
    out.extend_from_slice(b"Content-Transfer-Encoding: quoted-printable\r\n");
    out.extend_from_slice(format!("Content-Type: {media_type}; charset=UTF-8\r\n").as_bytes());
    out.extend_from_slice(b"\r\n");
    QuotedPrintableWriter::new(body).finish_into(out);
}

/// Byte-for-byte port of `mime/quotedprintable.Writer` with `Binary == false`.
struct QuotedPrintableWriter<'a> {
    input: &'a [u8],
    out: Vec<u8>,
    line: [u8; 78],
    len: usize,
    cr: bool,
}

impl<'a> QuotedPrintableWriter<'a> {
    const LINE_MAX_LEN: usize = 76;

    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            out: Vec::new(),
            line: [0; 78],
            len: 0,
            cr: false,
        }
    }

    fn finish_into(mut self, out: &mut Vec<u8>) {
        for &byte in self.input {
            if matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
                || (b'!'..=b'~').contains(&byte) && byte != b'='
            {
                self.write_byte(byte);
            } else {
                self.encode_byte(byte);
            }
        }
        self.check_last_byte();
        self.flush();
        out.extend_from_slice(&self.out);
    }

    fn write_byte(&mut self, byte: u8) {
        if byte == b'\n' || byte == b'\r' {
            if self.cr && byte == b'\n' {
                self.cr = false;
                return;
            }
            if byte == b'\r' {
                self.cr = true;
            }
            self.check_last_byte();
            self.insert_crlf();
            return;
        }
        if self.len == Self::LINE_MAX_LEN - 1 {
            self.insert_soft_line_break();
        }
        self.line[self.len] = byte;
        self.len += 1;
        self.cr = false;
    }

    fn encode_byte(&mut self, byte: u8) {
        if Self::LINE_MAX_LEN - 1 - self.len < 3 {
            self.insert_soft_line_break();
        }
        self.line[self.len] = b'=';
        self.line[self.len + 1] = upper_hex(byte >> 4);
        self.line[self.len + 2] = upper_hex(byte & 0x0f);
        self.len += 3;
    }

    fn check_last_byte(&mut self) {
        if self.len == 0 {
            return;
        }
        let last = self.line[self.len - 1];
        if last == b' ' || last == b'\t' {
            self.len -= 1;
            self.encode_byte(last);
        }
    }

    fn insert_soft_line_break(&mut self) {
        self.line[self.len] = b'=';
        self.len += 1;
        self.insert_crlf();
    }

    fn insert_crlf(&mut self) {
        self.line[self.len] = b'\r';
        self.line[self.len + 1] = b'\n';
        self.len += 2;
        self.flush();
    }

    fn flush(&mut self) {
        self.out.extend_from_slice(&self.line[..self.len]);
        self.len = 0;
    }
}

fn upper_hex(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0' + nibble,
        _ => b'A' + (nibble - 10),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn email(text: &str, html: &str) -> Email {
        Email {
            from: "\"Sourcegraph\" <noreply@example.com>".to_owned(),
            to: vec!["dest@example.com".to_owned()],
            subject: "Subject".to_owned(),
            text: text.as_bytes().to_vec(),
            html: html.as_bytes().to_vec(),
            ..Email::default()
        }
    }

    fn runtime() -> MimeRuntime {
        MimeRuntime::with_values(
            "Thu, 01 Jan 2026 00:00:00 +0000",
            "<1.2.3@host>",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
    }

    #[test]
    fn writes_alternative_parts() {
        let bytes = email("Hello\n", "<p>Hello</p>\n")
            .bytes_with(&runtime())
            .expect("bytes");
        let text = String::from_utf8(bytes).expect("ascii");
        assert!(text.contains("Content-Type: multipart/alternative;\r\n boundary="));
        assert!(text.contains("Content-Transfer-Encoding: quoted-printable\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\nHello\r\n"));
        assert!(text.contains("Content-Type: text/html; charset=UTF-8\r\n\r\n<p>Hello</p>\r\n"));
        assert!(text.ends_with(
            "\r\n--0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef--\r\n"
        ));
    }

    #[test]
    fn encodes_quoted_printable_lines() {
        let mut out = Vec::new();
        QuotedPrintableWriter::new(b"a b\t\r\nline with trailing space \nend\x00\xc3\xa9")
            .finish_into(&mut out);
        assert_eq!(
            String::from_utf8(out).expect("ascii"),
            "a b=09\r\nline with trailing space=20\r\nend=00=C3=A9"
        );
    }

    #[test]
    fn adds_both_message_id_headers() {
        let mut message = email("t\n", "h");
        message.headers.insert(
            "Message-ID".to_owned(),
            vec!["<mid@example.com>".to_owned()],
        );
        let text =
            String::from_utf8(message.bytes_with(&runtime()).expect("bytes")).expect("ascii");
        assert!(text.contains("Message-ID: <mid@example.com>\r\n"));
        assert!(text.contains("Message-Id: <1.2.3@host>\r\n"));
    }

    #[test]
    fn formats_rfc1123z_with_offset() {
        use time::{OffsetDateTime, UtcOffset};
        let utc = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        assert_eq!(rfc1123z(utc), "Tue, 14 Nov 2023 22:13:20 +0000");
        let shifted = utc.to_offset(UtcOffset::from_hms(-5, 0, 0).expect("offset"));
        assert_eq!(rfc1123z(shifted), "Tue, 14 Nov 2023 17:13:20 -0500");
    }

    #[test]
    fn generates_message_id_like_the_baseline() {
        let id = generate_message_id().expect("message id");
        let inner = id
            .strip_prefix('<')
            .and_then(|rest| rest.strip_suffix('>'))
            .expect("angle brackets");
        let (left, host) = inner.rsplit_once('@').expect("host");
        let parts: Vec<&str> = left.split('.').collect();
        assert_eq!(parts.len(), 3, "{id}");
        assert!(
            parts
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())),
            "{id}"
        );
        assert!(!host.is_empty(), "{id}");
    }

    #[test]
    fn emits_address_headers_through_net_mail() {
        let mut message = email("t\n", "h");
        message.reply_to = vec!["reply@example.com".to_owned()];
        let text =
            String::from_utf8(message.bytes_with(&runtime()).expect("bytes")).expect("ascii");
        assert!(text.contains("To: <dest@example.com>\r\n"));
        assert!(text.contains("Reply-To: reply@example.com\r\n"));
    }
}
