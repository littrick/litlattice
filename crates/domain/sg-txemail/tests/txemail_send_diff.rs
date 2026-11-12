//! M9.20a differential harness: `txemail.Send` end to end.
//!
//! Mirrors the untracked Go oracle
//! `cmd/frontend/internal/auth/userpasswd/zz_m9_20a_mail_oracle_test.go`.
//! Each corpus case configures the baseline's e-mail settings and a message,
//! optionally starts a scripted in-process SMTP server (identical state
//! machine on both sides), drives the real `txemail.Send` /
//! `sg_txemail::send`, and prints one `txsend ` JSON line:
//!
//! * `case` — corpus case name
//! * `error` — the client error text (empty on success)
//! * `transcript` — `C `/`S `/`D ` lines observed by the server
//!
//! The DATA block is canonicalized with the M9.8cp transform: multipart
//! boundaries, `Date` and the generated `Message-Id` become placeholders and
//! top-level headers are unfolded and sorted. Everything else is compared
//! byte for byte, so the templates and the SMTP dialogue must match exactly.
//!
//! Gated by `M920A_CORPUS`; `run_txemail_send_diff.sh` sets it.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sg_txemail::send::{Header, SendConfig, SmtpConfig};
use sg_txemail::{Message, Templates};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    config: Config,
    #[serde(default)]
    server: Option<Server>,
    message: MessageSpec,
    #[serde(default = "default_true")]
    canonicalize: bool,
}

#[derive(Debug, Default, Deserialize)]
struct Config {
    #[serde(default)]
    email_address: String,
    #[serde(default)]
    sender_name: String,
    #[serde(default)]
    smtp: Option<SmtpSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct SmtpSpec {
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    authentication: String,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    no_verify_tls: bool,
    #[serde(default)]
    additional_headers: Vec<HeaderSpec>,
}

#[derive(Debug, Deserialize)]
struct HeaderSpec {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct MessageSpec {
    #[serde(default)]
    to: Vec<String>,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    references: Vec<String>,
    template: TemplateSpec,
    #[serde(default = "empty_object")]
    data: Value,
}

#[derive(Debug, Deserialize)]
struct TemplateSpec {
    subject: String,
    #[serde(default)]
    text: String,
    html: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Server {
    greeting: String,
    #[serde(default)]
    ehlo: Vec<String>,
    #[serde(default)]
    helo: String,
    #[serde(default)]
    mail: String,
    #[serde(default)]
    rcpt: Vec<String>,
    #[serde(default)]
    data: String,
    #[serde(default)]
    data_done: String,
    #[serde(default)]
    quit: String,
    #[serde(default)]
    auth_challenge: Option<String>,
    #[serde(default)]
    auth_ok: String,
}

#[derive(Debug, Serialize)]
struct Row {
    case: String,
    error: String,
    transcript: Vec<String>,
}

fn default_true() -> bool {
    true
}

fn empty_object() -> Value {
    serde_json::json!({})
}

fn main_message(spec: &MessageSpec) -> Message {
    Message {
        to: spec.to.clone(),
        reply_to: spec.reply_to.clone(),
        message_id: spec.message_id.clone(),
        references: spec.references.clone(),
        template: Templates {
            subject: spec.template.subject.clone(),
            text: spec.template.text.clone(),
            html: spec.template.html.clone(),
        },
        data: spec.data.clone(),
    }
}

fn send_config(spec: &Config, port: Option<u16>) -> SendConfig {
    SendConfig {
        email_address: spec.email_address.clone(),
        sender_name: spec.sender_name.clone(),
        smtp: spec.smtp.as_ref().map(|smtp| SmtpConfig {
            // Both harness sides always point the client at loopback; the
            // corpus only varies the scripted server responses.
            host: "127.0.0.1".to_owned(),
            port: port.unwrap_or(0),
            username: smtp.username.clone(),
            password: smtp.password.clone(),
            authentication: smtp.authentication.clone(),
            domain: smtp.domain.clone(),
            no_verify_tls: smtp.no_verify_tls,
            additional_headers: smtp
                .additional_headers
                .iter()
                .map(|header| Header {
                    key: header.key.clone(),
                    value: header.value.clone(),
                })
                .collect(),
        }),
    }
}

fn run_case(case: &Case) {
    let (error, transcript) = match &case.server {
        Some(script) => {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind harness server");
            let port = listener.local_addr().expect("local addr").port();
            let transcript = Arc::new(Mutex::new(Vec::new()));
            let server_transcript = Arc::clone(&transcript);
            let script = script.clone();
            let handle =
                std::thread::spawn(move || run_server(listener, &script, server_transcript));

            let config = send_config(&case.config, Some(port));
            let message = main_message(&case.message);
            let result = sg_txemail::send(&config, &message).map_err(|error| error.to_string());
            handle.join().expect("harness server thread");

            let mut entries = transcript.lock().expect("transcript lock").clone();
            if case.canonicalize {
                entries = canonicalize_data_block(entries);
            }
            (result.err().unwrap_or_default(), entries)
        }
        None => {
            let config = send_config(&case.config, None);
            let message = main_message(&case.message);
            let result = sg_txemail::send(&config, &message).map_err(|error| error.to_string());
            (result.err().unwrap_or_default(), Vec::new())
        }
    };

    let row = Row {
        case: case.name.clone(),
        error,
        transcript,
    };
    println!("txsend {}", serde_json::to_string(&row).expect("json row"));
}

#[test]
fn txemail_send_diff() {
    let Ok(path) = std::env::var("M920A_CORPUS") else {
        eprintln!("M920A_CORPUS not set; skipping txemail send differential");
        return;
    };
    let data = std::fs::read_to_string(&path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&data).expect("parse corpus");
    for case in &cases {
        run_case(case);
    }
}

// ---- scripted server ----

fn run_server(listener: TcpListener, script: &Server, transcript: Arc<Mutex<Vec<String>>>) {
    let Ok((tcp, _)) = listener.accept() else {
        return;
    };
    let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(10)));
    let mut conn = tcp;
    write_line(&mut conn, &transcript, &script.greeting);

    let mut rcpt_count = 0usize;
    while let Some(line) = read_line(&mut conn) {
        transcript.lock().expect("lock").push(format!("C {line}"));
        let verb = line
            .split(' ')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        match verb.as_str() {
            "EHLO" => {
                for response in &script.ehlo {
                    write_line(&mut conn, &transcript, response);
                }
            }
            "HELO" => write_line(&mut conn, &transcript, &script.helo),
            "AUTH" => {
                if let Some(challenge) = &script.auth_challenge {
                    write_line(&mut conn, &transcript, &format!("334 {challenge}"));
                    let Some(response) = read_line(&mut conn) else {
                        break;
                    };
                    transcript
                        .lock()
                        .expect("lock")
                        .push(format!("C {response}"));
                }
                write_line(&mut conn, &transcript, &script.auth_ok);
            }
            "MAIL" => write_line(&mut conn, &transcript, &script.mail),
            "RCPT" => {
                let index = rcpt_count.min(script.rcpt.len().saturating_sub(1));
                rcpt_count += 1;
                let response = script.rcpt.get(index).cloned().unwrap_or_default();
                write_line(&mut conn, &transcript, &response);
            }
            "DATA" => {
                write_line(&mut conn, &transcript, &script.data);
                loop {
                    let Some(data_line) = read_line(&mut conn) else {
                        return;
                    };
                    if data_line == "." {
                        break;
                    }
                    transcript
                        .lock()
                        .expect("lock")
                        .push(format!("D {data_line}"));
                }
                write_line(&mut conn, &transcript, &script.data_done);
            }
            "QUIT" => {
                write_line(&mut conn, &transcript, &script.quit);
                break;
            }
            "*" => write_line(&mut conn, &transcript, "501 5.5.4 Syntax error"),
            _ => write_line(
                &mut conn,
                &transcript,
                "500 5.5.1 Error: command not recognized",
            ),
        }
    }
}

fn read_line(conn: &mut TcpStream) -> Option<String> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match conn.read(&mut byte) {
            Ok(0) | Err(_) => return None,
            Ok(_) => {}
        }
        if byte[0] == b'\n' {
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
            return String::from_utf8(bytes).ok();
        }
        bytes.push(byte[0]);
    }
}

fn write_line(conn: &mut TcpStream, transcript: &Mutex<Vec<String>>, line: &str) {
    let mut bytes = line.as_bytes().to_vec();
    bytes.extend_from_slice(b"\r\n");
    conn.write_all(&bytes).expect("server write");
    conn.flush().expect("server flush");
    transcript.lock().expect("lock").push(format!("S {line}"));
}

// ---- M9.8cp canonicalization (boundaries, Date, Message-Id, header order) ----

fn canonicalize_data_block(entries: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(entries.len());
    let mut index = 0;
    while index < entries.len() {
        let Some(first) = entries[index].strip_prefix("D ") else {
            out.push(entries[index].clone());
            index += 1;
            continue;
        };
        let mut lines = vec![first.to_owned()];
        let mut end = index + 1;
        while end < entries.len() {
            let Some(line) = entries[end].strip_prefix("D ") else {
                break;
            };
            lines.push(line.to_owned());
            end += 1;
        }
        let raw = lines.join("\r\n");
        for line in canonicalize_mime(&raw).split("\r\n") {
            out.push(format!("D {line}"));
        }
        index = end;
    }
    out
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn replace_boundaries(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if !is_lower_hex(bytes[index]) {
            out.push(bytes[index] as char);
            index += 1;
            continue;
        }
        let mut end = index;
        while end < bytes.len() && is_lower_hex(bytes[end]) {
            end += 1;
        }
        let run = &input[index..end];
        if run.len() == 60 {
            let placeholder = match seen.iter().find(|(token, _)| token == run) {
                Some((_, placeholder)) => placeholder.clone(),
                None => {
                    let placeholder = format!("BOUNDARY{}", seen.len());
                    seen.push((run.to_owned(), placeholder.clone()));
                    placeholder
                }
            };
            out.push_str(&placeholder);
        } else {
            out.push_str(run);
        }
        index = end;
    }
    out
}

fn looks_like_rfc1123z(value: &str) -> bool {
    let parts: Vec<&str> = value.split(' ').collect();
    if parts.len() != 6 {
        return false;
    }
    let (weekday, day, month, year, clock, zone) =
        (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
    weekday.len() == 4
        && weekday.ends_with(',')
        && day.len() == 2
        && day.bytes().all(|byte| byte.is_ascii_digit())
        && month.len() == 3
        && year.len() == 4
        && year.bytes().all(|byte| byte.is_ascii_digit())
        && clock.len() == 8
        && clock.as_bytes()[2] == b':'
        && clock.as_bytes()[5] == b':'
        && clock
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 2 || index == 5 || byte.is_ascii_digit())
        && zone.len() == 5
        && (zone.starts_with('+') || zone.starts_with('-'))
        && zone[1..].bytes().all(|byte| byte.is_ascii_digit())
}

fn looks_like_generated_message_id(value: &str) -> bool {
    let Some(inner) = value
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
    else {
        return false;
    };
    let Some(at) = inner.rfind('@') else {
        return false;
    };
    let parts: Vec<&str> = inner[..at].split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn canonicalize_mime(raw: &str) -> String {
    let replaced = replace_boundaries(raw);
    let Some(split) = replaced.find("\r\n\r\n") else {
        return replaced;
    };
    let header_block = &replaced[..split];
    let body = &replaced[split + 4..];

    let mut merged: Vec<String> = Vec::new();
    for line in header_block.split("\r\n") {
        let starts_folded = line.starts_with(' ') || line.starts_with('\t');
        if starts_folded && !merged.is_empty() {
            let last = merged.last_mut().expect("non-empty");
            last.push(' ');
            last.push_str(line.trim_start_matches([' ', '\t']));
            continue;
        }
        merged.push(line.to_owned());
    }
    for line in &mut merged {
        if let Some(value) = line.strip_prefix("Date: ") {
            if looks_like_rfc1123z(value) {
                *line = "Date: DATE".to_owned();
                continue;
            }
        }
        if let Some(value) = line.strip_prefix("Message-Id: ") {
            if looks_like_generated_message_id(value) {
                *line = "Message-Id: MESSAGEID".to_owned();
            }
        }
    }
    merged.sort();
    format!("{}\r\n\r\n{}", merged.join("\r\n"), body)
}
