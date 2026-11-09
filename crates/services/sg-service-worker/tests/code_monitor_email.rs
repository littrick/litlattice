//! M9.8cs differential harness: the code monitor e-mail transport
//! (`internal/codemonitors/background/email.go`: `sendEmail`).
//!
//! Mirrors `internal/codemonitors/background/cm_email_harness_test.go`. Both
//! sides seed the same users / `user_emails` rows in the scratch frontend
//! database, point the live site configuration at a scripted SMTP server, and
//! drive the real transport (`SendEmailForNewSearchResult` / [`TxEmailSender`]).
//!
//! Successful cases print the server-observed MIME message, canonicalized like
//! the M9.8cq harness (boundaries, `Date`, generated `Message-Id`, unfolded and
//! sorted headers), so the rendered templates are compared byte for byte.
//! Failure cases print the baseline error string.
//!
//! Only runs when `SG_CM_EMAIL_TEST_DSN` names the scratch database. Prints
//! canonical `cmemail ` lines.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use sg_config::{LiveConfig, SiteConfiguration, Unified};
use sg_service_worker::jobs::code_monitor_action_runner::EmailSender;
use sg_service_worker::jobs::code_monitor_actions::{DisplayResult, TemplateDataNewSearchResults};
use sg_service_worker::jobs::code_monitor_email::TxEmailSender;
use sqlx::PgPool;

#[derive(Serialize)]
struct Line {
    case: String,
    error: String,
    message: String,
}

fn emit(case: &str, error: Option<String>, message: String) {
    let line = Line {
        case: case.to_owned(),
        error: error.unwrap_or_else(|| "-".to_owned()),
        message,
    };
    println!(
        "cmemail {}",
        serde_json::to_string(&line).expect("serialize harness line")
    );
}

/// The canonical data used by the success cases; the Go harness builds the
/// same values.
fn main_data() -> TemplateDataNewSearchResults {
    TemplateDataNewSearchResults {
        priority: "[Critical] ".to_owned(),
        code_monitor_url: "https://sourcegraph.example.com/code-monitoring/Q29kZU1vbml0b3I6MQ==?utm_source=code-monitoring-email".to_owned(),
        search_url: "https://sourcegraph.example.com/search?q=type%3Acommit+after%3A2020-01-01&utm_source=code-monitoring-email".to_owned(),
        description: "my monitor".to_owned(),
        include_results: true,
        truncated_results: vec![
            DisplayResult {
                result_type: "Message".to_owned(),
                commit_url: "https://sourcegraph.example.com/github.com/acme/alpha/-/commit/a1b2c3d?utm_source=code-monitoring-email".to_owned(),
                repo_name: "github.com/acme/alpha".to_owned(),
                commit_id: "a1b2c3d".to_owned(),
                content: "crash <here> & now\nsecond line".to_owned(),
            },
            DisplayResult {
                result_type: "Diff".to_owned(),
                commit_url: "https://sourcegraph.example.com/github.com/acme/beta/-/commit/b1b2c3d?utm_source=code-monitoring-email".to_owned(),
                repo_name: "github.com/acme/beta".to_owned(),
                commit_id: "b1b2c3d".to_owned(),
                content: "  -panic()\n  +recover()".to_owned(),
            },
        ],
        total_count: 7,
        truncated_count: 2,
        result_pluralized: "results".to_owned(),
        truncated_result_pluralized: "results".to_owned(),
        display_more_link: true,
        is_test: false,
    }
}

fn no_results_data() -> TemplateDataNewSearchResults {
    TemplateDataNewSearchResults {
        priority: String::new(),
        code_monitor_url: "https://sourcegraph.example.com/code-monitoring/Q29kZU1vbml0b3I6Mg==?utm_source=code-monitoring-email".to_owned(),
        search_url: "https://sourcegraph.example.com/search?q=type%3Adiff&utm_source=code-monitoring-email".to_owned(),
        description: "quiet monitor".to_owned(),
        include_results: false,
        truncated_results: Vec::new(),
        total_count: 0,
        truncated_count: 0,
        result_pluralized: "results".to_owned(),
        truncated_result_pluralized: "results".to_owned(),
        display_more_link: false,
        is_test: false,
    }
}

fn test_preview_data() -> TemplateDataNewSearchResults {
    TemplateDataNewSearchResults {
        priority: String::new(),
        code_monitor_url: String::new(),
        search_url: String::new(),
        description: "preview monitor".to_owned(),
        include_results: true,
        truncated_results: vec![DisplayResult {
            result_type: "Test".to_owned(),
            commit_url: String::new(),
            repo_name: "testorg/testrepo".to_owned(),
            commit_id: "0000000".to_owned(),
            content: "This is a test\nfor a code monitoring result.".to_owned(),
        }],
        total_count: 1,
        truncated_count: 0,
        result_pluralized: "result".to_owned(),
        truncated_result_pluralized: "results".to_owned(),
        display_more_link: false,
        is_test: true,
    }
}

struct Capture {
    lines: Arc<Mutex<Vec<String>>>,
    done: mpsc::Receiver<()>,
}

impl Capture {
    fn wait(self) -> Vec<String> {
        let _ = self.done.recv_timeout(Duration::from_secs(10));
        self.lines.lock().expect("capture lock").clone()
    }
}

/// A minimal SMTP server: greeting, EHLO/HELO, MAIL, RCPT (scripted), DATA
/// capture and QUIT. No STARTTLS and no AUTH, which is what the harness
/// configuration (`authentication: none`, no TLS extension) selects.
fn smtp_server(rcpt_responses: &'static [&'static str]) -> (u16, Capture) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind SMTP harness");
    let port = listener.local_addr().expect("local addr").port();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let server_lines = Arc::clone(&lines);
    let (done_tx, done) = mpsc::channel();
    std::thread::spawn(move || {
        if let Ok((mut tcp, _)) = listener.accept() {
            let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
            let _ = tcp.set_write_timeout(Some(Duration::from_secs(10)));
            serve(&mut tcp, rcpt_responses, &server_lines);
        }
        let _ = done_tx.send(());
    });
    (port, Capture { lines, done })
}

fn serve(tcp: &mut TcpStream, rcpt_responses: &[&str], lines: &Mutex<Vec<String>>) {
    write_line(tcp, "220 test ESMTP");
    let mut rcpt_index = 0usize;
    while let Some(line) = read_line(tcp) {
        let verb = line
            .split(' ')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        match verb.as_str() {
            "EHLO" => {
                write_line(tcp, "250-test");
                write_line(tcp, "250 8BITMIME");
            }
            "HELO" => write_line(tcp, "250 test"),
            "MAIL" => write_line(tcp, "250 2.1.0 Ok"),
            "RCPT" => {
                let index = rcpt_index.min(rcpt_responses.len().saturating_sub(1));
                rcpt_index += 1;
                write_line(tcp, rcpt_responses[index]);
            }
            "DATA" => {
                write_line(tcp, "354 End data with <CR><LF>.<CR><LF>");
                loop {
                    let Some(data_line) = read_line(tcp) else {
                        return;
                    };
                    if data_line == "." {
                        break;
                    }
                    lines.lock().expect("capture lock").push(data_line);
                }
                write_line(tcp, "250 2.0.0 Ok: queued as 12345");
            }
            "QUIT" => {
                write_line(tcp, "221 2.0.0 Bye");
                return;
            }
            _ => write_line(tcp, "500 5.5.1 Error: command not recognized"),
        }
    }
}

fn write_line(tcp: &mut TcpStream, line: &str) {
    let _ = tcp.write_all(format!("{line}\r\n").as_bytes());
    let _ = tcp.flush();
}

fn read_line(tcp: &mut TcpStream) -> Option<String> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match tcp.read(&mut byte) {
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

fn site_config(email_address: &str, port: u16) -> SiteConfiguration {
    SiteConfiguration::parse(&format!(
        r#"{{
            "email.address": {email_address:?},
            "email.senderName": "Sourcegraph Test",
            "email.smtp": {{
                "host": "127.0.0.1",
                "port": {port},
                "authentication": "none",
                "additionalHeaders": [{{"key": "X-Code-Monitor", "value": "test"}}]
            }}
        }}"#
    ))
    .expect("parse site configuration")
}

fn sender(pool: &PgPool, site: SiteConfiguration) -> TxEmailSender {
    let unified = Unified {
        site,
        ..Unified::default()
    };
    TxEmailSender::new(pool.clone(), Arc::new(LiveConfig::new(unified)))
}

async fn seed(pool: &PgPool) {
    sqlx::query("TRUNCATE TABLE users RESTART IDENTITY CASCADE")
        .execute(pool)
        .await
        .expect("truncate users");
    sqlx::query(
        "INSERT INTO users (id, username, display_name) VALUES \
         (20, 'verified', 'Verified'), (21, 'unverified', 'Unverified'), \
         (22, 'no-primary', 'No Primary')",
    )
    .execute(pool)
    .await
    .expect("insert users");
    sqlx::query(
        "INSERT INTO user_emails (user_id, email, is_primary, verified_at) VALUES \
         (20, 'verified@example.com', TRUE, now()), \
         (21, 'unverified@example.com', TRUE, NULL), \
         (22, 'secondary@example.com', FALSE, now())",
    )
    .execute(pool)
    .await
    .expect("insert user_emails");
}

const BASIC_RCPT: &[&str] = &["250 2.1.5 Ok"];
const REJECTING_RCPT: &[&str] = &["550 5.1.1 <verified@example.com>: Recipient address rejected"];

#[tokio::test]
async fn code_monitor_email_harness() {
    let Ok(dsn) = std::env::var("SG_CM_EMAIL_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2).await.expect("connect");
    seed(&pool).await;

    // Success cases: the server captures the rendered MIME message.
    for (case, data) in [
        ("verified-main", main_data()),
        ("verified-no-results", no_results_data()),
        ("verified-test-preview", test_preview_data()),
    ] {
        let (port, capture) = smtp_server(BASIC_RCPT);
        let result = sender(&pool, site_config("noreply@example.com", port))
            .send(20, &data)
            .await;
        let lines = capture.wait();
        emit(case, result.err(), canonicalize_mime(&lines.join("\r\n")));
    }

    // Database checks: unverified primary, no primary, unknown user.
    for (case, user_id) in [
        ("unverified", 21),
        ("no-primary-email", 22),
        ("unknown-user", 23),
    ] {
        let result = sender(&pool, site_config("noreply@example.com", 2525))
            .send(user_id, &main_data())
            .await;
        emit(case, result.err(), String::new());
    }

    // Transport configuration errors: no `email.smtp`, no `email.address`.
    let no_smtp =
        SiteConfiguration::parse(r#"{"email.address": "noreply@example.com"}"#).expect("parse");
    let result = sender(&pool, no_smtp).send(20, &main_data()).await;
    emit("no-smtp-config", result.err(), String::new());

    let no_from = SiteConfiguration::parse(
        r#"{"email.smtp": {"host": "127.0.0.1", "port": 2525, "authentication": "none"}}"#,
    )
    .expect("parse");
    let result = sender(&pool, no_from).send(20, &main_data()).await;
    emit("no-from-address", result.err(), String::new());

    // Delivery error: the server rejects the recipient.
    let (port, capture) = smtp_server(REJECTING_RCPT);
    let result = sender(&pool, site_config("noreply@example.com", port))
        .send(20, &main_data())
        .await;
    let lines = capture.wait();
    emit(
        "smtp-rcpt-550",
        result.err(),
        canonicalize_mime(&lines.join("\r\n")),
    );
}

// ---- MIME canonicalization (same transform as the M9.8cq harness) ----

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

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
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
