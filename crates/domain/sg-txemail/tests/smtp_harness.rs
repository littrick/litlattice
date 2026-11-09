//! M9.8cq differential harness: the SMTP transport of `txemail.Send`.
//!
//! Mirrors `internal/txemail/txemail_smtp_harness_test.go`. Each case runs a
//! scripted SMTP server (in-process, identical logic on both sides), drives the
//! real client / `send` against it, and prints one `txsmtp ` JSON line with
//! the client error and the server-observed transcript:
//!
//! * `C <line>` — a client command line (or an `AUTH` continuation).
//! * `S <line>` — a server response line.
//! * `D <line>` — a `DATA` line, recorded exactly as received, so dot-stuffing
//!   and `\n` -> `\r\n` translation are visible.
//!
//! `send-*` cases serialize the real MIME message, so their `D` block is
//! canonicalized with the same transform as the M9.8cp harness (boundaries,
//! `Date`, generated `Message-Id`, unfolded/sorted headers).
//!
//! The STARTTLS cases use a fixed self-signed certificate
//! (`smtp_harness_cert.pem` / `smtp_harness_key.pem`, SAN `localhost` and
//! `127.0.0.1`); TLS handshake bytes are not part of the transcript.
//!
//! Gated by `SG_TXEMAIL_SMTP_HARNESS_ON=1`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConnection, StreamOwned};

use sg_txemail::send::{prepare_email, send, Header, SendConfig, SmtpConfig};
use sg_txemail::smtp::{Auth, Client};
use sg_txemail::{Message, Templates};

const CERT_PEM: &str = include_str!("smtp_harness_cert.pem");
const KEY_PEM: &str = include_str!("smtp_harness_key.pem");

type HarnessResult = Result<(), String>;

/// `try_!(expr)` maps an error to its `Display` text and propagates.
macro_rules! try_ {
    ($expr:expr) => {
        $expr.map_err(|error| error.to_string())?
    };
}

#[derive(Clone, Copy)]
struct Script {
    greeting: &'static str,
    ehlo: &'static [&'static str],
    ehlo_tls: &'static [&'static str],
    helo: &'static str,
    mail: &'static str,
    rcpt: &'static [&'static str],
    data: &'static str,
    data_done: &'static str,
    quit: &'static str,
    auth_challenge: Option<&'static str>,
    auth_ok: &'static str,
}

const BASIC: Script = Script {
    greeting: "220 test ESMTP",
    ehlo: &["250-test", "250-8BITMIME", "250 SMTPUTF8"],
    ehlo_tls: &["250-test"],
    helo: "250 test",
    mail: "250 2.1.0 Ok",
    rcpt: &["250 2.1.5 Ok"],
    data: "354 End data with <CR><LF>.<CR><LF>",
    data_done: "250 2.0.0 Ok: queued as 12345",
    quit: "221 2.0.0 Bye",
    auth_challenge: None,
    auth_ok: "235 2.7.0 Authentication successful",
};

fn script_with(ehlo: &'static [&'static str]) -> Script {
    Script { ehlo, ..BASIC }
}

#[test]
fn smtp_harness() {
    if std::env::var("SG_TXEMAIL_SMTP_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }

    // ---- client-level cases: net/smtp API surface ----

    run_case(
        "ehlo-basic",
        BASIC,
        |port| {
            let mut client = connect(port, "localhost")?;
            try_!(client.hello("localhost"));
            try_!(client.mail("from@example.test"));
            try_!(client.rcpt("to@example.test"));
            let mut writer = try_!(client.data());
            try_!(writer.write_all(b"Subject: test\r\n\r\nbody\r\n"));
            try_!(writer.finish());
            try_!(client.quit());
            Ok(())
        },
        false,
    );

    run_case(
        "helo-fallback",
        script_with(&["500 5.5.1 Command not recognized"]),
        |port| {
            let mut client = connect(port, "localhost")?;
            try_!(client.hello("localhost"));
            try_!(client.mail("from@example.test"));
            try_!(client.quit());
            Ok(())
        },
        false,
    );

    run_case(
        "rcpt-550",
        Script {
            rcpt: &["550 5.1.1 <nobody@example.test>: Recipient address rejected"],
            ..BASIC
        },
        |port| {
            let mut client = connect(port, "localhost")?;
            try_!(client.hello("localhost"));
            try_!(client.mail("from@example.test"));
            client
                .rcpt("nobody@example.test")
                .map_err(|error| error.to_string())
        },
        false,
    );

    run_case(
        "auth-plain",
        script_with(&["250-test", "250-AUTH PLAIN LOGIN", "250 SIZE 10485760"]),
        |port| {
            let mut client = connect(port, "localhost")?;
            try_!(client.hello("localhost"));
            try_!(client.auth(&Auth::plain("", "user", "pass", "localhost")));
            try_!(client.quit());
            Ok(())
        },
        false,
    );

    run_case(
        "auth-cram",
        Script {
            ehlo: &["250-test", "250 AUTH CRAM-MD5"],
            auth_challenge: Some("PDE4OTYuNjk3MTcwOTUyQHBvc3RvZmZpY2UucmVzdG9uLm1jaS5uZXQ+"),
            ..BASIC
        },
        |port| {
            let mut client = connect(port, "localhost")?;
            try_!(client.hello("localhost"));
            try_!(client.auth(&Auth::cram_md5("tim", "tanstaaftanstaaf")));
            try_!(client.quit());
            Ok(())
        },
        false,
    );

    run_case(
        "auth-plain-unencrypted",
        script_with(&["250-test", "250 AUTH PLAIN"]),
        |port| {
            let mut client = connect(port, "mail.example.test")?;
            try_!(client.hello("localhost"));
            client
                .auth(&Auth::plain("", "user", "pass", "mail.example.test"))
                .map_err(|error| error.to_string())
        },
        false,
    );

    run_case(
        "starttls",
        Script {
            ehlo: &["250-test", "250-STARTTLS", "250 AUTH PLAIN"],
            ehlo_tls: &["250-test", "250-AUTH PLAIN LOGIN", "250 SIZE 10485760"],
            ..BASIC
        },
        |port| {
            let mut client = connect(port, "localhost")?;
            try_!(client.hello("localhost"));
            assert!(client.extension("STARTTLS").0);
            try_!(client.start_tls(true));
            try_!(client.quit());
            Ok(())
        },
        false,
    );

    run_case(
        "data-dot-stuffing",
        BASIC,
        |port| {
            let mut client = connect(port, "localhost")?;
            try_!(client.hello("localhost"));
            try_!(client.mail("from@example.test"));
            try_!(client.rcpt("to@example.test"));
            let mut writer = try_!(client.data());
            try_!(writer.write_all(
                b"Subject: dot\r\n\r\n.leading\r\n..double\r\nmix\nlf\r\nends with cr\r"
            ));
            try_!(writer.finish());
            try_!(client.quit());
            Ok(())
        },
        false,
    );

    // ---- Send-level cases: txemail.Send end to end ----

    run_case(
        "send-basic",
        BASIC,
        |port| send_case(port, "none", false, Vec::new()),
        true,
    );

    run_case(
        "send-additional-headers",
        BASIC,
        |port| {
            send_case(
                port,
                "none",
                false,
                vec![
                    Header {
                        key: "x-custom".to_owned(),
                        value: "one".to_owned(),
                    },
                    Header {
                        key: "X-Other".to_owned(),
                        value: "two".to_owned(),
                    },
                ],
            )
        },
        true,
    );

    run_case(
        "send-bad-auth",
        BASIC,
        |port| send_case(port, "LOGIN", false, Vec::new()),
        false,
    );

    run_case(
        "send-auth-plain",
        script_with(&[
            "250-test",
            "250-8BITMIME",
            "250-AUTH PLAIN LOGIN",
            "250 SIZE 10485760",
        ]),
        |port| send_case(port, "PLAIN", false, Vec::new()),
        true,
    );

    run_case(
        "send-auth-cram",
        Script {
            ehlo: &["250-test", "250 AUTH CRAM-MD5"],
            auth_challenge: Some("PDE4OTYuNjk3MTcwOTUyQHBvc3RvZmZpY2UucmVzdG9uLm1jaS5uZXQ+"),
            ..BASIC
        },
        |port| send_case(port, "CRAM-MD5", false, Vec::new()),
        true,
    );

    run_case(
        "send-starttls",
        Script {
            ehlo: &["250-test", "250-STARTTLS", "250 AUTH PLAIN"],
            ehlo_tls: &["250-test", "250-AUTH PLAIN LOGIN", "250 SIZE 10485760"],
            ..BASIC
        },
        |port| send_case(port, "PLAIN", true, Vec::new()),
        true,
    );

    run_case(
        "send-rcpt-error",
        Script {
            rcpt: &[
                "250 2.1.5 Ok",
                "550 5.1.1 <two@example.test>: Recipient address rejected",
            ],
            ..BASIC
        },
        |port| send_case(port, "none", false, Vec::new()),
        false,
    );

    run_case(
        "send-mail-error",
        Script {
            mail: "550 5.1.8 Sender address rejected",
            ..BASIC
        },
        |port| send_case(port, "none", false, Vec::new()),
        false,
    );

    run_case(
        "send-quit-error",
        Script {
            quit: "500 5.5.1 QUIT not welcome",
            ..BASIC
        },
        |port| send_case(port, "none", false, Vec::new()),
        true,
    );

    // ---- configuration-only cases (no connection) ----

    emit_simple(
        "send-no-address",
        send(&SendConfig::default(), &message()),
        "",
        Vec::new(),
    );

    emit_simple(
        "send-no-smtp",
        send(
            &SendConfig {
                email_address: "noreply@example.com".to_owned(),
                sender_name: String::new(),
                smtp: None,
            },
            &message(),
        ),
        "",
        Vec::new(),
    );

    // Mandrill header overrides are checked at the prepare level: the branch is
    // keyed on `smtp.mandrillapp.com`, which the harness cannot dial locally.
    let mandrill = SendConfig {
        email_address: "noreply@example.com".to_owned(),
        sender_name: String::new(),
        smtp: Some(SmtpConfig {
            host: "smtp.mandrillapp.com".to_owned(),
            port: 587,
            ..SmtpConfig::default()
        }),
    };
    let mandrill_message = match prepare_email(&mandrill, &message()) {
        Ok(email) => match email.bytes() {
            Ok(raw) => canonicalize_mime(&String::from_utf8_lossy(&raw)),
            Err(error) => format!("get bytes: {error}"),
        },
        Err(error) => error.to_string(),
    };
    emit_simple(
        "send-mandrill-headers",
        Ok(()),
        &mandrill_message,
        Vec::new(),
    );
}

fn message() -> Message {
    Message {
        to: vec!["one@example.test".to_owned(), "two@example.test".to_owned()],
        reply_to: None,
        message_id: None,
        references: Vec::new(),
        template: Templates {
            subject: "Harness subject".to_owned(),
            text: "Text body\n".to_owned(),
            html: "<p>HTML body</p>\n".to_owned(),
        },
        data: serde_json::json!({}),
    }
}

fn send_case(
    port: u16,
    authentication: &str,
    no_verify_tls: bool,
    additional_headers: Vec<Header>,
) -> HarnessResult {
    let config = SendConfig {
        email_address: "noreply@example.com".to_owned(),
        sender_name: String::new(),
        smtp: Some(SmtpConfig {
            host: "127.0.0.1".to_owned(),
            port,
            username: "user".to_owned(),
            password: "pass".to_owned(),
            authentication: authentication.to_owned(),
            domain: "helo.example.test".to_owned(),
            no_verify_tls,
            additional_headers,
        }),
    };
    send(&config, &message()).map_err(|error| error.to_string())
}

fn connect(port: u16, server_name: &str) -> Result<Client, String> {
    let tcp = TcpStream::connect(("127.0.0.1", port)).map_err(|error| error.to_string())?;
    Client::new(tcp, server_name).map_err(|error| error.to_string())
}

fn run_case(
    name: &str,
    script: Script,
    run: impl FnOnce(u16) -> HarnessResult,
    canonicalize: bool,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind harness server");
    let port = listener.local_addr().expect("local addr").port();
    let transcript = Arc::new(Mutex::new(Vec::new()));
    let server_transcript = Arc::clone(&transcript);
    let tls = server_tls_config();
    let handle = std::thread::spawn(move || run_server(listener, script, server_transcript, tls));

    let error = run(port);
    handle.join().expect("harness server thread");
    let mut entries = transcript.lock().expect("transcript lock").clone();
    if canonicalize {
        entries = canonicalize_data_block(entries);
    }
    emit(name, error, &entries, "");
}

fn emit_simple(
    name: &str,
    result: Result<(), sg_txemail::SendError>,
    message: &str,
    transcript: Vec<String>,
) {
    emit(
        name,
        result.map_err(|error| error.to_string()),
        &transcript,
        message,
    );
}

fn emit(name: &str, result: HarnessResult, transcript: &[String], message: &str) {
    let error = result.err().unwrap_or_default();
    let line = serde_json::json!({
        "case": name,
        "error": error,
        "message": message,
        "transcript": transcript,
    });
    println!("txsmtp {line}");
}

// ---- scripted server ----

enum Conn {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Conn {
    fn is_tls(&self) -> bool {
        matches!(self, Conn::Tls(_))
    }

    fn into_plain(self) -> TcpStream {
        match self {
            Conn::Plain(tcp) => tcp,
            Conn::Tls(_) => panic!("cannot downgrade TLS"),
        }
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(tcp) => tcp.read(buf),
            Conn::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(tcp) => tcp.write(buf),
            Conn::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Conn::Plain(tcp) => tcp.flush(),
            Conn::Tls(stream) => stream.flush(),
        }
    }
}

fn run_server(
    listener: TcpListener,
    script: Script,
    transcript: Arc<Mutex<Vec<String>>>,
    tls: Arc<rustls::ServerConfig>,
) {
    let Ok((tcp, _)) = listener.accept() else {
        return;
    };
    let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(10)));
    let mut conn = Conn::Plain(tcp);
    write_line(&mut conn, &transcript, script.greeting);

    let mut rcpt_count = 0;
    while let Some(line) = read_line(&mut conn) {
        transcript.lock().expect("lock").push(format!("C {line}"));
        let verb = line
            .split(' ')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        match verb.as_str() {
            "EHLO" => {
                let responses = if conn.is_tls() {
                    script.ehlo_tls
                } else {
                    script.ehlo
                };
                for response in responses {
                    write_line(&mut conn, &transcript, response);
                }
            }
            "HELO" => write_line(&mut conn, &transcript, script.helo),
            "STARTTLS" => {
                write_line(&mut conn, &transcript, "220 2.0.0 Ready to start TLS");
                let tcp = conn.into_plain();
                let server = ServerConnection::new(Arc::clone(&tls)).expect("server connection");
                conn = Conn::Tls(Box::new(StreamOwned::new(server, tcp)));
            }
            "AUTH" => {
                if let Some(challenge) = script.auth_challenge {
                    write_line(&mut conn, &transcript, &format!("334 {challenge}"));
                    let Some(response) = read_line(&mut conn) else {
                        break;
                    };
                    transcript
                        .lock()
                        .expect("lock")
                        .push(format!("C {response}"));
                }
                write_line(&mut conn, &transcript, script.auth_ok);
            }
            "MAIL" => write_line(&mut conn, &transcript, script.mail),
            "RCPT" => {
                let index = rcpt_count.min(script.rcpt.len().saturating_sub(1));
                rcpt_count += 1;
                write_line(&mut conn, &transcript, script.rcpt[index]);
            }
            "DATA" => {
                write_line(&mut conn, &transcript, script.data);
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
                write_line(&mut conn, &transcript, script.data_done);
            }
            "QUIT" => {
                write_line(&mut conn, &transcript, script.quit);
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

fn read_line(conn: &mut Conn) -> Option<String> {
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

fn write_line(conn: &mut Conn, transcript: &Mutex<Vec<String>>, line: &str) {
    let mut bytes = line.as_bytes().to_vec();
    bytes.extend_from_slice(b"\r\n");
    conn.write_all(&bytes).expect("server write");
    conn.flush().expect("server flush");
    transcript.lock().expect("lock").push(format!("S {line}"));
}

fn server_tls_config() -> Arc<rustls::ServerConfig> {
    let certificate = CertificateDer::from(der_from_pem(CERT_PEM, "CERTIFICATE"));
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der_from_pem(
        KEY_PEM,
        "PRIVATE KEY",
    )));
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .expect("harness server certificate");
    Arc::new(config)
}

fn der_from_pem(pem: &str, label: &str) -> Vec<u8> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let body: String = pem
        .lines()
        .skip_while(|line| line.trim() != begin)
        .skip(1)
        .take_while(|line| line.trim() != end)
        .collect();
    BASE64.decode(body).expect("PEM base64")
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
    // "Www, dd Mmm yyyy HH:MM:SS +ZZZZ"
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
