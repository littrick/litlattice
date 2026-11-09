//! M9.8cp differential harness: MIME assembly (`email.Bytes()`).
//!
//! Mirrors `internal/txemail/txemail_mime_harness_test.go`. Each case builds the
//! same `Email` via M9.8co's `render` or directly, calls `Email::bytes`, strips
//! the nondeterministic fields (Date, generated Message-Id, multipart boundary)
//! and prints one canonical `txmime ` JSON line.
//!
//! Gated by `SG_TXEMAIL_MIME_HARNESS_ON=1`.

use std::collections::BTreeMap;

use sg_txemail::{mail_address, render, Email, Message, Templates};

#[test]
fn mime_harness() {
    if std::env::var("SG_TXEMAIL_MIME_HARNESS_ON").as_deref() != Ok("1") {
        eprintln!("set SG_TXEMAIL_MIME_HARNESS_ON=1 to run");
        return;
    }

    emit(
        "alt-ascii",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            "Hello",
            "Hello, world!\nLine two.\n",
            "<p>Hello, world!</p>\n<p>Line two.</p>\n",
        ),
    );

    emit(
        "alt-unicode-subject",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            "Hello José — 你好",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "alt-long-subject",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            &"é".repeat(40),
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "subject-specials",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            "a=b?c_d\te f",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "body-qp",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            "Body",
            &format!(
                "{} \n\ttab line\t\nlone\rcarriage\nlone\nlf\nequals = sign\nhéllo\0\n",
                "x".repeat(100)
            ),
            &format!("<p>{}</p>\n", "y".repeat(90)),
        ),
    );

    emit(
        "alt-from-unicode",
        &email(
            "noreply@example.com",
            "José",
            &["dest@example.com"],
            "Hello",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "alt-from-unicode-special",
        &email(
            "noreply@example.com",
            "José, Jr.",
            &["dest@example.com"],
            "Hello",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "alt-to-multi",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["a@x.test", "b@y.test"],
            "Hello",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "alt-to-names",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["\"Doe, John\" <j@x.test>", "Alice <a@x.test>"],
            "Hello",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "to-empty",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &[],
            "Hello",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    emit(
        "from-no-at",
        &email(
            "no-at",
            "Nobody",
            &["dest@example.com"],
            "Hello",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    let mut with_reply_to = email(
        "noreply@example.com",
        "Sourcegraph",
        &["dest@example.com"],
        "Hello",
        "Hi\n",
        "<p>Hi</p>\n",
    );
    with_reply_to.reply_to = vec!["Reply Person <r@x.test>".to_owned()];
    emit("reply-to", &with_reply_to);

    let mut with_headers = email(
        "noreply@example.com",
        "Sourcegraph",
        &["dest@example.com"],
        "Hello",
        "Hi\n",
        "<p>Hi</p>\n",
    );
    with_headers.headers.insert(
        "Message-ID".to_owned(),
        vec!["<mid@example.com>".to_owned()],
    );
    with_headers.headers.insert(
        "References".to_owned(),
        vec!["<r1@x.test> <r2@y.test>".to_owned()],
    );
    with_headers
        .headers
        .insert("X-Custom".to_owned(), vec!["value".to_owned()]);
    with_headers
        .headers
        .insert("X-Unicode".to_owned(), vec!["héllo".to_owned()]);
    emit("headers", &with_headers);

    emit(
        "html-only",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            "Only HTML",
            "",
            "<p>Only HTML</p>\n",
        ),
    );

    emit(
        "text-only",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            "Only text",
            "Only text\n",
            "",
        ),
    );

    emit(
        "empty-subject",
        &email(
            "noreply@example.com",
            "Sourcegraph",
            &["dest@example.com"],
            "",
            "Hi\n",
            "<p>Hi</p>\n",
        ),
    );

    // End-to-end: render (the M9.8co path) then bytes, like txemail.Send.
    let rendered = render(
        "noreply@example.com",
        "Sourcegraph",
        &Message {
            to: vec!["dest@example.com".to_owned()],
            reply_to: None,
            message_id: Some("<mid@example.com>".to_owned()),
            references: vec!["r1@x.test".to_owned(), "r2@y.test".to_owned()],
            template: Templates {
                subject: "Hi {{.Name}}".to_owned(),
                text: "Text {{.Name}}\n".to_owned(),
                html: "<p>HTML {{.Name}}</p>\n".to_owned(),
            },
            data: serde_json::json!({"Name": "A <B> & C+"}),
        },
    )
    .expect("render");
    emit("render-integration", &rendered.into_email());
}

fn email(
    from_address: &str,
    from_name: &str,
    to: &[&str],
    subject: &str,
    text: &str,
    html: &str,
) -> Email {
    Email {
        from: mail_address(from_name, from_address),
        to: to.iter().map(|address| (*address).to_owned()).collect(),
        subject: subject.to_owned(),
        text: text.as_bytes().to_vec(),
        html: html.as_bytes().to_vec(),
        headers: BTreeMap::new(),
        ..Email::default()
    }
}

fn emit(name: &str, message: &Email) {
    let raw = message
        .bytes()
        .unwrap_or_else(|error| panic!("case {name}: bytes: {error}"));
    let raw = String::from_utf8(raw).unwrap_or_else(|error| panic!("case {name}: utf8: {error}"));
    println!(
        "txmime {}",
        serde_json::json!({"case": name, "message": canonicalize_mime(&raw)})
    );
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

/// Same transform as the Go harness: BOUNDARY<n> for every maximal run of 60
/// lowercase hex characters, DATE/MESSAGEID placeholders, unfold and sort the
/// top-level header lines.
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
