//! M9.23c differential: `Query.highlightCode`.
//!
//! The Go oracle (`cmd/frontend/internal/highlight/zz_m9_23c_highlight_oracle_test.go`)
//! runs the real `highlight.Code` against a local syntect-server stand-in,
//! recording the request, the stand-in response and the resolver output.
//!
//! This harness replays the recorded response from a local TCP server, points
//! the production [`SyntectCodeHighlighter`] at it, and writes the same JSON
//! schema (`request`, `response`, `output`) so the two files can be compared
//! byte-for-byte.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use serde::Deserialize;

use sg_service_frontend::graphql_inventory_c::CodeHighlighter;
use sg_service_frontend::markdown_graphql::{SyntectCodeHighlighter, syntect_language};

#[derive(Deserialize)]
struct Case {
    name: String,
    fuzzy: String,
    code: String,
    disable_timeout: bool,
    #[allow(dead_code)]
    server: String,
}

#[derive(Deserialize)]
struct Golden {
    #[allow(dead_code)]
    request: serde_json::Value,
    response: GoldenResponse,
}

#[derive(Deserialize)]
struct GoldenResponse {
    status: u16,
    body: String,
}

struct Captured {
    method: String,
    path: String,
    content_type: String,
    stabilize_timeout: String,
    body: String,
}

#[tokio::test]
async fn highlight_code_diff() {
    let corpus = match std::env::var("M9_23C_HL_CORPUS") {
        Ok(path) => path,
        Err(_) => return,
    };
    let gold = std::env::var("M9_23C_HL_GOLD").expect("M9_23C_HL_GOLD");
    let out = std::env::var("M9_23C_HL_OUT").expect("M9_23C_HL_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    for case in &cases {
        let golden: Golden = serde_json::from_slice(
            &std::fs::read(std::path::Path::new(&gold).join(format!("{}.json", case.name)))
                .expect("golden"),
        )
        .expect("golden json");

        let status = golden.response.status;
        let response_body = golden.response.body.clone();
        let body_for_server = response_body.clone();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("addr");
        let (tx, rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
            let captured = read_request(&mut stream);
            let payload = format!(
                "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                body_for_server.len(),
                body_for_server
            );
            stream.write_all(payload.as_bytes()).expect("write");
            stream.flush().ok();
            tx.send(captured).expect("send capture");
        });

        let highlighter = SyntectCodeHighlighter::with_base_url(&format!("http://{address}"));
        let filepath = format!(
            "file.{}",
            syntect_language(&case.fuzzy.to_ascii_lowercase())
        );
        let result = highlighter
            .highlight(&case.code, &filepath, case.disable_timeout)
            .await;

        let captured = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("request captured");
        server.join().expect("server");

        let (ok, html, error) = match result {
            Ok(html) => (true, html, String::new()),
            Err(error) => (false, String::new(), error),
        };
        let json = format!(
            "{{\"request\":{{\"method\":{},\"path\":{},\"content_type\":{},\"stabilize_timeout\":{},\"body\":{}}},\"response\":{{\"status\":{},\"body\":{}}},\"output\":{{\"ok\":{},\"html\":{},\"error\":{}}}}}",
            go_json_string(&captured.method),
            go_json_string(&captured.path),
            go_json_string(&captured.content_type),
            go_json_string(&captured.stabilize_timeout),
            go_json_string(&captured.body),
            status,
            go_json_string(&response_body),
            ok,
            go_json_string(&html),
            go_json_string(&error),
        );
        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            json,
        )
        .expect("write output");
    }
    println!("RUST {} cases", cases.len());
}

fn read_request(stream: &mut impl Read) -> Captured {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(position) = find_header_end(&buffer) {
            break position;
        }
        let read = stream.read(&mut chunk).expect("read");
        if read == 0 {
            break buffer.len();
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = headers.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut content_type = String::new();
    let mut stabilize_timeout = String::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            match name.to_ascii_lowercase().as_str() {
                "content-type" => content_type = value.to_string(),
                "x-stabilize-timeout" => stabilize_timeout = value.to_string(),
                "content-length" => content_length = value.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    let body_start = (header_end + 4).min(buffer.len());
    while buffer.len() < body_start + content_length {
        let read = stream.read(&mut chunk).expect("read body");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&buffer[body_start..]).into_owned();
    Captured {
        method,
        path,
        content_type,
        stabilize_timeout,
        body,
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Go `encoding/json` string escaping (`json.Marshal`).
fn go_json_string(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('"');
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
